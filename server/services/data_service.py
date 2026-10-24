"""
Reading aligned telemetry samples back out of the database: bounded
single-signal window queries (post-mortem analysis) and cursor-based
incremental batches (live streaming).

Timestamps returned to clients are epoch milliseconds; stream cursors are
epoch MICROseconds so that a resumed stream never re-delivers a sample whose
sub-millisecond fraction was truncated away.
"""
import logging
import time
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import AlignedSample, Event, Signal

logger = logging.getLogger("sunbeam.server")

# 10 hours of a 10 Hz signal.
MAX_POINTS = 360_000

# Safety bound on rows fetched by a single stream poll.
STREAM_POLL_ROW_LIMIT = 10_000


def resolve_time_window(
    start: Optional[datetime],
    end: Optional[datetime],
    last_seconds: Optional[float],
    *,
    now: Optional[datetime] = None,
) -> tuple[datetime, datetime]:
    """
    Turn the three query modes into one half-open window [start, end).

      start + end          -> between the two instants
      start only           -> from start until now
      last_seconds only    -> the trailing window ending now

    Raises ValueError for invalid combinations; datetimes must be
    timezone-aware.
    """
    now = now or datetime.now(timezone.utc)

    if last_seconds is not None:
        if start is not None or end is not None:
            raise ValueError("last_seconds cannot be combined with start/end.")
        if last_seconds <= 0:
            raise ValueError("last_seconds must be positive.")
        return now - timedelta(seconds=last_seconds), now

    if start is None:
        raise ValueError("Provide either start (with optional end) or last_seconds.")

    if start.tzinfo is None:
        raise ValueError("start must be timezone-aware.")

    if end is None:
        end = now
    elif end.tzinfo is None:
        raise ValueError("end must be timezone-aware.")

    if start >= end:
        raise ValueError("start must be before end.")

    return start, end


def get_signal(db: Session, event_name: str, signal_name: str) -> Signal:
    """Resolve (event_name, signal_name) to a Signal row or raise LookupError."""
    event = db.scalar(select(Event).where(Event.name == event_name))
    if event is None:
        raise LookupError(f"No event named {event_name!r}.")

    signal = db.scalar(
        select(Signal).where(Signal.event_id == event.id, Signal.name == signal_name)
    )
    if signal is None:
        raise LookupError(
            f"Event {event_name!r} has no signal named {signal_name!r}."
        )

    return signal


def list_signals(db: Session, event_name: str) -> list[Signal]:
    event = db.scalar(select(Event).where(Event.name == event_name))
    if event is None:
        raise LookupError(f"No event named {event_name!r}.")

    return list(
        db.scalars(
            select(Signal).where(Signal.event_id == event.id).order_by(Signal.name)
        )
    )


def _epoch_ms(ts: datetime) -> int:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return round(ts.timestamp() * 1_000)


def _epoch_us(ts: datetime) -> int:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return round(ts.timestamp() * 1_000_000)


def _from_epoch_us(cursor_us: int) -> datetime:
    return datetime.fromtimestamp(cursor_us / 1_000_000, tz=timezone.utc)


def query_samples(
    db: Session,
    signal: Signal,
    start: datetime,
    end: datetime,
    limit: int = MAX_POINTS,
) -> tuple[list[int], list[Optional[float]], bool]:
    """
    Samples for one signal in [start, end), ascending. When the window holds
    more than `limit` rows, the MOST RECENT `limit` are returned and the
    truncated flag is set.
    """
    stmt = (
        select(AlignedSample.ts, AlignedSample.value_f64)
        .where(
            AlignedSample.event_id == signal.event_id,
            AlignedSample.signal_id == signal.id,
            AlignedSample.ts >= start,
            AlignedSample.ts < end,
        )
        .order_by(AlignedSample.ts.desc())
        .limit(limit + 1)
    )

    rows = list(db.execute(stmt).all())

    truncated = len(rows) > limit
    if truncated:
        rows = rows[:limit]

    rows.reverse()

    timestamps = [_epoch_ms(ts) for ts, _ in rows]
    values = [value for _, value in rows]

    return timestamps, values, truncated


def stream_batches(
    session_factory: Callable[[], Session],
    *,
    event_name: str,
    signal_names: list[str],
    since_us: Optional[int] = None,
    poll_interval_s: float = 0.5,
    keepalive_interval_s: float = 15.0,
    max_batches: Optional[int] = None,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[tuple[str, Optional[dict[str, Any]], Optional[int]]]:
    """
    Yield (kind, payload, cursor_us) tuples for one event's stream:

      ("meta", {signal: {...}}, start_cursor)   exactly once, first
      ("data", {signal: {timestamps, values}}, new_cursor)   when rows arrive
      ("keepalive", None, None)   periodically during quiet stretches
      ("idle", None, None)        every quiet poll (renders as zero bytes)

    The idle yields matter: a generator that loops without yielding cannot be
    interrupted, so they bound how long a disconnected client's stream keeps
    polling the database to one poll interval.

    The cursor is epoch microseconds; polls fetch strictly-greater timestamps,
    so a client resuming from a delivered cursor never sees duplicates.
    A short-lived session per poll - the generator may outlive any
    reasonable transaction.

    max_batches bounds the number of polls (for tests); None streams forever.
    """
    with session_factory() as db:
        signals = {
            name: get_signal(db, event_name, name) for name in signal_names
        }
        meta = {
            name: {
                "signal_id": signal.id,
                "unit": signal.unit,
                "frequency": signal.frequency,
            }
            for name, signal in signals.items()
        }
        event_id = next(iter(signals.values())).event_id
        ids_to_names = {signal.id: name for name, signal in signals.items()}

    cursor_us = since_us if since_us is not None else _epoch_us(datetime.now(timezone.utc))

    yield "meta", meta, cursor_us

    polls = 0
    last_emit_monotonic = time.monotonic()

    while max_batches is None or polls < max_batches:
        polls += 1

        with session_factory() as db:
            stmt = (
                select(AlignedSample.ts, AlignedSample.value_f64, AlignedSample.signal_id)
                .where(
                    AlignedSample.event_id == event_id,
                    AlignedSample.signal_id.in_(ids_to_names.keys()),
                    AlignedSample.ts > _from_epoch_us(cursor_us),
                )
                .order_by(AlignedSample.ts.asc())
                .limit(STREAM_POLL_ROW_LIMIT)
            )
            rows = db.execute(stmt).all()

        if rows:
            payload: dict[str, Any] = {
                name: {"timestamps": [], "values": []} for name in signal_names
            }
            for ts, value, signal_id in rows:
                series = payload[ids_to_names[signal_id]]
                series["timestamps"].append(_epoch_ms(ts))
                series["values"].append(value)

            cursor_us = max(_epoch_us(ts) for ts, _, _ in rows)
            last_emit_monotonic = time.monotonic()

            yield "data", payload, cursor_us

        elif time.monotonic() - last_emit_monotonic >= keepalive_interval_s:
            last_emit_monotonic = time.monotonic()
            yield "keepalive", None, None

        else:
            yield "idle", None, None

        sleep(poll_interval_s)
