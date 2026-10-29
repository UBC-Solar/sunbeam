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

from db.sunbeamdb.models import Event, Sample, Signal

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
) -> tuple[Optional[datetime], Optional[datetime]]:
    """
    Turn the query modes into one half-open window [start, end), where a
    None bound is unbounded.

      start + end          -> between the two instants
      start only           -> from start until now
      last_seconds only    -> the trailing window ending now
      end only             -> everything before end
      none of them         -> everything (the whole event)

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

    if end is not None and end.tzinfo is None:
        raise ValueError("end must be timezone-aware.")

    if start is None:
        return None, end

    if start.tzinfo is None:
        raise ValueError("start must be timezone-aware.")

    if end is None:
        end = now

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


def list_signals(db: Session, event_name: str, *, with_data: bool = False) -> list[Signal]:
    """
    The event's signals, by name. A signal exists as soon as the event is synced,
    whether or not a worker has written it; with_data keeps only those with at
    least one (non-null) sample to query.
    """
    event = db.scalar(select(Event).where(Event.name == event_name))
    if event is None:
        raise LookupError(f"No event named {event_name!r}.")

    stmt = select(Signal).where(Signal.event_id == event.id)
    if with_data:
        # An index lookup per signal on (event_id, signal_id, ts), not a scan
        stmt = stmt.where(
            select(Sample.ts)
            .where(
                Sample.event_id == Signal.event_id,
                Sample.signal_id == Signal.id,
                Sample.value_f64.is_not(None),
            )
            .exists()
        )

    return list(db.scalars(stmt.order_by(Signal.name)))


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
    start: Optional[datetime],
    end: Optional[datetime],
    limit: int = MAX_POINTS,
) -> tuple[list[int], list[Optional[float]], bool]:
    """
    Samples for one signal in [start, end), ascending; a None bound is
    unbounded. When the window holds more than `limit` rows, the MOST RECENT
    `limit` are returned and the truncated flag is set, so a client wanting
    everything pages backwards by passing the earliest timestamp it got as
    the next request's end.
    """
    stmt = select(Sample.ts, Sample.value_f64).where(
        Sample.event_id == signal.event_id,
        Sample.signal_id == signal.id,
    )
    if start is not None:
        stmt = stmt.where(Sample.ts >= start)
    if end is not None:
        stmt = stmt.where(Sample.ts < end)
    stmt = stmt.order_by(Sample.ts.desc()).limit(limit + 1)

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
                select(Sample.ts, Sample.value_f64, Sample.signal_id)
                .where(
                    Sample.event_id == event_id,
                    Sample.signal_id.in_(ids_to_names.keys()),
                    Sample.ts > _from_epoch_us(cursor_us),
                )
                .order_by(Sample.ts.asc())
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
