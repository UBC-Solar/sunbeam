"""
Telemetry reads, by event and signal name. The query endpoint is for analysis over any window; the stream is for a
live view of many signals over one connection.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from server.deps import get_db, get_db_session_factory
from server.schemas import SignalDataResponse, SignalInfo
from server.services import data_service

logger = logging.getLogger("sunbeam.server")

router = APIRouter(prefix="/events", tags=["data"])


@router.get("/{event_name}/signals", response_model=list[SignalInfo])
def list_signals(event_name: str, with_data: bool = False, db: Session = Depends(get_db)):
    """ Every signal the event defines; `with_data=true` lists only those with at least one sample. """
    return data_service.list_signals(db, event_name, with_data=with_data)


@router.get("/{event_name}/signals/{signal_name}/data", response_model=SignalDataResponse)
def query_signal_data(
    event_name: str,
    signal_name: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    last_seconds: Optional[float] = None,
    limit: int = Query(
        default=data_service.DEFAULT_LIMIT, ge=0, description="The most samples to return; 0 for no limit.",
    ),
    db: Session = Depends(get_db),
):
    """
    One signal over a window, chosen by the parameters given:

    | Parameters | Window |
    |---|---|
    | none | the whole event |
    | `start`, `end` | `[start, end)` |
    | `start` | `[start, now)` |
    | `end` | everything before `end` |
    | `last_seconds` | `[now - last_seconds, now)` |

    Datetimes must have a time zone. Mixing `last_seconds` with `start`/`end`, or `start >= end`, is 422. An open side
    of the window is reported as the event's own start (or end, or now for a live event).

    Timestamps are epoch milliseconds. More than `limit` samples returns the most recent `limit` with
    `truncated: true`. To get everything, pass `limit=0` for the whole window in one response (which can be large),
    or page backwards by passing the earliest timestamp as the next `end` (what data_tools'
    `SunbeamClient.get_time_series` does).
    """
    try:
        window_start, window_end = data_service.resolve_time_window(
            start, end, last_seconds
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    signal = data_service.get_signal(db, event_name, signal_name)

    timestamps, values, truncated = data_service.query_samples(
        db, signal, window_start, window_end, limit=limit or None
    )

    # An unbounded side of the window reports the event's own bound
    event = signal.event
    return SignalDataResponse(
        event_name=event_name,
        signal=signal_name,
        unit=signal.unit,
        frequency=signal.frequency,
        source=signal.source,
        description=signal.description,
        start=window_start or event.starts_at,
        end=window_end or event.ends_at or datetime.now(timezone.utc),
        count=len(timestamps),
        truncated=truncated,
        timestamps=timestamps,
        values=values,
    )


@router.get("/{event_name}/data/stream")
def stream_signal_data(
    event_name: str,
    signals: str = Query(description="Comma-separated signal names."),
    since: Optional[int] = Query(
        default=None,
        description="Resume cursor (epoch microseconds); omit to tail from now.",
    ),
    poll_interval_s: float = Query(default=0.5, ge=0.1, le=5.0),
    last_event_id: Optional[str] = Header(default=None),
    session_factory=Depends(get_db_session_factory),
):
    """
    Server-sent events for any number of the event's signals:

    ```
    event: meta
    data: {"VehicleSpeed": {"signal_id": 14, "unit": "m/s", "frequency": 5.0}, ...}

    event: data
    id: 1752968412558123
    data: {"VehicleSpeed": {"timestamps": [1752968412558], "values": [13.1]}, "PackCurrent": {"timestamps": [], "values": []}}

    : keepalive
    ```

    `meta` comes first, once. Each poll that finds new rows sends a `data` message with every subscribed signal
    (empty arrays if it had nothing new) and `id:`, the newest row's time in epoch microseconds. Keepalive comments
    fill quiet stretches. An unknown event or signal is a 404 before the stream opens.

    The stream resumes strictly after `since`, or after the `Last-Event-ID` header, which `EventSource` sends by
    itself on reconnect: no gaps and no duplicates. To backfill a chart first, query `last_seconds`, then stream with
    `since` = its last timestamp × 1000.
    """
    signal_names = [name.strip() for name in signals.split(",") if name.strip()]
    if not signal_names:
        raise HTTPException(status_code=422, detail="No signal names given.")

    # EventSource reconnection: the browser replays the last delivered cursor
    # in the Last-Event-ID header; it wins over the query parameter.
    since_us = since
    if last_event_id is not None:
        try:
            since_us = int(last_event_id)
        except ValueError:
            raise HTTPException(status_code=422, detail="Invalid Last-Event-ID.")

    batches = data_service.stream_batches(
        session_factory,
        event_name=event_name,
        signal_names=signal_names,
        since_us=since_us,
        poll_interval_s=poll_interval_s,
    )

    # Resolve signals eagerly so an unknown event/signal is a clean 404 (a LookupError) instead of an error mid-stream
    first = next(batches)

    def event_source():
        try:
            yield _format_sse(*first)
            for kind, payload, cursor in batches:
                yield _format_sse(kind, payload, cursor)
        except Exception:
            logger.exception("Data stream for event %r failed", event_name)

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _format_sse(kind: str, payload, cursor) -> str:
    if kind == "idle":
        return ""

    if kind == "keepalive":
        return ": keepalive\n\n"

    message = f"event: {kind}\n"
    if cursor is not None:
        message += f"id: {cursor}\n"
    message += f"data: {json.dumps(payload)}\n\n"
    return message
