"""
Telemetry data read endpoints, keyed by event NAME and signal name.

  GET /events/{event_name}/signals                       - list signals
  GET /events/{event_name}/signals/{signal_name}/data    - windowed query
  GET /events/{event_name}/data/stream                   - multiplexed SSE

The stream protocol (see server/static/stream_viewer.html for a worked
client example):

  event: meta          first message; per-signal {signal_id, unit, frequency}
  event: data          columnar batches {signal: {timestamps[], values[]}}
  : keepalive          comment lines during quiet stretches

Every data event carries `id: <cursor>` (epoch microseconds). EventSource
sends it back as Last-Event-ID on reconnect, and the server resumes strictly
after it - no gaps, no duplicates.
"""
import json
import logging
from datetime import datetime
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
def list_signals(event_name: str, db: Session = Depends(get_db)):
    try:
        return data_service.list_signals(db, event_name)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{event_name}/signals/{signal_name}/data", response_model=SignalDataResponse)
def query_signal_data(
    event_name: str,
    signal_name: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    last_seconds: Optional[float] = None,
    limit: int = Query(default=data_service.MAX_POINTS, ge=1, le=data_service.MAX_POINTS),
    db: Session = Depends(get_db),
):
    try:
        window_start, window_end = data_service.resolve_time_window(
            start, end, last_seconds
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        signal = data_service.get_signal(db, event_name, signal_name)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    timestamps, values, truncated = data_service.query_samples(
        db, signal, window_start, window_end, limit=limit
    )

    return SignalDataResponse(
        event_name=event_name,
        signal=signal_name,
        unit=signal.unit,
        frequency=signal.frequency,
        start=window_start,
        end=window_end,
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

    # Resolve signals eagerly so an unknown event/signal is a clean 404
    # instead of an error mid-stream.
    try:
        first = next(batches)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

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
