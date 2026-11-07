import functools
import logging
import uuid

import anyio

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import TERMINAL_WORKER_STATUSES, WorkerRun
from server.deps import get_db
from server.schemas import (
    LaunchWorkerRequest,
    WorkerCompleteRequest,
    WorkerHeartbeatRequest,
    WorkerMetricsRead,
    WorkerMetricsReport,
    WorkerPermissionResponse,
    WorkerRegisterRequest,
    WorkerRunRead,
)
from server.services.worker_service import WorkerService

logger = logging.getLogger("sunbeam.server")

router = APIRouter(prefix="/workers", tags=["workers"])


@functools.cache
def get_worker_service() -> WorkerService:
    # One for the lifetime of the process: a fresh WorkerService() opens its own docker.from_env() client that is
    # never closed, so it must not be created per request.
    return WorkerService()


@router.get("", response_model=list[WorkerRunRead])
def list_workers(
    active_only: bool = False,
    limit: int | None = Query(default=None, ge=1, description="Only the most recent runs."),
    db: Session = Depends(get_db),
):
    """ Newest first. `active_only` leaves out terminal runs (completed, failed, lost, cancelled). """
    stmt = select(WorkerRun).order_by(WorkerRun.created_at.desc())

    if active_only:
        stmt = stmt.where(WorkerRun.status.not_in(TERMINAL_WORKER_STATUSES))
    if limit is not None:
        stmt = stmt.limit(limit)

    return db.scalars(stmt).all()


@router.post("/launch", response_model=WorkerRunRead)
def launch_worker(
    payload: LaunchWorkerRequest,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    """
    Runs a worker container for the event, with the event's own pipeline edition. What the dashboard calls.

    First tests the connection over the event's window: 422 with the failing step, and nothing created, if it can't
    reach InfluxDB, find the bucket or see any data. 409 if the event already has an active worker; 404 for an unknown
    event or connection; 400 for an unknown git ref or edition.

    The image is `sunbeam-worker:<edition>`, or `<edition>-<sha12>` from a git ref. If that isn't built yet the run is
    returned as `building` and starts when the build finishes (or fails with the build log's tail). If Docker itself
    fails, the run is still returned (200) but `failed`, with the reason in `failure_reason`.
    """
    return service.launch_worker(
        db,
        event_id=payload.event_id,
        influx_connection_id=payload.influx_connection_id,
        source_ref=payload.source_ref,
        reprocess=payload.reprocess,
    )


@router.post("/register", response_model=WorkerRunRead)
def register_worker(
    payload: WorkerRegisterRequest,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    """
    How `uv run sunbeam.py` run by hand gets an (`external`) run ID. 404 for an unknown event, 400 for an unknown
    edition, 409 if the event already has an active worker.
    """
    return service.register_worker(
        db,
        event_name=payload.event_name,
        pipeline_edition=payload.pipeline_edition,
        host=payload.host,
        influx_connection_name=payload.influx_connection_name,
        reprocess=payload.reprocess,
    )


@router.post("/{worker_id}/heartbeat", response_model=WorkerRunRead)
def heartbeat(
    worker_id: uuid.UUID,
    payload: WorkerHeartbeatRequest,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    """
    Sent by the worker about every second. Once a stop is requested the status stays `stop_requested` whatever the
    worker reports, and a heartbeat for a terminal run is ignored.
    """
    return service.heartbeat(
        db,
        worker_id=worker_id,
        status=payload.status,
        status_message=payload.status_message,
        host=payload.host,
    )


@router.get("/{worker_id}/permission", response_model=WorkerPermissionResponse)
def get_permission(
    worker_id: uuid.UUID,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    """
    Polled by the worker about every second; `allowed: false` means stop now. A worker that can't reach the server
    for about a minute stops itself.
    """
    allowed, reason = service.permission_for(db, worker_id)
    return WorkerPermissionResponse(allowed=allowed, reason=reason)


@router.post("/{worker_id}/stop", response_model=WorkerRunRead)
def request_stop(
    worker_id: uuid.UUID,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    """
    Asks the worker to stop; it notices on its next permission poll. If it hasn't stopped within the watchdog's grace
    period, its container is killed (or, for an external worker, the run is marked `lost`). No-op for a terminal run.
    """
    return service.request_stop(db, worker_id)


@router.post("/{worker_id}/complete", response_model=WorkerRunRead)
def complete_worker(
    worker_id: uuid.UUID,
    payload: WorkerCompleteRequest,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    """
    Sent by the worker once its writes have finished, so `completed` means the data is there. An unsuccessful run is
    `cancelled` if a stop was requested (how every realtime run ends), otherwise `failed` with `message` as the reason.
    A run the watchdog already resolved keeps its status.
    """
    return service.complete(db, worker_id=worker_id, success=payload.success, message=payload.message)


@router.post("/{worker_id}/metrics", status_code=204)
def report_metrics(
    worker_id: uuid.UUID,
    payload: WorkerMetricsReport,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    """
    Timing and writer-queue snapshot, sent every 0.5 s by a replay and every 5 s by a realtime worker (1 s in a
    terminal). 404 once the run is terminal, which a worker's last report can race harmlessly.
    """
    service.record_metrics(db, worker_id=worker_id, payload=payload.model_dump())


@router.get("/{worker_id}/metrics", response_model=WorkerMetricsRead)
def get_metrics(
    worker_id: uuid.UUID,
    service: WorkerService = Depends(get_worker_service),
):
    """
    The latest snapshot, held in memory until the run completes or the server restarts; 404 if there's none.
    `replay` is only reported by replays: `speed` is how many times faster than realtime it's running.
    """
    return service.get_metrics(worker_id)


@router.get("/{worker_id}/logs")
def get_logs(
    worker_id: uuid.UUID,
    tail: int = 500,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    """
    The container's last `tail` log lines. 404 if the run has no container (external, or failed before launching).
    """
    return {"lines": service.get_logs(db, worker_id, tail=tail)}


@router.get("/{worker_id}/logs/stream")
def stream_logs(
    worker_id: uuid.UUID,
    tail: int = 200,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    """ The same, followed live as server-sent events: one `data: <line>` message per line. """
    log_stream = service.stream_logs(db, worker_id, tail=tail)

    # The stream can stay open for as long as the worker runs, so give the session's pooled connection back now
    # rather than when the response finishes; otherwise a handful of open log viewers exhausts the pool and every
    # other route (including /health) blocks waiting for a connection.
    db.close()

    async def event_source():
        try:
            while True:
                # Docker's follow stream blocks until the container writes a line. abandon_on_cancel lets a client
                # disconnect cancel this immediately instead of waiting for the next line, which for a quiet worker
                # may never come; the finally below then closes the stream, unblocking the abandoned thread.
                chunk = await anyio.to_thread.run_sync(next, log_stream, None, abandon_on_cancel=True)
                if chunk is None:
                    break
                text = chunk.decode("utf-8", errors="replace")
                for line in text.splitlines():
                    yield f"data: {line}\n\n"
        except Exception:
            logger.exception("Log stream for worker %s failed", worker_id)
        finally:
            close = getattr(log_stream, "close", None)
            if close is not None:
                close()

    return StreamingResponse(event_source(), media_type="text/event-stream")