import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import WorkerRun, WorkerStatus
from server.deps import get_db
from server.schemas import (
    LaunchWorkerRequest,
    WorkerCompleteRequest,
    WorkerHeartbeatRequest,
    WorkerMetricsRead,
    WorkerMetricsReport,
    WorkerPermissionResponse,
    WorkerRunRead,
)
from server.services.worker_service import WorkerService

logger = logging.getLogger("sunbeam.server")

router = APIRouter(prefix="/workers", tags=["workers"])


_worker_service: WorkerService | None = None


def get_worker_service() -> WorkerService:
    # A fresh WorkerService() opens its own docker.from_env() client that is
    # never closed, so it must not be re-created per request. Reuse one for
    # the lifetime of the process instead.
    global _worker_service
    if _worker_service is None:
        _worker_service = WorkerService()
    return _worker_service


@router.get("", response_model=list[WorkerRunRead])
def list_workers(
    active_only: bool = False,
    db: Session = Depends(get_db),
):
    stmt = select(WorkerRun).order_by(WorkerRun.created_at.desc())

    if active_only:
        stmt = stmt.where(
            WorkerRun.status.in_(
                [
                    WorkerStatus.REQUESTED,
                    WorkerStatus.STARTING,
                    WorkerStatus.RUNNING,
                    WorkerStatus.STOP_REQUESTED,
                    WorkerStatus.STOPPING,
                ]
            )
        )

    return db.scalars(stmt).all()


@router.post("/launch", response_model=WorkerRunRead)
def launch_worker(
    payload: LaunchWorkerRequest,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    try:
        return service.launch_worker(
            db,
            event_id=payload.event_id,
            pipeline_edition=payload.pipeline_edition,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{worker_id}/heartbeat", response_model=WorkerRunRead)
def heartbeat(
    worker_id: uuid.UUID,
    payload: WorkerHeartbeatRequest,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    worker = service.heartbeat(
        db,
        worker_id=worker_id,
        status=payload.status,
        current_stage=payload.current_stage,
        status_message=payload.status_message,
        host=payload.host,
    )

    if worker is None:
        raise HTTPException(status_code=404, detail="Worker not found")

    return worker


@router.get("/{worker_id}/permission", response_model=WorkerPermissionResponse)
def get_permission(
    worker_id: uuid.UUID,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    allowed, reason, stop_requested = service.permission_for(db, worker_id)

    return WorkerPermissionResponse(
        allowed=allowed,
        reason=reason,
        stop_requested=stop_requested,
    )


@router.post("/{worker_id}/stop", response_model=WorkerRunRead)
def request_stop(
    worker_id: uuid.UUID,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    worker = service.request_stop(db, worker_id)

    if worker is None:
        raise HTTPException(status_code=404, detail="Worker not found")

    return worker


@router.post("/{worker_id}/complete", response_model=WorkerRunRead)
def complete_worker(
    worker_id: uuid.UUID,
    payload: WorkerCompleteRequest,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    worker = service.complete(
        db,
        worker_id=worker_id,
        success=payload.success,
        message=payload.message,
    )

    if worker is None:
        raise HTTPException(status_code=404, detail="Worker not found")

    return worker


@router.post("/{worker_id}/metrics", status_code=204)
def report_metrics(
    worker_id: uuid.UUID,
    payload: WorkerMetricsReport,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    accepted = service.record_metrics(
        db,
        worker_id=worker_id,
        payload=payload.model_dump(),
    )

    if not accepted:
        raise HTTPException(
            status_code=404,
            detail="Worker not found or no longer active",
        )


@router.get("/{worker_id}/metrics", response_model=WorkerMetricsRead)
def get_metrics(
    worker_id: uuid.UUID,
    service: WorkerService = Depends(get_worker_service),
):
    metrics = service.get_metrics(worker_id)

    if metrics is None:
        raise HTTPException(status_code=404, detail="No metrics available")

    return metrics


@router.get("/{worker_id}/logs")
def get_logs(
    worker_id: uuid.UUID,
    tail: int = 500,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    lines = service.get_logs(db, worker_id, tail=tail)

    if lines is None:
        raise HTTPException(
            status_code=404,
            detail="Worker or its container could not be found",
        )

    return {"lines": lines}


@router.get("/{worker_id}/logs/stream")
def stream_logs(
    worker_id: uuid.UUID,
    tail: int = 200,
    db: Session = Depends(get_db),
    service: WorkerService = Depends(get_worker_service),
):
    log_stream = service.stream_logs(db, worker_id, tail=tail)

    if log_stream is None:
        raise HTTPException(
            status_code=404,
            detail="Worker or its container could not be found",
        )

    def event_source():
        try:
            for chunk in log_stream:
                text = chunk.decode("utf-8", errors="replace")
                for line in text.splitlines():
                    yield f"data: {line}\n\n"
        except Exception:
            logger.exception("Log stream for worker %s failed", worker_id)

    return StreamingResponse(event_source(), media_type="text/event-stream")