import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import WorkerRun, WorkerStatus
from orchestration.deps import get_db
from orchestration.schemas import (
    LaunchWorkerRequest,
    WorkerCompleteRequest,
    WorkerHeartbeatRequest,
    WorkerPermissionResponse,
    WorkerRunRead,
)
from orchestration.services.worker_service import WorkerService


router = APIRouter(prefix="/workers", tags=["workers"])


def get_worker_service() -> WorkerService:
    return WorkerService()


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