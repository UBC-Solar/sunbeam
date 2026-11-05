import functools

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from db.telemetrydb.connection_check import CheckResult
from server.deps import get_db
from server.schemas import (
    ConnectionCreate,
    ConnectionRead,
    ConnectionTestRequest,
    ConnectionTestResult,
    ConnectionTestStep,
    ConnectionUpdate,
)
from server.services.connection_service import ConnectionService


router = APIRouter(prefix="/connections", tags=["connections"])


@functools.cache
def get_connection_service() -> ConnectionService:
    return ConnectionService()


def _test_result(result: CheckResult, window) -> ConnectionTestResult:
    start, end = window if window is not None else (None, None)
    return ConnectionTestResult(
        ok=result.ok,
        detail=result.detail,
        steps=[ConnectionTestStep(name=s.name, ok=s.ok, detail=s.detail) for s in result.steps],
        window_start=start,
        window_end=end,
    )


@router.get("", response_model=list[ConnectionRead])
def list_connections(
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    return service.list(db)


@router.post("", response_model=ConnectionRead, status_code=201)
def create_connection(
    payload: ConnectionCreate,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    return service.create(db, **payload.model_dump())


# Declared before /{connection_id} routes so "test" is never parsed as an id
@router.post("/test", response_model=ConnectionTestResult)
def test_unsaved_connection(
    payload: ConnectionTestRequest,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    """ Test connection settings before saving them. """
    window = service.window_for(db, payload.event_id)
    result = service.check(**payload.model_dump(exclude={"event_id"}), window=window)
    return _test_result(result, window)


@router.patch("/{connection_id}", response_model=ConnectionRead)
def update_connection(
    connection_id: int,
    payload: ConnectionUpdate,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    return service.update(db, connection_id, **payload.model_dump())


@router.delete("/{connection_id}", status_code=204)
def delete_connection(
    connection_id: int,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    service.delete(db, connection_id)


@router.post("/{connection_id}/default", response_model=ConnectionRead)
def make_default_connection(
    connection_id: int,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    return service.set_default(db, connection_id)


@router.post("/{connection_id}/test", response_model=ConnectionTestResult)
def test_connection(
    connection_id: int,
    event_id: int | None = None,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    """ Test a saved connection, optionally for data over an event, and record the result on it. """
    connection = service.get(db, connection_id)
    window = service.window_for(db, event_id)
    return _test_result(service.check_saved(db, connection, window), window)
