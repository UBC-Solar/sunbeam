from fastapi import APIRouter, Depends, HTTPException
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

_connection_service: ConnectionService | None = None


def get_connection_service() -> ConnectionService:
    global _connection_service
    if _connection_service is None:
        _connection_service = ConnectionService()
    return _connection_service


def _test_result(result: CheckResult, window) -> ConnectionTestResult:
    start, end = window if window is not None else (None, None)
    return ConnectionTestResult(
        ok=result.ok,
        detail=result.detail,
        steps=[ConnectionTestStep(name=s.name, ok=s.ok, detail=s.detail) for s in result.steps],
        window_start=start,
        window_end=end,
    )


def _window(service: ConnectionService, db: Session, event_id: int | None):
    try:
        return service.window_for(db, event_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


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
    try:
        return service.create(db, **payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


# Declared before /{connection_id} routes so "test" is never parsed as an id
@router.post("/test", response_model=ConnectionTestResult)
def test_unsaved_connection(
    payload: ConnectionTestRequest,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    """ Test connection settings before saving them. """
    window = _window(service, db, payload.event_id)
    result = service.check(**payload.model_dump(exclude={"event_id"}), window=window)
    return _test_result(result, window)


@router.patch("/{connection_id}", response_model=ConnectionRead)
def update_connection(
    connection_id: int,
    payload: ConnectionUpdate,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    try:
        connection = service.update(db, connection_id, **payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    if connection is None:
        raise HTTPException(status_code=404, detail="Connection not found")
    return connection


@router.delete("/{connection_id}", status_code=204)
def delete_connection(
    connection_id: int,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    if not service.delete(db, connection_id):
        raise HTTPException(status_code=404, detail="Connection not found")


@router.post("/{connection_id}/default", response_model=ConnectionRead)
def make_default_connection(
    connection_id: int,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    connection = service.set_default(db, connection_id)
    if connection is None:
        raise HTTPException(status_code=404, detail="Connection not found")
    return connection


@router.post("/{connection_id}/test", response_model=ConnectionTestResult)
def test_connection(
    connection_id: int,
    event_id: int | None = None,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    """ Test a saved connection, optionally for data over an event, and record the result on it. """
    connection = service.get(db, connection_id)
    if connection is None:
        raise HTTPException(status_code=404, detail="Connection not found")

    window = _window(service, db, event_id)
    return _test_result(service.check_saved(db, connection, window), window)
