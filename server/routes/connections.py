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
    """
    By name. Exactly one is the default whenever any exist: the first created, then whichever was last made default;
    deleting the default promotes the oldest left. Tokens are shown in full: everything is inside the Tailnet.
    """
    return service.list(db)


@router.post("", response_model=ConnectionRead, status_code=201)
def create_connection(
    payload: ConnectionCreate,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    """ 409 if the name is taken. """
    return service.create(db, **payload.model_dump())


# Declared before /{connection_id} routes so "test" is never parsed as an id
@router.post("/test", response_model=ConnectionTestResult)
def test_unsaved_connection(
    payload: ConnectionTestRequest,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    """ The same test for settings that aren't saved; nothing is recorded. """
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
    """ Changing anything but the name clears the last test result. 409 if the new name is taken. """
    return service.update(db, connection_id, **payload.model_dump())


@router.delete("/{connection_id}", status_code=204)
def delete_connection(
    connection_id: int,
    db: Session = Depends(get_db),
    service: ConnectionService = Depends(get_connection_service),
):
    """ Past worker runs keep its name (`influx_connection_name`). """
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
    """
    Tests a saved connection and records the result on it. Steps run in order, stopping at the first failure:
    `connect` and `bucket` (5 s timeouts), then with `event_id`, `data`: at least one point in the event's window (all
    of it if it's over, start to now if it's live; skipped if it hasn't started), with a 15 s timeout.
    """
    connection = service.get(db, connection_id)
    window = service.window_for(db, event_id)
    return _test_result(service.check_saved(db, connection, window), window)
