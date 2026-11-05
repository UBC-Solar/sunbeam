from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import TERMINAL_WORKER_STATUSES, Event, WorkerRun
from server.deps import get_db
from server.services.sample_counts import SampleCounts


router = APIRouter(prefix="/events", tags=["events"])


def get_sample_counts(request: Request) -> SampleCounts:
    return request.app.state.sample_counts


@router.get("")
def list_events(db: Session = Depends(get_db), counts: SampleCounts = Depends(get_sample_counts)):
    events = db.scalars(
        select(Event).order_by(Event.starts_at.desc())
    ).all()
    running = _running_event_ids(db)

    return [_event_read(event, event.id in running, counts.count(db, event)) for event in events]


@router.get("/{event_id}")
def get_event(event_id: int, db: Session = Depends(get_db), counts: SampleCounts = Depends(get_sample_counts)):
    event = db.get(Event, event_id)
    if event is None:
        raise LookupError("Event not found")
    return _event_read(event, event.id in _running_event_ids(db), counts.count(db, event))


def _running_event_ids(db: Session) -> set[int]:
    return set(db.scalars(select(WorkerRun.event_id).where(WorkerRun.status.not_in(TERMINAL_WORKER_STATUSES))))


def _event_read(event: Event, running: bool, sample_count: int) -> dict:
    return {
        "id": event.id,
        "name": event.name,
        "vehicle_id": event.vehicle_id,
        "starts_at": event.starts_at,
        "ends_at": event.ends_at,
        "pipeline_edition": event.pipeline_edition,
        "status": event.status.value,
        "description": event.description,
        # A worker launched for a past event replays it, always wiping its earlier samples first
        "is_replay": event.is_past(),
        # Whether a worker run for this event hasn't finished yet
        "running": running,
        # Samples stored for the event; refreshed at most every few seconds while it's being written
        "sample_count": sample_count,
    }
