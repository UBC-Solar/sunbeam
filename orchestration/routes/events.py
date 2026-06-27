from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import Event
from orchestration.deps import get_db


router = APIRouter(prefix="/events", tags=["events"])


@router.get("")
def list_events(db: Session = Depends(get_db)):
    events = db.scalars(
        select(Event).order_by(Event.starts_at.desc())
    ).all()

    return [
        {
            "id": event.id,
            "name": event.name,
            "vehicle_id": event.vehicle_id,
            "starts_at": event.starts_at,
            "ends_at": event.ends_at,
            "pipeline_edition": event.pipeline_edition,
            "status": event.status.value,
            "description": event.description,
        }
        for event in events
    ]


@router.get("/{event_id}")
def get_event(event_id: int, db: Session = Depends(get_db)):
    event = db.get(Event, event_id)

    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")

    return {
        "id": event.id,
        "name": event.name,
        "vehicle_id": event.vehicle_id,
        "starts_at": event.starts_at,
        "ends_at": event.ends_at,
        "pipeline_edition": event.pipeline_edition,
        "status": event.status.value,
        "description": event.description,
    }