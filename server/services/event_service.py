"""
Creating, editing and deleting events. The database is the only place an event is defined: workers read theirs from
it when they start.
"""
from datetime import datetime

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from config.signals import add_signals
from db.sunbeamdb.models import Event, EventStatus, Sample, Signal, Vehicle, WorkerRun
from db.timeutil import as_utc
from server.errors import Conflict
from server.services.worker_service import check_no_active_run
from stage.stage_library import INGRESS_STAGES, StageLibrary

EDITABLE_FIELDS = ("name", "vehicle_id", "description", "starts_at", "ends_at", "pipeline_edition", "stages")
# The rest can't be cleared
NULLABLE_FIELDS = ("description", "ends_at")


def get_event(db: Session, event_id: int) -> Event:
    event = db.get(Event, event_id)
    if event is None:
        raise LookupError("Event not found")
    return event


def create_event(
    db: Session,
    *,
    name: str,
    vehicle_id: int,
    starts_at: datetime,
    pipeline_edition: str,
    stages: list[str],
    ends_at: datetime | None = None,
    description: str | None = None,
) -> Event:
    event = Event(status=EventStatus.UNPROCESSED)
    _apply(db, event, {
        "name": name, "vehicle_id": vehicle_id, "description": description, "starts_at": starts_at,
        "ends_at": ends_at, "pipeline_edition": pipeline_edition, "stages": stages,
    })
    db.add(event)
    db.flush()

    add_signals(db, event)
    db.commit()
    db.refresh(event)
    return event


def update_event(db: Session, event_id: int, changes: dict) -> Event:
    """
    Changes take effect on the event's next run. A replay rewrites everything; a realtime run appends to what's
    there, so launch it with reprocess to start clean after changing its stages.
    """
    event = get_event(db, event_id)
    check_no_active_run(db, event)

    for field, value in changes.items():
        if value is None and field not in NULLABLE_FIELDS:
            raise ValueError(f"{field} can't be empty.")

    _apply(db, event, {field: changes[field] for field in EDITABLE_FIELDS if field in changes})

    # A new date can localize signals the old one didn't; the old ones stay, since samples may reference them
    add_signals(db, event)
    db.commit()
    db.refresh(event)
    return event


def delete_event(db: Session, event_id: int) -> None:
    """ Deletes the event with all of its samples, signals and worker run history. """
    event = get_event(db, event_id)
    check_no_active_run(db, event)

    db.execute(delete(Sample).where(Sample.event_id == event.id))
    db.execute(delete(Signal).where(Signal.event_id == event.id))
    db.execute(delete(WorkerRun).where(WorkerRun.event_id == event.id))
    db.delete(event)
    db.commit()


def _apply(db: Session, event: Event, fields: dict) -> None:
    """ Set the fields on the event, then check the result as a whole. """
    # The checks query the session; an edit mustn't be flushed (and hit the database's own constraints) before then
    with db.no_autoflush:
        _apply_and_check(db, event, fields)


def _apply_and_check(db: Session, event: Event, fields: dict) -> None:
    for field, value in fields.items():
        if field in ("starts_at", "ends_at") and value is not None:
            value = as_utc(value)
        if field in ("name", "description") and value is not None:
            value = value.strip() or None
        setattr(event, field, value)

    if not event.name:
        raise ValueError("An event needs a name.")
    taken = db.scalar(select(Event).where(Event.name == event.name, Event.id != event.id))
    if taken is not None:
        raise Conflict(f"An event named {event.name!r} already exists.")

    if db.get(Vehicle, event.vehicle_id) is None:
        raise ValueError(f"No vehicle with id {event.vehicle_id}.")

    if event.ends_at is not None and as_utc(event.ends_at) <= as_utc(event.starts_at):
        raise ValueError("The event must end after it starts.")

    if event.pipeline_edition not in StageLibrary().pipeline_editions:
        raise ValueError(f"Unknown pipeline edition {event.pipeline_edition!r}.")

    # Stages the server's own copy of the code doesn't have are allowed: they may exist in a git ref the event will be
    # launched from (trying out a new stage). A worker without them fails, saying so.
    event.stages = list(dict.fromkeys(stage.strip() for stage in event.stages if stage.strip()))
    if not event.stages:
        raise ValueError("Choose at least one stage.")
    ingress = [stage for stage in event.stages if stage in INGRESS_STAGES]
    if ingress:
        raise ValueError(f"{', '.join(ingress)} can't be chosen: the pipeline adds ingress itself.")
