from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import TERMINAL_WORKER_STATUSES, Event, WorkerRun
from server.deps import get_db
from server.schemas import EventCreate, EventRead, EventUpdate
from server.services import event_service
from server.services.sample_counts import SampleCounts
from stage.stage_library import StageLibrary


router = APIRouter(prefix="/events", tags=["events"])


def get_sample_counts(request: Request) -> SampleCounts:
    return request.app.state.sample_counts


@router.get("", response_model=list[EventRead])
def list_events(db: Session = Depends(get_db), counts: SampleCounts = Depends(get_sample_counts)):
    """
    Newest-starting first. `status` is `ongoing` from when a worker starts writing the event and `processed` once it
    has finished, whether or not the run succeeded: that's the status of its worker run (`GET /workers`).
    """
    events = db.scalars(select(Event).order_by(Event.starts_at.desc())).all()
    running = _running_event_ids(db)
    return [_event_read(event, event.id in running, counts.count(db, event)) for event in events]


@router.post("", response_model=EventRead, status_code=201)
def create_event(
    payload: EventCreate,
    db: Session = Depends(get_db),
    counts: SampleCounts = Depends(get_sample_counts),
):
    """
    Creates the event and its signals. 409 if the name is taken; 400 if the vehicle or edition doesn't exist, there
    are no stages or one is ingress, or it ends before it starts.

    Stages needn't be in the server's copy of the edition (`GET /pipeline-editions`): a new stage can be tried by
    launching the event from a git ref that has it. Launching without a ref refuses stages the server lacks, and a
    worker missing one fails, naming it.
    """
    event = event_service.create_event(db, **payload.model_dump())
    return _event_read(event, False, counts.count(db, event))


@router.get("/{event_id}", response_model=EventRead)
def get_event(event_id: int, db: Session = Depends(get_db), counts: SampleCounts = Depends(get_sample_counts)):
    event = event_service.get_event(db, event_id)
    return _event_read(event, event.id in _running_event_ids(db), counts.count(db, event))


@router.patch("/{event_id}", response_model=EventRead)
def update_event(
    event_id: int,
    payload: EventUpdate,
    db: Session = Depends(get_db),
    counts: SampleCounts = Depends(get_sample_counts),
):
    """
    Only the fields sent change; send `"ends_at": null` to make the event ongoing. Checked as for create, and 409
    while the event has an active worker. Applies from the next run: a replay rewrites everything, but a realtime run
    appends, so launch it with `reprocess` after changing its stages.
    """
    event = event_service.update_event(db, event_id, payload.model_dump(exclude_unset=True))
    return _event_read(event, False, counts.count(db, event))


@router.delete("/{event_id}", status_code=204)
def delete_event(event_id: int, db: Session = Depends(get_db)):
    """ Deletes the event with all of its samples, signals and worker runs. 409 while it has an active worker. """
    event_service.delete_event(db, event_id)
    return Response(status_code=204)


def _running_event_ids(db: Session) -> set[int]:
    return set(db.scalars(select(WorkerRun.event_id).where(WorkerRun.status.not_in(TERMINAL_WORKER_STATUSES))))


def _registered_stages(pipeline_edition: str) -> list[str]:
    try:
        return StageLibrary(pipeline_edition).stage_names
    except ValueError:  # An edition since removed from the registry
        return []


def _event_read(event: Event, running: bool, sample_count: int) -> EventRead:
    return EventRead(
        id=event.id,
        name=event.name,
        vehicle_id=event.vehicle_id,
        description=event.description,
        starts_at=event.starts_at,
        ends_at=event.ends_at,
        pipeline_edition=event.pipeline_edition,
        stages=event.stages,
        unregistered_stages=[stage for stage in event.stages if stage not in _registered_stages(event.pipeline_edition)],
        status=event.status,
        is_replay=event.is_past(),
        running=running,
        sample_count=sample_count,
    )
