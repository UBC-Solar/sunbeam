from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import Event
from db.timeutil import as_utc, is_past


@dataclass(frozen=True)
class EventConfig:
    """ What a worker needs to know about its event, read from the database once at startup. """
    name: str
    description: str | None
    stages: list[str]
    pipeline_edition: str
    starts_at: datetime
    ends_at: datetime | None  # None for ongoing events

    @classmethod
    def from_row(cls, event: Event) -> "EventConfig":
        return cls(
            name=event.name,
            description=event.description,
            stages=list(event.stages),
            pipeline_edition=event.pipeline_edition,
            # SQLite (the default test suite) drops tzinfo; stored values are UTC
            starts_at=as_utc(event.starts_at),
            ends_at=as_utc(event.ends_at) if event.ends_at is not None else None,
        )

    def is_past(self) -> bool:
        """ True if the event has ended, so a worker replays it rather than running it live. """
        return is_past(self.ends_at)


def load_event(engine: Engine, event_name: str) -> EventConfig:
    """
    :raises LookupError: No event has that name
    """
    with Session(engine) as session:
        event = session.scalar(select(Event).where(Event.name == event_name))
        if event is None:
            raise LookupError(f"Event {event_name!r} not found. Events are created in the dashboard.")
        return EventConfig.from_row(event)
