import tomllib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from config import EVENTS_PATH
from db.sunbeamdb.models import Event, EventStatus, Vehicle
from db.timeutil import is_past


def _as_datetime(value: str | datetime) -> datetime:
    """
    events.toml stores timestamps as quoted ISO strings, which tomllib returns
    as str. Parse them before they reach a DateTime column: psycopg happens to
    coerce strings, but other backends (and SQLite in tests) reject them.
    """
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


@dataclass(frozen=True)
class EventConfig:
    """ One ``[[event]]`` entry of ``events.toml``. """
    name: str
    vehicle: str
    description: str | None
    stages: list[str]
    pipeline_edition: str
    starts_at: datetime
    ends_at: datetime | None  # None for ongoing events

    @classmethod
    def from_raw(cls, raw_event: dict) -> "EventConfig":
        return cls(
            name=raw_event["name"],
            vehicle=raw_event["vehicle"],
            description=raw_event.get("description"),
            stages=raw_event["stages"],
            pipeline_edition=raw_event["pipeline_edition"],
            starts_at=_as_datetime(raw_event["starts_at"]),
            ends_at=_as_datetime(raw_event["ends_at"]) if "ends_at" in raw_event else None,
        )

    def is_past(self) -> bool:
        """ True if the event has ended, so a worker replays it rather than running it live. """
        return is_past(self.ends_at)


class EventManager:
    def __init__(self, path: Path | None = None):
        with open(path or EVENTS_PATH, "rb") as f:
            self._events = [EventConfig.from_raw(raw) for raw in tomllib.load(f)["event"]]

    @property
    def events(self) -> list[EventConfig]:
        return list(self._events)

    def get_event(self, event_name: str) -> EventConfig:
        """
        :raises ValueError: Event name not found in events
        """
        for event in self._events:
            if event.name == event_name:
                return event

        raise ValueError(f"Event {event_name!r} not found!")

    def sync_events(self, engine: Engine) -> None:
        """ Create or update an Event row for every configured event, so edits to events.toml reach the database. """
        with Session(engine) as session:
            for config in self._events:
                vehicle = session.scalar(select(Vehicle).where(Vehicle.name == config.vehicle))
                if vehicle is None:
                    raise RuntimeError(f"Vehicle {config.vehicle} not found! Check `events.toml`.")

                event = session.scalar(select(Event).where(Event.name == config.name))
                if event is None:
                    event = Event(name=config.name, status=EventStatus.UNPROCESSED)
                    session.add(event)

                event.vehicle_id = vehicle.id
                event.starts_at = config.starts_at
                event.ends_at = config.ends_at
                event.description = config.description
                event.pipeline_edition = config.pipeline_edition

            session.commit()
