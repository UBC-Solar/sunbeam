import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from config import EVENTS_PATH
from db.sunbeamdb.models import Event, Vehicle
from db.sunbeamdb.seed_data import get_or_create_event


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
    stages: list[str]
    pipeline_edition: str
    starts_at: datetime
    ends_at: datetime | None  # None for ongoing events

    @classmethod
    def from_raw(cls, raw_event: dict) -> "EventConfig":
        return cls(
            name=raw_event["name"],
            stages=raw_event["stages"],
            pipeline_edition=raw_event["pipeline_edition"],
            starts_at=_as_datetime(raw_event["starts_at"]),
            ends_at=_as_datetime(raw_event["ends_at"]) if "ends_at" in raw_event else None,
        )

    def is_past(self) -> bool:
        """ True if the event has an end date and it precedes the current time. """
        return self.ends_at is not None and self.ends_at < datetime.now(UTC)


class EventManager:
    def __init__(self):
        with open(EVENTS_PATH, "rb") as f:
            self._raw_events: list[dict] = tomllib.load(f)["event"]
            self._events: dict[str, Event] = {}

    def get_event(self, event_name: str) -> EventConfig:
        '''
        Returns the configuration of an event

        :param str event_name: Event name
        :return EventConfig: The event's configuration

        :raises ValueError: Event name not found in events
        '''
        for raw_event in self._raw_events:
            if raw_event["name"] == event_name:
                return EventConfig.from_raw(raw_event)

        raise ValueError(f"Event {event_name} not found!")

    def sync_events(self, engine: Engine) -> dict[str, Event]:
        with Session(engine) as session:
            for raw_event in self._raw_events:
                vehicle_name = raw_event["vehicle"]
                vehicle = session.execute(select(Vehicle).where(Vehicle.name == vehicle_name)).scalar_one_or_none()

                if not vehicle:
                    raise RuntimeError(f"Vehicle {vehicle_name} not found! Check `events.toml`.")

                raw_ends_at = raw_event.get("ends_at")
                event = get_or_create_event(
                    session,
                    name=raw_event["name"],
                    vehicle_id=vehicle.id,
                    starts_at=_as_datetime(raw_event["starts_at"]),
                    ends_at=_as_datetime(raw_ends_at) if raw_ends_at is not None else None,
                    description=raw_event["description"],
                    pipeline_edition=raw_event["pipeline_edition"]
                )

                self._events[event.name] = event

                session.add(event)
                session.flush()

            session.commit()

        return self._events
