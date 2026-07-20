from db.sunbeamdb.models import Event, Vehicle
from db.sunbeamdb.seed_data import get_or_create_event
from config import EVENTS_PATH


from sqlalchemy import select, Engine
from sqlalchemy.orm import Session
import tomllib
from datetime import datetime


def _as_datetime(value: str | datetime) -> datetime:
    """
    events.toml stores timestamps as quoted ISO strings, which tomllib returns
    as str. Parse them before they reach a DateTime column: psycopg happens to
    coerce strings, but other backends (and SQLite in tests) reject them.
    """
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


class EventManager:
    def __init__(self):
        with open(EVENTS_PATH, "rb") as f:
            self._raw_events: list[dict] = tomllib.load(f)["event"]
            self._events: dict[str, Event] = {}

    def get_stages_for_event(self, event_name: str) -> list[str]:
        for event in self._raw_events:
            if event["name"] == event_name:
                return event["stages"]
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

    def get_event_date(self, event_name) -> datetime:
        for event in self._raw_events:
            if event["name"] == event_name:
                return datetime.fromisoformat(event["starts_at"])

        raise ValueError(f"Event {event_name} not found!")

    def get_event_pipeline_edition(self, event_name) -> str:
        for event in self._raw_events:
            if event["name"] == event_name:
                return event["pipeline_edition"]

        raise ValueError(f"Event {event_name} not found!")
