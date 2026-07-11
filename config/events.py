from db.sunbeamdb.models import Event, Vehicle, EventStatus
from db.sunbeamdb.seed_data import get_or_create_event
from config import EVENTS_PATH


from sqlalchemy import select, Engine
from sqlalchemy.orm import Session
import tomllib
from datetime import datetime, timezone


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
                vehicle: Vehicle = session.execute(select(Vehicle).where(Vehicle.name == vehicle_name)).scalar_one_or_none()

                if not vehicle:
                    raise RuntimeError(f"Vehicle {vehicle_name} not found! Check `events.toml`.")

                event = get_or_create_event(
                    session,
                    name=raw_event["name"],
                    vehicle_id=vehicle.id,
                    starts_at=raw_event["starts_at"],
                    ends_at=raw_event.get("ends_at"),
                    description=raw_event["description"],
                    pipeline_edition=raw_event["pipeline_edition"]
                )

                self._events[event.name] = event

                session.add(event)
                session.flush()

            session.commit()

        return self._events

    def check_if_past_event(self, event_name) -> bool:
        '''
        Finds if an event is over by checking if its end data is in the past

        :param str event_name: Event name
        
        :return bool: If the event is past or not

        :raises ValueError: Event name not found 
        '''
        for event in self._raw_events: # Very if statements, would like to rewrite
            if event["name"] == event_name: # Checks if event exists
                if "ends_at" in event: # Checks if event has an ending date
                    if datetime.fromisoformat(event["ends_at"]) < datetime.now(timezone.utc):
                        return True # End date in the past
                    else:
                        return False # End date in the future
                else:
                    return False # Event has no end date
        raise ValueError(f"Event {event_name} not found!")

    def get_event_start_date(self, event_name) -> datetime:
        '''
        Returns the starting date of an event

        :param str event_name: Event name
        :return: Starting date of event

        :raises ValueError: Event name not found in events
        '''
        for event in self._raw_events:
            if event["name"] == event_name:
                return datetime.fromisoformat(event["starts_at"])

        raise ValueError(f"Event {event_name} not found!")
    
    def get_event_end_date(self, event_name) -> datetime:
        '''
        Returns the ending date of an event

        :param str event_name: Event name
        :return: Ending date of event

        :raises ValueError: Event name not found in events
        :raises ValueError: Event does not have an end date
        '''
        for event in self._raw_events:
            if event["name"] == event_name:
                if "ends_at" in event:
                    return datetime.fromisoformat(event["ends_at"])
                else:
                    raise ValueError(f"Event {event_name} has not end date!")
        raise ValueError(f"Event {event_name} not found!")

    def get_event_pipeline_edition(self, event_name) -> str:
        for event in self._raw_events:
            if event["name"] == event_name:
                return event["pipeline_edition"]

        raise ValueError(f"Event {event_name} not found!")
