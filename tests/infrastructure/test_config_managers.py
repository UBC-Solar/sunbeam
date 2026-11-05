import tomllib

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from config import EVENTS_PATH, VEHICLES_PATH, EventManager, SignalManager, VehicleManager
from db.sunbeamdb.models import Event, EventStatus, Signal, Vehicle


@pytest.fixture
def raw_vehicles():
    with open(VEHICLES_PATH, "rb") as f:
        return tomllib.load(f)["vehicle"]


@pytest.fixture
def raw_events():
    with open(EVENTS_PATH, "rb") as f:
        return tomllib.load(f)["event"]


def _to_toml(events: list[dict]) -> str:
    def value(v):
        if isinstance(v, list):
            return "[" + ", ".join(value(item) for item in v) + "]"
        return f'"{v}"'

    return "\n".join(
        "[[event]]\n" + "\n".join(f"{key} = {value(v)}" for key, v in event.items()) + "\n"
        for event in events
    )


def count(engine, model) -> int:
    with Session(engine) as session:
        return session.scalar(select(func.count()).select_from(model))


class TestVehicleManager:
    def test_sync_creates_all_configured_vehicles(self, engine, raw_vehicles):
        VehicleManager().sync_vehicles(engine)

        with Session(engine) as session:
            names = set(session.scalars(select(Vehicle.name)))

        assert names == {vehicle["name"] for vehicle in raw_vehicles}

    def test_sync_is_idempotent(self, engine, raw_vehicles):
        VehicleManager().sync_vehicles(engine)
        VehicleManager().sync_vehicles(engine)

        assert count(engine, Vehicle) == len(raw_vehicles)


class TestEventManager:
    def test_sync_requires_vehicles(self, engine):
        with pytest.raises(RuntimeError, match="not found"):
            EventManager().sync_events(engine)

    def test_sync_creates_all_configured_events(self, engine, raw_events):
        VehicleManager().sync_vehicles(engine)
        EventManager().sync_events(engine)

        with Session(engine) as session:
            events = session.scalars(select(Event)).all()

        assert {event.name for event in events} == {
            event["name"] for event in raw_events
        }
        assert all(event.status == EventStatus.UNPROCESSED for event in events)

    def test_sync_is_idempotent(self, engine, raw_events):
        VehicleManager().sync_vehicles(engine)
        EventManager().sync_events(engine)
        EventManager().sync_events(engine)

        assert count(engine, Event) == len(raw_events)

    def test_sync_updates_events_edited_in_the_config(self, engine, raw_events, tmp_path):
        VehicleManager().sync_vehicles(engine)
        EventManager().sync_events(engine)

        edited = dict(raw_events[0], ends_at="2030-01-01T00:00:00Z", description="edited")
        events_path = tmp_path / "events.toml"
        events_path.write_text(_to_toml([edited]))
        EventManager(events_path).sync_events(engine)

        with Session(engine) as session:
            event = session.scalar(select(Event).where(Event.name == edited["name"]))

        assert event.description == "edited"
        assert event.ends_at.year == 2030

    def test_lookup_helpers_agree_with_toml(self, raw_events):
        manager = EventManager()
        first = raw_events[0]

        event = manager.get_event(first["name"])

        assert event.stages == first["stages"]
        assert event.pipeline_edition == first["pipeline_edition"]
        assert event.starts_at.date().isoformat() == str(first["starts_at"])[:10]

    def test_unknown_event_raises(self):
        manager = EventManager()

        with pytest.raises(ValueError, match="not found"):
            manager.get_event("no-such-event")


class TestSignalManager:
    @pytest.fixture
    def synced_engine(self, engine):
        VehicleManager().sync_vehicles(engine)
        EventManager().sync_events(engine)
        return engine

    def test_sync_creates_signals_for_events(self, synced_engine):
        SignalManager.sync_signals(synced_engine)

        with Session(synced_engine) as session:
            signals = session.scalars(select(Signal)).all()

        assert len(signals) > 0
        event_ids = {signal.event_id for signal in signals}
        with Session(synced_engine) as session:
            known_event_ids = set(session.scalars(select(Event.id)))
        assert event_ids <= known_event_ids

    def test_sync_is_idempotent(self, synced_engine):
        SignalManager.sync_signals(synced_engine)
        first_count = count(synced_engine, Signal)

        SignalManager.sync_signals(synced_engine)

        assert count(synced_engine, Signal) == first_count