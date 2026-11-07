from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from config import SignalManager
from config.events import load_event
from db.sunbeamdb.models import Event, EventStatus, Signal, Vehicle


def count(engine, model) -> int:
    with Session(engine) as session:
        return session.scalar(select(func.count()).select_from(model))


class TestLoadEvent:
    def test_loads_the_event_from_the_database(self, engine, seeded_event):
        event = load_event(engine, seeded_event.event_name)

        assert event.name == seeded_event.event_name
        assert event.pipeline_edition == "v3_0"
        assert event.stages == ["MotorPower"]
        assert event.starts_at.tzinfo is not None
        assert event.ends_at is None and not event.is_past()

    def test_unknown_event_raises(self, engine):
        with pytest.raises(LookupError, match="not found"):
            load_event(engine, "no-such-event")


class TestSignalManager:
    @pytest.fixture
    def synced_engine(self, engine):
        with Session(engine) as session:
            vehicle = Vehicle(name="Brightside")
            session.add(vehicle)
            session.flush()
            session.add(Event(
                name="FSGP_2024_Day_1", vehicle_id=vehicle.id, pipeline_edition="v3_0",
                stages=["MotorPower"], status=EventStatus.UNPROCESSED, starts_at=datetime(2024, 7, 16, 15, tzinfo=UTC),
            ))
            session.commit()
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