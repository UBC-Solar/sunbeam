import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from db import create_schema
from db.sunbeamdb.models import AlignedSample, EventStatus, Event, WorkerRun, WorkerStatus
from db.sunbeamdb.writer import EventWriter
from state.frame import FrameView

pytestmark = pytest.mark.postgres


class TestCreateSchema:
    def test_hypertables_created(self, pg_engine):
        with pg_engine.connect() as conn:
            hypertables = set(
                conn.scalars(
                    text(
                        "SELECT hypertable_name FROM timescaledb_information.hypertables"
                    )
                )
            )

        assert {"raw_sample", "aligned_sample"} <= hypertables

    def test_create_schema_is_idempotent(self, pg_engine):
        # The fixture already ran create_schema once; a second run must not
        # raise (IF NOT EXISTS everywhere).
        create_schema(pg_engine)

    def test_worker_run_uuid_primary_key_round_trips(self, pg_engine, pg_seeded_event):
        with Session(pg_engine) as session:
            worker = WorkerRun(
                event_id=pg_seeded_event.event_id,
                pipeline_edition="v3_0",
                image_tag="sunbeam-worker:v3_0",
                status=WorkerStatus.REQUESTED,
            )
            session.add(worker)
            session.commit()
            worker_id = worker.id

        assert isinstance(worker_id, uuid.UUID)

        with Session(pg_engine) as session:
            fetched = session.get(WorkerRun, worker_id)
            assert fetched is not None
            assert fetched.status == WorkerStatus.REQUESTED
            # server_default=func.now() must fire and come back aware.
            assert fetched.created_at.tzinfo is not None

    def test_bigint_autoincrement_pks_work(self, pg_engine, pg_seeded_event):
        with Session(pg_engine) as session:
            event = session.get(Event, pg_seeded_event.event_id)
            assert event.id == pg_seeded_event.event_id
            assert event.status == EventStatus.UNPROCESSED


class TestEventWriterOnHypertable:
    def test_frames_persist_into_aligned_sample(self, pg_engine, pg_seeded_event):
        writer = EventWriter(pg_seeded_event.event_name, pg_engine)
        ts = datetime(2026, 7, 1, 12, 0, 1, tzinfo=timezone.utc)

        writer.write_frame(FrameView(ts, {"speed": 10.0, "power": 200.0}))
        writer.close()

        with Session(pg_engine) as session:
            samples = session.scalars(select(AlignedSample)).all()
            event = session.get(Event, pg_seeded_event.event_id)

        assert len(samples) == 2
        assert all(sample.ts == ts for sample in samples)
        assert all(sample.ts.tzinfo is not None for sample in samples)
        assert event.status == EventStatus.PROCESSED
