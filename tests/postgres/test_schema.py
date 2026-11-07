import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from db.sunbeamdb.models import Event, EventStatus, Sample, WorkerRun, WorkerStatus
from db.sunbeamdb.writer import EventWriter
from state.frame import FrameView

pytestmark = pytest.mark.postgres


class TestSchema:
    def test_hypertables_created(self, pg_engine):
        with pg_engine.connect() as conn:
            hypertables = set(
                conn.scalars(
                    text(
                        "SELECT hypertable_name FROM timescaledb_information.hypertables"
                    )
                )
            )

        assert hypertables == {"sample"}

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
    def test_frames_persist_into_sample(self, pg_engine, pg_seeded_event):
        writer = EventWriter(pg_seeded_event.event_name, pg_engine)
        ts = datetime(2026, 7, 1, 12, 0, 1, tzinfo=timezone.utc)

        writer.write_frame(FrameView(ts, {"speed": 10.0, "power": 200.0}))
        writer.close()

        with Session(pg_engine) as session:
            samples = session.scalars(select(Sample)).all()
            event = session.get(Event, pg_seeded_event.event_id)

        assert len(samples) == 2
        assert all(sample.ts == ts for sample in samples)
        assert all(sample.ts.tzinfo is not None for sample in samples)
        assert event.status == EventStatus.PROCESSED

    def test_timeseries_and_plain_values_load_together(self, pg_engine, pg_seeded_event):
        """ A replay's flush: its fetched telemetry (TimeSeries) and computed values, in one COPY. """
        import numpy as np
        from data_tools.collections import TimeSeries

        start = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
        series = TimeSeries(
            np.arange(11, dtype=float), start_time=start, stop_time=start + timedelta(seconds=1),
            period=0.1, length=1.0, units="",
        )
        writer = EventWriter(pg_seeded_event.event_name, pg_engine)

        writer.write_frames([FrameView(start, {"speed": series}), FrameView(start, {"power": 200.0})])
        writer.close()

        with Session(pg_engine) as session:
            speed = session.scalars(
                select(Sample).where(Sample.signal_id == pg_seeded_event.signal_ids["speed"]).order_by(Sample.ts)
            ).all()
            power = session.scalars(select(Sample).where(Sample.signal_id == pg_seeded_event.signal_ids["power"])).all()

        assert [sample.value_f64 for sample in speed] == [float(i) for i in range(11)]
        assert (speed[0].ts, speed[-1].ts) == (start, start + timedelta(seconds=1))
        assert [(sample.ts, sample.value_f64) for sample in power] == [(start, 200.0)]

    def test_a_failed_write_leaves_nothing_behind(self, pg_engine, pg_seeded_event):
        writer = EventWriter(pg_seeded_event.event_name, pg_engine)
        ts = datetime(2026, 7, 1, 12, 0, 1, tzinfo=timezone.utc)
        writer.write_frame(FrameView(ts, {"speed": 1.0}))

        # The same sample again breaks the primary key, so the whole COPY rolls back, including the new one
        with pytest.raises(Exception, match="duplicate key"):
            writer.write_frames([FrameView(ts, {"power": 2.0}), FrameView(ts, {"speed": 1.0})])

        with Session(pg_engine) as session:
            assert len(session.scalars(select(Sample)).all()) == 1
