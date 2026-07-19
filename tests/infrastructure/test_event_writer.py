from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import AlignedSample, Event, EventStatus
from db.sunbeamdb.queued_writer import QueuedEventWriter
from db.sunbeamdb.writer import EventWriter
from state.frame import FrameView


def make_frame(timestamp: datetime, speed: float, power: float) -> FrameView:
    return FrameView(timestamp, {"speed": speed, "power": power})


def event_status(engine, event_id) -> EventStatus:
    with Session(engine) as session:
        return session.get(Event, event_id).status


def all_samples(engine) -> list[AlignedSample]:
    with Session(engine) as session:
        return session.scalars(
            select(AlignedSample).order_by(AlignedSample.ts, AlignedSample.signal_id)
        ).all()


class TestEventWriterLifecycle:
    def test_init_marks_event_ongoing(self, engine, seeded_event):
        writer = EventWriter(seeded_event.event_name, engine)

        assert event_status(engine, seeded_event.event_id) == EventStatus.ONGOING
        writer.close()

    def test_close_marks_event_processed(self, engine, seeded_event):
        writer = EventWriter(seeded_event.event_name, engine)
        writer.close()

        assert event_status(engine, seeded_event.event_id) == EventStatus.PROCESSED


class TestEventWriterPersistence:
    def test_write_frame_persists_one_row_per_signal(self, engine, seeded_event):
        writer = EventWriter(seeded_event.event_name, engine)
        ts = datetime(2026, 7, 1, 12, 0, 1)

        writer.write_frame(make_frame(ts, speed=10.0, power=200.0))
        writer.close()

        samples = all_samples(engine)
        assert len(samples) == 2

        by_signal = {sample.signal_id: sample for sample in samples}
        assert by_signal[seeded_event.signal_ids["speed"]].value_f64 == 10.0
        assert by_signal[seeded_event.signal_ids["power"]].value_f64 == 200.0
        assert all(sample.event_id == seeded_event.event_id for sample in samples)
        assert all(sample.ts == ts for sample in samples)

    def test_write_frames_persists_batch(self, engine, seeded_event):
        writer = EventWriter(seeded_event.event_name, engine)
        base_ts = datetime(2026, 7, 1, 12, 0, 0)

        frames = [
            make_frame(base_ts + timedelta(seconds=i), speed=float(i), power=2.0 * i)
            for i in range(5)
        ]
        writer.write_frames(frames)
        writer.close()

        samples = all_samples(engine)
        assert len(samples) == 5 * 2

        speed_values = [
            sample.value_f64
            for sample in samples
            if sample.signal_id == seeded_event.signal_ids["speed"]
        ]
        assert speed_values == [0.0, 1.0, 2.0, 3.0, 4.0]

    def test_write_frames_with_no_frames_is_a_noop(self, engine, seeded_event):
        writer = EventWriter(seeded_event.event_name, engine)

        writer.write_frames([])
        writer.close()

        assert all_samples(engine) == []


class TestQueuedEventWriterAgainstDatabase:
    def test_queued_frames_reach_the_database(self, engine, seeded_event):
        writer = QueuedEventWriter(
            EventWriter(seeded_event.event_name, engine),
            batch_size=2,
            flush_interval_s=0.01,
        )
        base_ts = datetime(2026, 7, 1, 12, 0, 0)

        for i in range(5):
            writer.write_frame(
                make_frame(base_ts + timedelta(seconds=i), speed=float(i), power=1.0)
            )

        writer.close()

        assert len(all_samples(engine)) == 5 * 2
        assert event_status(engine, seeded_event.event_id) == EventStatus.PROCESSED