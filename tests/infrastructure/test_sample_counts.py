from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from db.sunbeamdb.models import Event, EventStatus, Sample
from server.services.sample_counts import REFRESH_INTERVAL_S, SampleCounts


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def add_samples(session: Session, seeded_event, n: int, start: int = 0) -> None:
    signal_id = next(iter(seeded_event.signal_ids.values()))
    session.add_all([
        Sample(event_id=seeded_event.event_id, signal_id=signal_id,
               ts=datetime(2026, 7, 1, 12) + timedelta(seconds=start + i), value_f64=1.0)
        for i in range(n)
    ])
    session.commit()


def set_status(session: Session, event: Event, status: EventStatus) -> None:
    event.status = status
    session.commit()


def test_an_event_being_written_is_recounted_at_most_every_interval(engine, seeded_event):
    clock = Clock()
    counts = SampleCounts(monotonic=clock)

    with Session(engine) as session:
        event = session.get(Event, seeded_event.event_id)
        set_status(session, event, EventStatus.ONGOING)
        add_samples(session, seeded_event, 2)
        assert counts.count(session, event) == 2

        add_samples(session, seeded_event, 3, start=2)
        clock.now = REFRESH_INTERVAL_S - 0.1
        assert counts.count(session, event) == 2

        clock.now = REFRESH_INTERVAL_S
        assert counts.count(session, event) == 5


def test_a_finished_event_is_not_recounted_until_its_status_changes(engine, seeded_event):
    clock = Clock()
    counts = SampleCounts(monotonic=clock)

    with Session(engine) as session:
        event = session.get(Event, seeded_event.event_id)
        set_status(session, event, EventStatus.ONGOING)
        add_samples(session, seeded_event, 2)
        assert counts.count(session, event) == 2

        # The run's last writes land and it finishes before the next refresh: the status change forces a recount
        add_samples(session, seeded_event, 1, start=2)
        set_status(session, event, EventStatus.PROCESSED)
        assert counts.count(session, event) == 3

        add_samples(session, seeded_event, 1, start=3)
        clock.now = 1000.0
        assert counts.count(session, event) == 3
