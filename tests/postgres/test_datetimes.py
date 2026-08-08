"""
Timezone-aware datetime behavior that SQLite cannot exercise: the services
run with real aware utcnow() here — no naive-clock monkeypatching.
"""
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("docker")

from sqlalchemy.orm import Session

from db.sunbeamdb.models import WorkerRun, WorkerStatus
from server.services.watchdog_service import WatchdogService
from server.services.worker_service import WorkerService
from tests.infrastructure.test_worker_service import FakeDockerClient

pytestmark = pytest.mark.postgres


def make_worker(session_factory, seeded_event, **overrides) -> WorkerRun:
    worker = WorkerRun(
        event_id=seeded_event.event_id,
        pipeline_edition="v3_0",
        image_tag="sunbeam-worker:v3_0",
        status=overrides.pop("status", WorkerStatus.RUNNING),
        **overrides,
    )
    with session_factory() as session:
        session.add(worker)
        session.commit()
        session.refresh(worker)
        session.expunge(worker)
        return worker


class TestAwareDatetimeRoundTrips:
    def test_heartbeat_timestamp_is_aware_and_current(
        self, pg_engine, pg_session_factory, pg_seeded_event
    ):
        worker = make_worker(pg_session_factory, pg_seeded_event)
        service = WorkerService(FakeDockerClient(), worker_network=None)

        with pg_session_factory() as db:
            service.heartbeat(
                db,
                worker_id=worker.id,
                status=WorkerStatus.RUNNING,
                current_stage=None,
                status_message=None,
                host=None,
            )

        with Session(pg_engine) as session:
            stored = session.get(WorkerRun, worker.id).last_heartbeat_at

        assert stored.tzinfo is not None
        assert abs(datetime.now(timezone.utc) - stored) < timedelta(seconds=30)


class TestDataQueryTimezones:
    def test_window_in_any_offset_matches_utc(
        self, pg_engine, pg_session_factory, pg_seeded_event
    ):
        from sqlalchemy.orm import Session

        from db.sunbeamdb.models import AlignedSample
        from server.services.data_service import get_signal, query_samples

        base = datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc)
        with Session(pg_engine) as session:
            for i in range(5):
                session.add(
                    AlignedSample(
                        event_id=pg_seeded_event.event_id,
                        ts=base + timedelta(seconds=i),
                        signal_id=pg_seeded_event.signal_ids["speed"],
                        value_f64=float(i),
                    )
                )
            session.commit()

        pacific = timezone(timedelta(hours=-7))
        start_utc = base + timedelta(seconds=1)
        end_utc = base + timedelta(seconds=4)

        with pg_session_factory() as db:
            signal = get_signal(db, pg_seeded_event.event_name, "speed")

            utc_result = query_samples(db, signal, start_utc, end_utc)
            offset_result = query_samples(
                db, signal, start_utc.astimezone(pacific), end_utc.astimezone(pacific)
            )

        # Same instants expressed in a different offset select the same rows.
        assert utc_result == offset_result
        assert utc_result[1] == [1.0, 2.0, 3.0]


class TestWatchdogWithAwareClock:
    def test_startup_grace_arithmetic_with_aware_datetimes(
        self, pg_session_factory, pg_seeded_event
    ):
        expired = make_worker(
            pg_session_factory,
            pg_seeded_event,
            status=WorkerStatus.REQUESTED,
            created_at=datetime.now(timezone.utc) - timedelta(seconds=120),
        )
        fresh = make_worker(
            pg_session_factory,
            pg_seeded_event,
            status=WorkerStatus.REQUESTED,
            created_at=datetime.now(timezone.utc),
        )

        watchdog = WatchdogService(
            docker_client=FakeDockerClient(),
            session_factory=pg_session_factory,
            startup_grace_s=60.0,
        )
        watchdog._sweep()

        with pg_session_factory() as session:
            assert session.get(WorkerRun, expired.id).status == WorkerStatus.LOST
            assert session.get(WorkerRun, fresh.id).status == WorkerStatus.REQUESTED
