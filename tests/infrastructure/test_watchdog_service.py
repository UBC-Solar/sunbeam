import uuid
from datetime import timedelta

import pytest

pytest.importorskip("docker")

from db.sunbeamdb.models import WorkerRun, WorkerStatus
from server.services.metrics_cache import MetricsCache
from server.services.watchdog_service import WatchdogService
from tests.infrastructure.conftest import naive_utcnow
from tests.infrastructure.test_worker_service import FakeContainer, FakeDockerClient


@pytest.fixture(autouse=True)
def naive_watchdog_clock(monkeypatch):
    monkeypatch.setattr("server.services.watchdog_service.utcnow", naive_utcnow)


@pytest.fixture(autouse=True)
def clean_metrics_cache():
    MetricsCache._store.clear()
    yield
    MetricsCache._store.clear()


@pytest.fixture
def fake_docker():
    return FakeDockerClient()


@pytest.fixture
def watchdog(fake_docker, session_factory):
    return WatchdogService(
        docker_client=fake_docker,
        session_factory=session_factory,
        heartbeat_timeout_s=10.0,
        startup_grace_s=60.0,
        stop_grace_s=30.0,
    )


@pytest.fixture
def make_worker(session_factory, seeded_event):
    def _make(
        *,
        status=WorkerStatus.RUNNING,
        age_s: float = 0.0,
        heartbeat_age_s: float | None = None,
        stop_requested_age_s: float | None = None,
        container_id: str | None = None,
    ) -> uuid.UUID:
        now = naive_utcnow()
        worker = WorkerRun(
            event_id=seeded_event.event_id,
            pipeline_edition="v3_0",
            image_tag="sunbeam-worker:v3_0",
            status=status,
            container_id=container_id,
            created_at=now - timedelta(seconds=age_s),
            last_heartbeat_at=(
                now - timedelta(seconds=heartbeat_age_s)
                if heartbeat_age_s is not None
                else None
            ),
            stop_requested=stop_requested_age_s is not None,
            stop_requested_at=(
                now - timedelta(seconds=stop_requested_age_s)
                if stop_requested_age_s is not None
                else None
            ),
        )

        with session_factory() as session:
            session.add(worker)
            session.commit()
            return worker.id

    return _make


@pytest.fixture
def get_worker(session_factory):
    def _get(worker_id: uuid.UUID) -> WorkerRun:
        with session_factory() as session:
            return session.get(WorkerRun, worker_id)

    return _get


def add_container(fake_docker, **kwargs) -> FakeContainer:
    container = FakeContainer(**kwargs)
    fake_docker.containers.by_id[container.id] = container
    return container


class TestNeverLaunched:
    def test_young_worker_without_container_left_alone(
        self, watchdog, make_worker, get_worker
    ):
        worker_id = make_worker(status=WorkerStatus.REQUESTED, age_s=5)

        watchdog._sweep()

        assert get_worker(worker_id).status == WorkerStatus.REQUESTED

    def test_worker_that_never_launched_is_lost_after_grace(
        self, watchdog, make_worker, get_worker
    ):
        worker_id = make_worker(status=WorkerStatus.REQUESTED, age_s=120)

        watchdog._sweep()

        worker = get_worker(worker_id)
        assert worker.status == WorkerStatus.LOST
        assert "never launched" in worker.status_message


class TestContainerGone:
    def test_missing_container_marks_worker_lost(
        self, watchdog, make_worker, get_worker
    ):
        worker_id = make_worker(container_id="vanished")

        watchdog._sweep()

        worker = get_worker(worker_id)
        assert worker.status == WorkerStatus.LOST
        assert "no longer exists" in worker.status_message

    def test_clean_exit_without_completion_report_is_failed(
        self, watchdog, fake_docker, make_worker, get_worker
    ):
        container = add_container(fake_docker, status="exited", exit_code=0)
        worker_id = make_worker(container_id=container.id)

        watchdog._sweep()

        worker = get_worker(worker_id)
        assert worker.status == WorkerStatus.FAILED
        assert "exited without reporting completion" in worker.status_message

    def test_nonzero_exit_is_lost_with_exit_code(
        self, watchdog, fake_docker, make_worker, get_worker
    ):
        container = add_container(fake_docker, status="exited", exit_code=137)
        worker_id = make_worker(container_id=container.id)

        watchdog._sweep()

        worker = get_worker(worker_id)
        assert worker.status == WorkerStatus.LOST
        assert "exit code 137" in worker.status_message


class TestStopGrace:
    def test_worker_ignoring_stop_request_is_killed(
        self, watchdog, fake_docker, make_worker, get_worker
    ):
        container = add_container(fake_docker, status="running")
        worker_id = make_worker(
            status=WorkerStatus.STOP_REQUESTED,
            container_id=container.id,
            heartbeat_age_s=1,
            stop_requested_age_s=60,
        )

        watchdog._sweep()

        worker = get_worker(worker_id)
        assert container.killed is True
        assert worker.status == WorkerStatus.CANCELLED
        assert "grace period" in worker.status_message

    def test_worker_within_stop_grace_not_killed(
        self, watchdog, fake_docker, make_worker, get_worker
    ):
        container = add_container(fake_docker, status="running")
        worker_id = make_worker(
            status=WorkerStatus.STOP_REQUESTED,
            container_id=container.id,
            heartbeat_age_s=1,
            stop_requested_age_s=5,
        )

        watchdog._sweep()

        worker = get_worker(worker_id)
        assert container.killed is False
        assert worker.status == WorkerStatus.STOP_REQUESTED


class TestHeartbeatTimeout:
    def test_stale_heartbeat_marks_worker_lost(
        self, watchdog, fake_docker, make_worker, get_worker
    ):
        container = add_container(fake_docker, status="running")
        worker_id = make_worker(container_id=container.id, heartbeat_age_s=60)

        watchdog._sweep()

        worker = get_worker(worker_id)
        assert worker.status == WorkerStatus.LOST
        assert "stopped heartbeating" in worker.status_message

    def test_fresh_heartbeat_left_alone(
        self, watchdog, fake_docker, make_worker, get_worker
    ):
        container = add_container(fake_docker, status="running")
        worker_id = make_worker(container_id=container.id, heartbeat_age_s=1)

        watchdog._sweep()

        assert get_worker(worker_id).status == WorkerStatus.RUNNING


class TestStartupGrace:
    def test_starting_worker_with_container_lost_after_grace(
        self, watchdog, fake_docker, make_worker, get_worker
    ):
        container = add_container(fake_docker, status="running")
        worker_id = make_worker(
            status=WorkerStatus.STARTING, container_id=container.id, age_s=120
        )

        watchdog._sweep()

        worker = get_worker(worker_id)
        assert worker.status == WorkerStatus.LOST
        assert "never became healthy" in worker.status_message

    def test_starting_worker_within_grace_left_alone(
        self, watchdog, fake_docker, make_worker, get_worker
    ):
        container = add_container(fake_docker, status="running")
        worker_id = make_worker(
            status=WorkerStatus.STARTING, container_id=container.id, age_s=5
        )

        watchdog._sweep()

        assert get_worker(worker_id).status == WorkerStatus.STARTING


class TestResolveSideEffects:
    def test_resolution_clears_metrics_cache(
        self, watchdog, make_worker, get_worker
    ):
        worker_id = make_worker(container_id="vanished")
        MetricsCache.set(worker_id, {"idle_pct": 1.0})

        watchdog._sweep()

        assert MetricsCache.get(worker_id) is None

    def test_terminal_worker_is_not_touched(
        self, watchdog, session_factory, make_worker, get_worker
    ):
        worker_id = make_worker(status=WorkerStatus.COMPLETED, container_id="vanished")

        watchdog._sweep()

        # Terminal workers are excluded from the sweep entirely.
        assert get_worker(worker_id).status == WorkerStatus.COMPLETED
