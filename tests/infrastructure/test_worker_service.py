import uuid

import pytest

pytest.importorskip("docker")

from docker.errors import DockerException, NotFound

from db.sunbeamdb.models import WorkerKind, WorkerRun, WorkerStatus
from server.services.metrics_cache import MetricsCache
from server.services.worker_service import WorkerService
from tests.infrastructure.conftest import naive_utcnow


class FakeContainer:
    def __init__(self, container_id=None, status="running", exit_code=None):
        self.id = container_id or f"container-{uuid.uuid4().hex[:8]}"
        self.status = status
        self.attrs = {"State": {"ExitCode": exit_code}}
        self.killed = False

    def kill(self):
        self.killed = True

    def logs(self, **kwargs):
        if kwargs.get("stream"):
            return iter([b"line-1\n", b"line-2\n"])
        return b"line-1\nline-2"


class FakeContainers:
    def __init__(self):
        self.run_calls = []
        self.fail_run_with = None
        self.by_id = {}

    def run(self, **kwargs):
        if self.fail_run_with is not None:
            raise self.fail_run_with

        self.run_calls.append(kwargs)
        container = FakeContainer()
        self.by_id[container.id] = container
        return container

    def get(self, container_id):
        if container_id not in self.by_id:
            raise NotFound(f"No such container: {container_id}")
        return self.by_id[container_id]


class FakeDockerClient:
    def __init__(self):
        self.containers = FakeContainers()


@pytest.fixture(autouse=True)
def clean_metrics_cache():
    MetricsCache._store.clear()
    yield
    MetricsCache._store.clear()


@pytest.fixture(autouse=True)
def naive_service_clock(monkeypatch):
    # SQLite round-trips naive datetimes; keep the service's clock naive so
    # stored values stay comparable in assertions.
    monkeypatch.setattr("server.services.worker_service.utcnow", naive_utcnow)


@pytest.fixture
def fake_docker():
    return FakeDockerClient()


@pytest.fixture
def service(fake_docker):
    return WorkerService(fake_docker, worker_network=None)


@pytest.fixture
def db(session_factory):
    session = session_factory()
    yield session
    session.close()


def make_worker(db, seeded_event, *, status=WorkerStatus.RUNNING, **overrides) -> WorkerRun:
    worker = WorkerRun(
        event_id=seeded_event.event_id,
        pipeline_edition="v3_0",
        image_tag="sunbeam-worker:v3_0",
        status=status,
        **overrides,
    )
    db.add(worker)
    db.commit()
    db.refresh(worker)
    return worker


class TestLaunchWorker:
    def test_launch_success(self, service, fake_docker, db, seeded_event):
        worker = service.launch_worker(
            db, event_id=seeded_event.event_id, pipeline_edition="v3_0"
        )

        assert worker.status == WorkerStatus.STARTING
        assert worker.container_id is not None
        assert worker.container_name.startswith("sunbeam-worker-")
        assert worker.started_at is not None

        run_kwargs = fake_docker.containers.run_calls[0]
        assert run_kwargs["image"] == "sunbeam-worker:v3_0"
        assert run_kwargs["environment"]["SUNBEAM_EVENT_NAME"] == seeded_event.event_name
        assert run_kwargs["environment"]["SUNBEAM_WORKER_RUN_ID"] == str(worker.id)
        assert run_kwargs["labels"]["sunbeam.kind"] == "worker"

    def test_launch_docker_failure_marks_worker_failed(
        self, service, fake_docker, db, seeded_event
    ):
        fake_docker.containers.fail_run_with = DockerException("daemon down")

        worker = service.launch_worker(
            db, event_id=seeded_event.event_id, pipeline_edition="v3_0"
        )

        assert worker.status == WorkerStatus.FAILED
        assert "Failed to launch Docker container" in worker.failure_reason
        assert worker.stopped_at is not None

    def test_launch_unknown_pipeline_edition_rejected(self, service, db, seeded_event):
        with pytest.raises(ValueError, match="Unknown pipeline edition"):
            service.launch_worker(
                db, event_id=seeded_event.event_id, pipeline_edition="nope"
            )

    def test_launch_unknown_event_rejected(self, service, db, seeded_event):
        with pytest.raises(ValueError, match="No event exists"):
            service.launch_worker(db, event_id=999_999, pipeline_edition="v3_0")


class TestRegisterWorker:
    def test_register_creates_external_worker(self, service, db, seeded_event):
        worker = service.register_worker(
            db,
            event_name=seeded_event.event_name,
            pipeline_edition="v3_0",
            host="joshuas-laptop",
        )

        assert worker.kind == WorkerKind.EXTERNAL
        assert worker.status == WorkerStatus.STARTING
        assert worker.event_id == seeded_event.event_id
        assert worker.image_tag is None
        assert worker.container_id is None
        assert worker.host == "joshuas-laptop"
        assert worker.started_at is not None

    def test_registered_worker_can_heartbeat_and_complete(self, service, db, seeded_event):
        worker = service.register_worker(
            db,
            event_name=seeded_event.event_name,
            pipeline_edition="v3_0",
            host=None,
        )

        heartbeated = service.heartbeat(
            db,
            worker_id=worker.id,
            status=WorkerStatus.RUNNING,
            current_stage="compute",
            status_message=None,
            host=None,
        )
        assert heartbeated.status == WorkerStatus.RUNNING

        completed = service.complete(
            db, worker_id=worker.id, success=True, message="done"
        )
        assert completed.status == WorkerStatus.COMPLETED

    def test_register_unknown_event_rejected(self, service, db, seeded_event):
        with pytest.raises(ValueError, match="No event exists"):
            service.register_worker(
                db, event_name="no-such-event", pipeline_edition="v3_0", host=None
            )

    def test_register_unknown_edition_rejected(self, service, db, seeded_event):
        with pytest.raises(ValueError, match="Unknown pipeline edition"):
            service.register_worker(
                db,
                event_name=seeded_event.event_name,
                pipeline_edition="nope",
                host=None,
            )


class TestHeartbeat:
    def test_heartbeat_updates_status_and_timestamps(self, service, db, seeded_event):
        worker = make_worker(db, seeded_event, status=WorkerStatus.STARTING)

        updated = service.heartbeat(
            db,
            worker_id=worker.id,
            status=WorkerStatus.RUNNING,
            current_stage="ingress",
            status_message="all good",
            host="worker-host",
        )

        assert updated.status == WorkerStatus.RUNNING
        assert updated.current_stage == "ingress"
        assert updated.status_message == "all good"
        assert updated.host == "worker-host"
        assert updated.last_heartbeat_at is not None

    def test_heartbeat_after_stop_request_forces_stop_requested_status(
        self, service, db, seeded_event
    ):
        worker = make_worker(db, seeded_event, stop_requested=True)

        updated = service.heartbeat(
            db,
            worker_id=worker.id,
            status=WorkerStatus.RUNNING,
            current_stage=None,
            status_message=None,
            host=None,
        )

        assert updated.status == WorkerStatus.STOP_REQUESTED

    def test_heartbeat_cannot_resurrect_terminal_worker(self, service, db, seeded_event):
        worker = make_worker(db, seeded_event, status=WorkerStatus.FAILED)

        updated = service.heartbeat(
            db,
            worker_id=worker.id,
            status=WorkerStatus.RUNNING,
            current_stage=None,
            status_message=None,
            host=None,
        )

        assert updated.status == WorkerStatus.FAILED
        assert updated.last_heartbeat_at is None

    def test_heartbeat_unknown_worker_returns_none(self, service, db):
        assert (
            service.heartbeat(
                db,
                worker_id=uuid.uuid4(),
                status=WorkerStatus.RUNNING,
                current_stage=None,
                status_message=None,
                host=None,
            )
            is None
        )


class TestPermission:
    def test_active_worker_is_allowed(self, service, db, seeded_event):
        worker = make_worker(db, seeded_event)

        assert service.permission_for(db, worker.id) == (True, None, False)

    def test_unknown_worker_is_denied(self, service, db):
        allowed, reason, stop_requested = service.permission_for(db, uuid.uuid4())

        assert allowed is False
        assert reason == "Unknown worker."
        assert stop_requested is True

    def test_stop_requested_worker_is_denied(self, service, db, seeded_event):
        worker = make_worker(db, seeded_event, stop_requested=True)

        allowed, reason, stop_requested = service.permission_for(db, worker.id)

        assert allowed is False
        assert stop_requested is True

    def test_terminal_worker_is_denied(self, service, db, seeded_event):
        worker = make_worker(db, seeded_event, status=WorkerStatus.COMPLETED)

        allowed, reason, _ = service.permission_for(db, worker.id)

        assert allowed is False
        assert "terminal" in reason


class TestRequestStop:
    def test_request_stop_marks_worker(self, service, db, seeded_event):
        worker = make_worker(db, seeded_event)

        updated = service.request_stop(db, worker.id)

        assert updated.stop_requested is True
        assert updated.status == WorkerStatus.STOP_REQUESTED
        assert updated.stop_requested_at is not None

    def test_request_stop_terminal_worker_is_noop(self, service, db, seeded_event):
        worker = make_worker(db, seeded_event, status=WorkerStatus.COMPLETED)

        updated = service.request_stop(db, worker.id)

        assert updated.status == WorkerStatus.COMPLETED
        assert updated.stop_requested is False


class TestComplete:
    def test_success_marks_completed(self, service, db, seeded_event):
        worker = make_worker(db, seeded_event)

        updated = service.complete(
            db, worker_id=worker.id, success=True, message="done"
        )

        assert updated.status == WorkerStatus.COMPLETED
        assert updated.status_message == "done"
        assert updated.failure_reason is None
        assert updated.stopped_at is not None

    def test_failure_marks_failed_with_reason(self, service, db, seeded_event):
        worker = make_worker(db, seeded_event)

        updated = service.complete(
            db, worker_id=worker.id, success=False, message="crashed"
        )

        assert updated.status == WorkerStatus.FAILED
        assert updated.failure_reason == "crashed"

    def test_complete_clears_metrics(self, service, db, seeded_event):
        worker = make_worker(db, seeded_event)
        service.record_metrics(db, worker_id=worker.id, payload={"idle_pct": 5.0})
        assert service.get_metrics(worker.id) is not None

        service.complete(db, worker_id=worker.id, success=True, message=None)

        assert service.get_metrics(worker.id) is None


class TestMetrics:
    def test_metrics_recorded_for_active_worker(self, service, db, seeded_event):
        worker = make_worker(db, seeded_event)

        accepted = service.record_metrics(
            db, worker_id=worker.id, payload={"idle_pct": 42.0}
        )

        assert accepted is True
        metrics = service.get_metrics(worker.id)
        assert metrics["idle_pct"] == 42.0
        assert "reported_at" in metrics

    def test_metrics_rejected_for_terminal_worker(self, service, db, seeded_event):
        worker = make_worker(db, seeded_event, status=WorkerStatus.FAILED)

        accepted = service.record_metrics(
            db, worker_id=worker.id, payload={"idle_pct": 42.0}
        )

        assert accepted is False
        assert service.get_metrics(worker.id) is None


class TestLogs:
    def test_get_logs_from_live_container(self, service, fake_docker, db, seeded_event):
        worker = service.launch_worker(
            db, event_id=seeded_event.event_id, pipeline_edition="v3_0"
        )

        lines = service.get_logs(db, worker.id)

        assert lines == ["line-1", "line-2"]

    def test_get_logs_without_container_returns_none(self, service, db, seeded_event):
        worker = make_worker(db, seeded_event)

        assert service.get_logs(db, worker.id) is None

    def test_get_logs_when_container_vanished_returns_none(
        self, service, db, seeded_event
    ):
        worker = make_worker(db, seeded_event, container_id="gone")

        assert service.get_logs(db, worker.id) is None