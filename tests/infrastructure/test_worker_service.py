import uuid
from datetime import datetime

import pytest

pytest.importorskip("docker")

from docker.errors import BuildError, DockerException, ImageNotFound, NotFound

from db.sunbeamdb.models import Connection, Event, WorkerKind, WorkerRun, WorkerStatus
from server.services.connection_service import ConnectionTestFailed
from server.services.metrics_cache import MetricsCache
from tests.infrastructure.conftest import FakeChecker, make_worker_service, naive_utcnow

SHA = "0123456789abcdef0123456789abcdef01234567"


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


class FakeImages:
    def __init__(self):
        self.tags: set[str] = set()
        self.build_calls = []
        self.fail_build_with = None

    def get(self, tag):
        if tag not in self.tags:
            raise ImageNotFound(f"No such image: {tag}")
        return tag

    def build(self, **kwargs):
        self.build_calls.append(kwargs)
        if self.fail_build_with is not None:
            raise self.fail_build_with
        self.tags.add(kwargs["tag"])
        return kwargs["tag"], iter([])


class FakeDockerClient:
    def __init__(self):
        self.containers = FakeContainers()
        self.images = FakeImages()


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
def checker():
    return FakeChecker()


def fake_ls_remote(remote, ref):
    return {
        "v4_0": f"{SHA}\trefs/heads/v4_0\n{'f' * 40}\trefs/heads/feature/v4_0\n",
        "v1.0": f"{'a' * 40}\trefs/tags/v1.0\n{SHA}\trefs/tags/v1.0^{{}}\n",
    }.get(ref, "")


@pytest.fixture
def service(fake_docker, session_factory, checker):
    return make_worker_service(fake_docker, session_factory, checker=checker, ls_remote=fake_ls_remote)


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
            db, event_id=seeded_event.event_id
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

    def test_launch_defaults_to_the_default_connection_without_reprocessing(
        self, service, fake_docker, db, seeded_event,
    ):
        worker = service.launch_worker(db, event_id=seeded_event.event_id)

        assert (worker.influx_connection_id, worker.influx_connection_name) == (seeded_event.connection_id, "test-influx")
        assert worker.reprocess is False
        environment = fake_docker.containers.run_calls[0]["environment"]
        assert environment["SUNBEAM_REPROCESS"] == "false"

    def test_launch_hands_the_worker_every_connection(self, service, fake_docker, db, seeded_event):
        service.launch_worker(db, event_id=seeded_event.event_id)

        environment = fake_docker.containers.run_calls[0]["environment"]
        assert environment["SUNBEAM_INFLUX_URL"] == "http://influx.test:8086"
        assert environment["SUNBEAM_INFLUX_ORG"] == "test-org"
        assert environment["SUNBEAM_INFLUX_BUCKET"] == "test-bucket"
        assert environment["SUNBEAM_INFLUX_TOKEN"] == "test-token"
        assert environment["SUNBEAM_DATABASE_URL"] == "postgresql+psycopg://telemetry:telemetry@db:5432/telemetry"
        assert environment["SUNBEAM_SERVER_URL"] == "http://server:8000"
        assert "SUNBEAM_CONFIGURATION_PROFILE" not in environment

    def test_launch_uses_the_chosen_connection_and_reprocess(self, service, fake_docker, db, seeded_event):
        other = Connection(name="other", url="http://other:8086", organization="o", bucket="b2", token="t2")
        db.add(other)
        db.commit()

        worker = service.launch_worker(
            db, event_id=seeded_event.event_id,
            influx_connection_id=other.id, reprocess=True,
        )

        assert (worker.influx_connection_name, worker.reprocess) == ("other", True)
        environment = fake_docker.containers.run_calls[0]["environment"]
        assert environment["SUNBEAM_INFLUX_BUCKET"] == "b2"
        assert environment["SUNBEAM_REPROCESS"] == "true"

    def test_launch_unknown_connection_rejected(self, service, fake_docker, db, seeded_event):
        with pytest.raises(ValueError, match="No connection exists"):
            service.launch_worker(
                db, event_id=seeded_event.event_id, influx_connection_id=999,
            )

        assert fake_docker.containers.run_calls == []

    def test_launch_tests_the_connection_over_the_event(self, service, checker, db, seeded_event):
        db.get(Event, seeded_event.event_id).ends_at = datetime(2026, 7, 1, 13, 0, 0)
        db.commit()

        service.launch_worker(db, event_id=seeded_event.event_id)

        start, stop = checker.calls[0]["window"]
        assert (start.hour, stop.hour) == (12, 13)
        assert db.get(Connection, seeded_event.connection_id).last_test_ok is True

    def test_failing_connection_fails_fast_without_a_worker(self, service, checker, fake_docker, db, seeded_event):
        checker.fail_with = "no data in 'test-bucket'"

        with pytest.raises(ConnectionTestFailed, match="no data in 'test-bucket'"):
            service.launch_worker(db, event_id=seeded_event.event_id)

        assert fake_docker.containers.run_calls == []
        assert db.query(WorkerRun).count() == 0
        assert db.get(Connection, seeded_event.connection_id).last_test_ok is False

    def test_replay_always_reprocesses(self, service, fake_docker, db, seeded_event):
        db.get(Event, seeded_event.event_id).ends_at = datetime(2026, 7, 1, 13, 0, 0)
        db.commit()

        worker = service.launch_worker(db, event_id=seeded_event.event_id, reprocess=False)

        assert worker.reprocess is True
        assert fake_docker.containers.run_calls[0]["environment"]["SUNBEAM_REPROCESS"] == "true"

    def test_launch_docker_failure_marks_worker_failed(
        self, service, fake_docker, db, seeded_event
    ):
        fake_docker.containers.fail_run_with = DockerException("daemon down")

        worker = service.launch_worker(
            db, event_id=seeded_event.event_id
        )

        assert worker.status == WorkerStatus.FAILED
        assert "Failed to launch Docker container" in worker.failure_reason
        assert worker.stopped_at is not None

    def test_launch_runs_the_events_own_edition(self, service, fake_docker, db, seeded_event):
        worker = service.launch_worker(db, event_id=seeded_event.event_id)

        assert worker.pipeline_edition == "v3_0"
        assert fake_docker.containers.run_calls[0]["environment"]["SUNBEAM_PIPELINE_EDITION"] == "v3_0"

    def test_event_with_an_edition_the_server_lacks_is_rejected(self, service, fake_docker, db, seeded_event):
        db.get(Event, seeded_event.event_id).pipeline_edition = "nope"
        db.commit()

        with pytest.raises(ValueError, match="Unknown pipeline edition 'nope'"):
            service.launch_worker(db, event_id=seeded_event.event_id)

        assert fake_docker.containers.run_calls == []

    def test_launch_unknown_event_rejected(self, service, db, seeded_event):
        with pytest.raises(ValueError, match="No event exists"):
            service.launch_worker(db, event_id=999_999)


class TestGitSource:
    def test_branch_is_resolved_and_built_before_launch(self, service, fake_docker, db, seeded_event):
        worker = service.launch_worker(db, event_id=seeded_event.event_id, source_ref="v4_0")

        db.refresh(worker)
        assert (worker.source_ref, worker.source_sha) == ("v4_0", SHA)
        assert worker.image_tag == f"sunbeam-worker:v3_0-{SHA[:12]}"
        assert worker.status == WorkerStatus.STARTING

        build = fake_docker.images.build_calls[0]
        assert build["tag"] == worker.image_tag
        assert build["buildargs"]["GIT_REF"] == SHA
        assert build["buildargs"]["PIPELINE_EDITION"] == "v3_0"
        assert "path" not in build  # the Dockerfile clones the code; there is no local build context
        assert fake_docker.containers.run_calls[0]["image"] == worker.image_tag

    def test_annotated_tag_resolves_to_its_commit(self, service, db, seeded_event):
        worker = service.launch_worker(db, event_id=seeded_event.event_id, source_ref="v1.0")

        assert worker.source_sha == SHA

    def test_full_sha_is_used_as_is(self, service, db, seeded_event):
        sha = "b" * 40

        worker = service.launch_worker(db, event_id=seeded_event.event_id, source_ref=sha)

        assert worker.source_sha == sha

    def test_already_built_commit_skips_the_build(self, service, fake_docker, db, seeded_event):
        fake_docker.images.tags.add(f"sunbeam-worker:v3_0-{SHA[:12]}")

        worker = service.launch_worker(db, event_id=seeded_event.event_id, source_ref="v4_0")

        assert fake_docker.images.build_calls == []
        assert worker.status == WorkerStatus.STARTING

    def test_new_build_starts_as_building(self, fake_docker, session_factory, db, seeded_event):
        builds = []
        service = make_worker_service(
            fake_docker, session_factory, ls_remote=fake_ls_remote, run_in_background=builds.append,
        )

        worker = service.launch_worker(db, event_id=seeded_event.event_id, source_ref="v4_0")

        assert worker.status == WorkerStatus.BUILDING
        assert worker.container_id is None
        assert len(builds) == 1 and fake_docker.containers.run_calls == []

    def test_stop_while_building_launches_nothing(self, fake_docker, session_factory, db, seeded_event):
        builds = []
        service = make_worker_service(
            fake_docker, session_factory, ls_remote=fake_ls_remote, run_in_background=builds.append,
        )
        worker = service.launch_worker(db, event_id=seeded_event.event_id, source_ref="v4_0")
        service.request_stop(db, worker.id)

        builds[0]()

        db.refresh(worker)
        assert worker.status == WorkerStatus.CANCELLED
        assert fake_docker.containers.run_calls == []

    def test_failed_build_fails_the_worker_with_the_log(self, service, fake_docker, db, seeded_event):
        fake_docker.images.fail_build_with = BuildError(
            "The command returned a non-zero code: 1",
            [{"stream": "Step 5/7 : RUN test -f ..."}, {"error": "predates server-managed connections"}],
        )

        worker = service.launch_worker(db, event_id=seeded_event.event_id, source_ref="v4_0")

        db.refresh(worker)
        assert worker.status == WorkerStatus.FAILED
        assert "predates server-managed connections" in worker.failure_reason
        assert fake_docker.containers.run_calls == []

    def test_unknown_ref_rejected(self, service, db, seeded_event):
        with pytest.raises(ValueError, match="No branch or tag named 'nope'"):
            service.launch_worker(db, event_id=seeded_event.event_id, source_ref="nope")

    def test_short_sha_rejected_with_guidance(self, service, db, seeded_event):
        with pytest.raises(ValueError, match="full 40-character SHA"):
            service.launch_worker(db, event_id=seeded_event.event_id, source_ref="abc1234")

    def test_blank_ref_means_local(self, service, fake_docker, db, seeded_event):
        worker = service.launch_worker(db, event_id=seeded_event.event_id, source_ref="  ")

        assert worker.source_ref is None
        assert fake_docker.containers.run_calls[0]["image"] == "sunbeam-worker:v3_0"


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

    def test_register_records_the_workers_connection_and_reprocess(self, service, db, seeded_event):
        worker = service.register_worker(
            db, event_name=seeded_event.event_name, pipeline_edition="v3_0", host="laptop",
            influx_connection_name="external: http://localhost:8086 / CAN_log", reprocess=True,
        )

        assert (worker.influx_connection_name, worker.reprocess) == ("external: http://localhost:8086 / CAN_log", True)

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
            db, event_id=seeded_event.event_id
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