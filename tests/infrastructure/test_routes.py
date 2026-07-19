import uuid

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("docker")

from docker.errors import DockerException
from fastapi.testclient import TestClient

from db.sunbeamdb.models import WorkerRun, WorkerStatus
from server.deps import get_db
from server.routes.workers import get_worker_service
from server.services.metrics_cache import MetricsCache
from server.services.worker_service import WorkerService
from tests.infrastructure.conftest import naive_utcnow
from tests.infrastructure.test_worker_service import FakeDockerClient


@pytest.fixture(autouse=True)
def clean_metrics_cache():
    MetricsCache._store.clear()
    yield
    MetricsCache._store.clear()


@pytest.fixture(autouse=True)
def naive_service_clock(monkeypatch):
    monkeypatch.setattr("server.services.worker_service.utcnow", naive_utcnow)


@pytest.fixture
def fake_docker():
    return FakeDockerClient()


@pytest.fixture
def client(session_factory, fake_docker):
    from server.main import create_app

    app = create_app(lifespan=None)

    def override_get_db():
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    service = WorkerService(fake_docker, worker_network=None)

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_worker_service] = lambda: service

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def make_worker(session_factory, seeded_event):
    def _make(*, status=WorkerStatus.RUNNING, **overrides) -> uuid.UUID:
        worker = WorkerRun(
            event_id=seeded_event.event_id,
            pipeline_edition="v3_0",
            image_tag="sunbeam-worker:v3_0",
            status=status,
            **overrides,
        )
        with session_factory() as session:
            session.add(worker)
            session.commit()
            return worker.id

    return _make


def launch_worker(client, seeded_event) -> dict:
    response = client.post(
        "/workers/launch",
        json={"event_id": seeded_event.event_id, "pipeline_edition": "v3_0"},
    )
    assert response.status_code == 200
    return response.json()


METRICS_PAYLOAD = {"idle_pct": 90.0, "busy_pct": 10.0, "writer_ms": 1.5, "pipelines": []}


class TestEventRoutes:
    def test_list_events(self, client, seeded_event):
        response = client.get("/events")

        assert response.status_code == 200
        events = response.json()
        assert len(events) == 1
        assert events[0]["name"] == seeded_event.event_name
        assert events[0]["status"] == "unprocessed"
        assert events[0]["pipeline_edition"] == "v3_0"

    def test_get_event_by_id(self, client, seeded_event):
        response = client.get(f"/events/{seeded_event.event_id}")

        assert response.status_code == 200
        assert response.json()["id"] == seeded_event.event_id

    def test_get_unknown_event_is_404(self, client):
        assert client.get("/events/999999").status_code == 404


class TestPipelineEditionRoutes:
    def test_lists_registered_editions(self, client):
        response = client.get("/pipeline-editions")

        assert response.status_code == 200
        assert "v3_0" in response.json()


class TestLaunchRoute:
    def test_launch_returns_starting_worker(self, client, seeded_event, fake_docker):
        worker = launch_worker(client, seeded_event)

        assert worker["status"] == "starting"
        assert worker["event_id"] == seeded_event.event_id
        assert worker["container_id"] is not None
        assert len(fake_docker.containers.run_calls) == 1

    def test_launch_unknown_edition_is_400(self, client, seeded_event):
        response = client.post(
            "/workers/launch",
            json={"event_id": seeded_event.event_id, "pipeline_edition": "nope"},
        )

        assert response.status_code == 400
        assert "Unknown pipeline edition" in response.json()["detail"]

    def test_launch_unknown_event_is_400(self, client):
        response = client.post(
            "/workers/launch",
            json={"event_id": 999999, "pipeline_edition": "v3_0"},
        )

        assert response.status_code == 400
        assert "No event exists" in response.json()["detail"]


class TestListWorkersRoute:
    def test_lists_all_workers(self, client, make_worker):
        make_worker(status=WorkerStatus.RUNNING)
        make_worker(status=WorkerStatus.COMPLETED)

        response = client.get("/workers")

        assert response.status_code == 200
        assert len(response.json()) == 2

    def test_active_only_filters_terminal_workers(self, client, make_worker):
        active_id = make_worker(status=WorkerStatus.RUNNING)
        make_worker(status=WorkerStatus.COMPLETED)
        make_worker(status=WorkerStatus.FAILED)

        response = client.get("/workers", params={"active_only": True})

        workers = response.json()
        assert [w["id"] for w in workers] == [str(active_id)]


class TestWorkerLifecycleRoutes:
    def test_heartbeat_updates_worker(self, client, make_worker):
        worker_id = make_worker(status=WorkerStatus.STARTING)

        response = client.post(
            f"/workers/{worker_id}/heartbeat",
            json={"status": "running", "current_stage": "ingress"},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "running"
        assert body["current_stage"] == "ingress"
        assert body["last_heartbeat_at"] is not None

    def test_heartbeat_unknown_worker_is_404(self, client):
        response = client.post(
            f"/workers/{uuid.uuid4()}/heartbeat", json={"status": "running"}
        )

        assert response.status_code == 404

    def test_permission_allowed_for_active_worker(self, client, make_worker):
        worker_id = make_worker()

        response = client.get(f"/workers/{worker_id}/permission")

        assert response.status_code == 200
        assert response.json() == {
            "allowed": True,
            "reason": None,
            "stop_requested": False,
        }

    def test_stop_then_permission_denied(self, client, make_worker):
        worker_id = make_worker()

        stop_response = client.post(f"/workers/{worker_id}/stop")
        assert stop_response.status_code == 200
        assert stop_response.json()["status"] == "stop_requested"

        permission = client.get(f"/workers/{worker_id}/permission").json()
        assert permission["allowed"] is False
        assert permission["stop_requested"] is True

    def test_stop_unknown_worker_is_404(self, client):
        assert client.post(f"/workers/{uuid.uuid4()}/stop").status_code == 404

    def test_complete_marks_worker(self, client, make_worker):
        worker_id = make_worker()

        response = client.post(
            f"/workers/{worker_id}/complete",
            json={"success": False, "message": "boom"},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "failed"
        assert body["failure_reason"] == "boom"

    def test_complete_unknown_worker_is_404(self, client):
        response = client.post(
            f"/workers/{uuid.uuid4()}/complete", json={"success": True}
        )

        assert response.status_code == 404


class TestMetricsRoutes:
    def test_report_then_read_metrics(self, client, make_worker):
        worker_id = make_worker()

        report = client.post(f"/workers/{worker_id}/metrics", json=METRICS_PAYLOAD)
        assert report.status_code == 204

        read = client.get(f"/workers/{worker_id}/metrics")
        assert read.status_code == 200
        body = read.json()
        assert body["idle_pct"] == 90.0
        assert body["reported_at"] is not None

    def test_report_for_terminal_worker_is_404(self, client, make_worker):
        worker_id = make_worker(status=WorkerStatus.COMPLETED)

        response = client.post(f"/workers/{worker_id}/metrics", json=METRICS_PAYLOAD)

        assert response.status_code == 404

    def test_read_without_report_is_404(self, client, make_worker):
        worker_id = make_worker()

        assert client.get(f"/workers/{worker_id}/metrics").status_code == 404

    def test_invalid_payload_is_422(self, client, make_worker):
        worker_id = make_worker()

        response = client.post(f"/workers/{worker_id}/metrics", json={"idle_pct": 1.0})

        assert response.status_code == 422


class TestLogsRoutes:
    def test_get_logs_for_launched_worker(self, client, seeded_event):
        worker = launch_worker(client, seeded_event)

        response = client.get(f"/workers/{worker['id']}/logs")

        assert response.status_code == 200
        assert response.json()["lines"] == ["line-1", "line-2"]

    def test_logs_without_container_is_404(self, client, make_worker):
        worker_id = make_worker()

        assert client.get(f"/workers/{worker_id}/logs").status_code == 404

    def test_stream_logs_emits_sse_lines(self, client, seeded_event):
        worker = launch_worker(client, seeded_event)

        with client.stream("GET", f"/workers/{worker['id']}/logs/stream") as response:
            assert response.status_code == 200
            body = "".join(response.iter_text())

        assert "data: line-1" in body
        assert "data: line-2" in body


class TestHealthRoute:
    def test_healthy(self, client, engine, monkeypatch):
        class FakeDockerDaemon:
            def ping(self):
                return True

            def close(self):
                pass

        monkeypatch.setattr("server.main.get_engine", lambda: engine)
        monkeypatch.setattr(
            "server.main.docker.from_env", lambda: FakeDockerDaemon()
        )

        response = client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_database_down_is_503(self, client, monkeypatch):
        def broken_engine():
            raise RuntimeError("db down")

        monkeypatch.setattr("server.main.get_engine", broken_engine)

        response = client.get("/health")

        assert response.status_code == 503
        assert "PostgreSQL unavailable" in response.json()["detail"]

    def test_docker_down_is_503(self, client, engine, monkeypatch):
        monkeypatch.setattr("server.main.get_engine", lambda: engine)

        def broken_docker():
            raise DockerException("daemon down")

        monkeypatch.setattr("server.main.docker.from_env", broken_docker)

        response = client.get("/health")

        assert response.status_code == 503
        assert "Docker unavailable" in response.json()["detail"]
