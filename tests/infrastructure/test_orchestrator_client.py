import uuid

import pytest
import requests

from orchestration.client import OrchestratorClient


class FakeResponse:
    def __init__(self, json_data=None, status_code=200):
        self._json = json_data or {}
        self.status_code = status_code

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"status {self.status_code}")


@pytest.fixture
def worker_id():
    return uuid.uuid4()


@pytest.fixture
def client(worker_id):
    return OrchestratorClient(base_url="http://broker:9000", worker_run_id=worker_id)


class TestClientConstruction:
    def test_explicit_base_url_and_worker_id(self, client, worker_id):
        assert client._base_url == "http://broker:9000"
        assert client._worker_run_id == str(worker_id)

    def test_worker_id_from_environment(self, monkeypatch):
        monkeypatch.setenv("SUNBEAM_WORKER_RUN_ID", "env-worker-id")
        client = OrchestratorClient(base_url="http://broker:9000")
        assert client._worker_run_id == "env-worker-id"

    def test_missing_worker_id_raises(self, monkeypatch):
        monkeypatch.delenv("SUNBEAM_WORKER_RUN_ID", raising=False)
        with pytest.raises(ValueError, match="SUNBEAM_WORKER_RUN_ID"):
            OrchestratorClient(base_url="http://broker:9000")


class TestClientRequests:
    def test_heartbeat_posts_payload(self, client, worker_id, monkeypatch):
        calls = []

        def fake_post(url, json=None, timeout=None):
            calls.append((url, json))
            return FakeResponse()

        monkeypatch.setattr("orchestration.client.requests.post", fake_post)

        client.heartbeat(status="running", current_stage="ingress", status_message="ok")

        url, payload = calls[0]
        assert url == f"http://broker:9000/workers/{worker_id}/heartbeat"
        assert payload["status"] == "running"
        assert payload["current_stage"] == "ingress"
        assert payload["status_message"] == "ok"
        assert "host" in payload

    def test_permission_parses_response(self, client, worker_id, monkeypatch):
        def fake_get(url, timeout=None):
            assert url == f"http://broker:9000/workers/{worker_id}/permission"
            return FakeResponse({"allowed": False, "reason": "stop", "stop_requested": True})

        monkeypatch.setattr("orchestration.client.requests.get", fake_get)

        permission = client.permission()

        assert permission.allowed is False
        assert permission.reason == "stop"

    def test_complete_posts_success_and_message(self, client, worker_id, monkeypatch):
        calls = []

        def fake_post(url, json=None, timeout=None):
            calls.append((url, json))
            return FakeResponse()

        monkeypatch.setattr("orchestration.client.requests.post", fake_post)

        client.complete(success=True, message="all done")

        url, payload = calls[0]
        assert url == f"http://broker:9000/workers/{worker_id}/complete"
        assert payload == {"success": True, "message": "all done"}

    def test_report_metrics_posts_payload(self, client, worker_id, monkeypatch):
        calls = []

        def fake_post(url, json=None, timeout=None):
            calls.append((url, json))
            return FakeResponse()

        monkeypatch.setattr("orchestration.client.requests.post", fake_post)

        client.report_metrics({"idle_pct": 12.5})

        url, payload = calls[0]
        assert url == f"http://broker:9000/workers/{worker_id}/metrics"
        assert payload == {"idle_pct": 12.5}

    def test_register_posts_and_returns_ready_client(self, monkeypatch):
        issued_id = str(uuid.uuid4())
        calls = []

        def fake_post(url, json=None, timeout=None):
            calls.append((url, json))
            return FakeResponse({"id": issued_id, "status": "starting"})

        monkeypatch.setattr("orchestration.client.requests.post", fake_post)

        client = OrchestratorClient.register(
            event_name="realtime",
            pipeline_edition="v3_0",
            base_url="http://broker:9000",
        )

        url, payload = calls[0]
        assert url == "http://broker:9000/workers/register"
        assert payload["event_name"] == "realtime"
        assert payload["pipeline_edition"] == "v3_0"
        assert "host" in payload

        assert client._worker_run_id == issued_id
        assert client._base_url == "http://broker:9000"

    def test_register_http_error_propagates(self, monkeypatch):
        monkeypatch.setattr(
            "orchestration.client.requests.post",
            lambda url, json=None, timeout=None: FakeResponse(status_code=400),
        )

        with pytest.raises(requests.exceptions.HTTPError):
            OrchestratorClient.register(
                event_name="nope",
                pipeline_edition="v3_0",
                base_url="http://broker:9000",
            )

    def test_http_error_propagates(self, client, monkeypatch):
        monkeypatch.setattr(
            "orchestration.client.requests.post",
            lambda url, json=None, timeout=None: FakeResponse(status_code=500),
        )

        with pytest.raises(requests.exceptions.HTTPError):
            client.complete(success=True)
