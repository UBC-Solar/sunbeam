import requests

from orchestration.client import WorkerPermission
from orchestration.control import OrchestratedWorkerControl, ServerlessWorkerControl
from tests.infrastructure.conftest import wait_until


class FakeOrchestratorClient:
    def __init__(self):
        self.heartbeats = []
        self.metrics = []
        self.completions = []
        self.allowed = True
        self.reason = None
        self.permission_error = None
        self.complete_error = None

    def heartbeat(self, *, status="running", current_stage=None, status_message=None):
        self.heartbeats.append((status, current_stage, status_message))

    def permission(self) -> WorkerPermission:
        if self.permission_error is not None:
            raise self.permission_error
        return WorkerPermission(allowed=self.allowed, reason=self.reason)

    def report_metrics(self, payload):
        if self.complete_error is not None:
            raise self.complete_error
        self.metrics.append(payload)

    def complete(self, *, success, message=None):
        if self.complete_error is not None:
            raise self.complete_error
        self.completions.append((success, message))


def make_control(client) -> OrchestratedWorkerControl:
    return OrchestratedWorkerControl(
        client,
        heartbeat_interval_s=0.01,
        permission_interval_s=0.01,
        poll_interval_s=0.005,
    )


class TestServerlessControl:
    def test_never_asks_to_stop(self):
        control = ServerlessWorkerControl()
        control.start()
        assert control.should_stop() is False
        control.complete(success=True)
        control.stop()

    def test_request_stop_flips_should_stop(self):
        control = ServerlessWorkerControl()
        control.request_stop("local failure")
        assert control.should_stop() is True


class TestOrchestratedControlLoop:
    def test_heartbeats_carry_stage_and_message(self):
        client = FakeOrchestratorClient()
        control = make_control(client)

        control.set_stage("ingress")
        control.set_message("warming up")
        control.start()
        try:
            assert wait_until(lambda: len(client.heartbeats) >= 2)
        finally:
            control.stop()

        status, stage, message = client.heartbeats[0]
        assert status == "running"
        assert stage == "ingress"
        assert message == "warming up"

    def test_permission_denied_requests_stop(self):
        client = FakeOrchestratorClient()
        client.allowed = False
        client.reason = "Stop requested."
        control = make_control(client)

        control.start()
        try:
            assert wait_until(control.should_stop)
        finally:
            control.stop()

        assert control._snapshot()[1] == "Stop requested."

    def test_permission_denied_without_reason_gets_default_message(self):
        client = FakeOrchestratorClient()
        client.allowed = False
        control = make_control(client)

        control.start()
        try:
            assert wait_until(control.should_stop)
        finally:
            control.stop()

        assert control._snapshot()[1] == "Stop requested by server."

    def test_unreachable_server_is_treated_as_stop(self):
        client = FakeOrchestratorClient()
        client.permission_error = requests.exceptions.ConnectionError("down")
        control = make_control(client)

        control.start()
        try:
            assert wait_until(control.should_stop)
        finally:
            control.stop()

        assert control._snapshot()[1] == "Failed to connect to server."

    def test_request_stop_works_without_server(self):
        client = FakeOrchestratorClient()
        control = make_control(client)

        control.request_stop("compute scheduler crashed")

        assert control.should_stop() is True
        assert control._snapshot()[1] == "compute scheduler crashed"

    def test_stop_joins_thread(self):
        client = FakeOrchestratorClient()
        control = make_control(client)

        control.start()
        control.stop()

        assert not control._thread.is_alive()


class TestOrchestratedControlReporting:
    def test_heartbeat_now_sends_snapshot(self):
        client = FakeOrchestratorClient()
        control = make_control(client)

        control.set_stage("compute")
        control.heartbeat_now(status="stopping")

        assert client.heartbeats == [("stopping", "compute", None)]

    def test_complete_swallows_request_errors(self):
        client = FakeOrchestratorClient()
        client.complete_error = requests.exceptions.ConnectionError("down")
        control = make_control(client)

        # Must not raise: completion reporting is best-effort.
        control.complete(success=True, message="done")
        control.report_metrics({"idle_pct": 1.0})

    def test_complete_and_metrics_forwarded(self):
        client = FakeOrchestratorClient()
        control = make_control(client)

        control.complete(success=False, message="oops")
        control.report_metrics({"idle_pct": 50.0})

        assert client.completions == [(False, "oops")]
        assert client.metrics == [{"idle_pct": 50.0}]
