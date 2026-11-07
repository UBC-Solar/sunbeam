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
        self.heartbeat_error = None

    def heartbeat(self, *, status="running", status_message=None):
        if self.heartbeat_error is not None:
            raise self.heartbeat_error
        self.heartbeats.append((status, status_message))

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


def make_control(client, **kwargs) -> OrchestratedWorkerControl:
    return OrchestratedWorkerControl(
        client,
        heartbeat_interval_s=0.01,
        permission_interval_s=0.01,
        poll_interval_s=0.005,
        **kwargs,
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
        control.request_stop()
        assert control.should_stop() is True


class TestOrchestratedControlLoop:
    def test_heartbeats_carry_the_message(self):
        client = FakeOrchestratorClient()
        control = make_control(client)

        control.set_message("warming up")
        control.start()
        try:
            assert wait_until(lambda: len(client.heartbeats) >= 2)
        finally:
            control.stop()

        assert client.heartbeats[0] == ("running", "warming up")

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

        assert control._status_message == "Stop requested."

    def test_permission_denied_without_reason_gets_default_message(self):
        client = FakeOrchestratorClient()
        client.allowed = False
        control = make_control(client)

        control.start()
        try:
            assert wait_until(control.should_stop)
        finally:
            control.stop()

        assert control._status_message == "Stop requested by server."

    def test_briefly_unreachable_server_is_tolerated(self):
        client = FakeOrchestratorClient()
        client.permission_error = requests.exceptions.ConnectionError("down")
        control = OrchestratedWorkerControl(
            client, heartbeat_interval_s=0.01, permission_interval_s=0.01, poll_interval_s=0.005,
            max_unreachable_polls=1000,
        )

        control.start()
        try:
            assert wait_until(lambda: len(client.heartbeats) >= 3)
        finally:
            control.stop()

        assert not control.should_stop()

    def test_server_unreachable_for_too_long_stops_the_worker(self):
        client = FakeOrchestratorClient()
        client.permission_error = requests.exceptions.ConnectionError("down")
        control = make_control(client, max_unreachable_polls=3)

        control.start()
        try:
            assert wait_until(control.should_stop)
        finally:
            control.stop()

        assert control._status_message == "Lost contact with the server."

    def test_an_unreachable_server_is_a_warning_not_a_traceback(self, caplog):
        # A restarting server refuses connections; a hung one times out
        for error in (requests.exceptions.ConnectionError("refused"), requests.exceptions.Timeout("slow")):
            client = FakeOrchestratorClient()
            client.permission_error = client.heartbeat_error = error
            control = make_control(client, max_unreachable_polls=1000)
            caplog.clear()

            control.start()
            try:
                assert wait_until(lambda: "3 in a row" in caplog.text)
            finally:
                control.stop()

            assert not control.should_stop()
            assert "Worker control loop error" not in caplog.text
            assert "Traceback" not in caplog.text

    def test_request_stop_works_without_server(self):
        control = make_control(FakeOrchestratorClient())

        control.request_stop()

        assert control.should_stop() is True

    def test_heartbeats_continue_as_stopping_after_a_stop(self):
        client = FakeOrchestratorClient()
        control = make_control(client)

        control.request_stop()
        control.start()
        try:
            assert wait_until(lambda: len(client.heartbeats) >= 2)
        finally:
            control.stop()

        assert {status for status, _ in client.heartbeats} == {"stopping"}

    def test_stop_joins_thread(self):
        client = FakeOrchestratorClient()
        control = make_control(client)

        control.start()
        control.stop()

        assert not control._thread.is_alive()


class TestOrchestratedControlReporting:
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
