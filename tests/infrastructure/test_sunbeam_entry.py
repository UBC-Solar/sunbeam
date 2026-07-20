import uuid

import pytest
import requests

from orchestration.bootstrap import build_control
from orchestration.client import OrchestratorClient
from orchestration.control import OrchestratedWorkerControl, ServerlessWorkerControl


class TestBuildControl:
    def test_serverless_flag_wins(self):
        control = build_control(True, "realtime")

        assert isinstance(control, ServerlessWorkerControl)

    def test_server_launched_worker_uses_env_run_id(self, monkeypatch, test_context):
        monkeypatch.setenv("SUNBEAM_WORKER_RUN_ID", str(uuid.uuid4()))

        control = build_control(False, "realtime")

        assert isinstance(control, OrchestratedWorkerControl)

    def test_hand_run_worker_registers_itself(self, monkeypatch, test_context):
        monkeypatch.delenv("SUNBEAM_WORKER_RUN_ID", raising=False)
        registrations = []

        def fake_register(cls=None, *, event_name, pipeline_edition, base_url=None):
            registrations.append((event_name, pipeline_edition))
            return OrchestratorClient(
                base_url="http://broker:9000", worker_run_id=uuid.uuid4()
            )

        monkeypatch.setattr(OrchestratorClient, "register", fake_register)

        control = build_control(False, "realtime")

        assert isinstance(control, OrchestratedWorkerControl)
        # The pipeline edition comes from events.toml for the event.
        assert registrations == [("realtime", "v3_0")]

    def test_unreachable_server_exits_with_guidance(self, monkeypatch, test_context):
        monkeypatch.delenv("SUNBEAM_WORKER_RUN_ID", raising=False)

        def fake_register(*args, **kwargs):
            raise requests.exceptions.ConnectionError("refused")

        monkeypatch.setattr(OrchestratorClient, "register", fake_register)

        with pytest.raises(SystemExit, match="--serverless"):
            build_control(False, "realtime")