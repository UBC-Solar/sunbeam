import uuid

import pytest
import requests

from datetime import datetime, timezone

from config.events import EventConfig
from orchestration.bootstrap import build_control
from orchestration.client import OrchestratorClient
from orchestration.control import OrchestratedWorkerControl, ServerlessWorkerControl


EVENT = EventConfig(
    name="realtime", vehicle="Brightside", description=None, stages=[], pipeline_edition="v3_0",
    starts_at=datetime(2026, 1, 1, tzinfo=timezone.utc), ends_at=None,
)


class TestBuildControl:
    def test_serverless_flag_wins(self):
        control = build_control(True, EVENT)

        assert isinstance(control, ServerlessWorkerControl)

    def test_server_launched_worker_uses_env_run_id(self, monkeypatch, test_context):
        monkeypatch.setenv("SUNBEAM_WORKER_RUN_ID", str(uuid.uuid4()))

        control = build_control(False, EVENT)

        assert isinstance(control, OrchestratedWorkerControl)

    def test_hand_run_worker_registers_itself(self, monkeypatch, test_context):
        monkeypatch.delenv("SUNBEAM_WORKER_RUN_ID", raising=False)
        registrations = []

        def fake_register(
                cls=None, *, event_name, pipeline_edition, base_url=None, influx_connection_name=None, reprocess=False,
        ):
            registrations.append((event_name, pipeline_edition, influx_connection_name, reprocess))
            return OrchestratorClient(
                base_url="http://server:9000", worker_run_id=uuid.uuid4()
            )

        monkeypatch.setattr(OrchestratorClient, "register", fake_register)

        control = build_control(False, EVENT)

        assert isinstance(control, OrchestratedWorkerControl)
        # The pipeline edition comes from the event's config; the connection is the one this process was given
        assert registrations == [("realtime", "v3_0", "external: http://localhost:8086 / test-bucket", False)]

    def test_unreachable_server_exits_with_guidance(self, monkeypatch, test_context):
        monkeypatch.delenv("SUNBEAM_WORKER_RUN_ID", raising=False)

        def fake_register(*args, **kwargs):
            raise requests.exceptions.ConnectionError("refused")

        monkeypatch.setattr(OrchestratorClient, "register", fake_register)

        with pytest.raises(SystemExit, match="--serverless"):
            build_control(False, EVENT)