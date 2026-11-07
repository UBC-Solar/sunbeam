import uuid

import pytest
import requests

from datetime import datetime, timezone

from config.events import EventConfig
from orchestration.bootstrap import build_control
from orchestration.client import OrchestratorClient
from orchestration.control import OrchestratedWorkerControl, ServerlessWorkerControl


EVENT = EventConfig(
    name="realtime", description=None, stages=[], pipeline_edition="v3_0",
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

class TestMissingStage:
    def test_is_reported_to_the_server_before_anything_else(self, monkeypatch):
        import importlib.util
        import pathlib

        # sunbeam.py by path: `import sunbeam` finds the repository's package instead
        path = pathlib.Path(__file__).parents[2] / "sunbeam.py"
        spec = importlib.util.spec_from_file_location("sunbeam_entry", path)
        sunbeam = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(sunbeam)

        class RecordingControl(ServerlessWorkerControl):
            is_orchestrated = False
            completed: dict = {}

            def complete(self, *, success, message=None):
                self.completed.update(success=success, message=message)

        # Nothing is reachable: the stage check comes first, needing nothing
        monkeypatch.setattr(sunbeam, "run_preflight_checks", lambda *args, **kwargs: pytest.fail("ran preflight"))
        event = EventConfig(
            name="trial", description=None, stages=["TryingThisOut"], pipeline_edition="v3_0",
            starts_at=datetime(2026, 1, 1, tzinfo=timezone.utc), ends_at=None,
        )
        control = RecordingControl()

        with pytest.raises(SystemExit):
            sunbeam.run(event, control, engine=None, reprocess=False)

        assert control.completed["success"] is False
        assert "This worker's code can't run 'trial'" in control.completed["message"]
        assert "'TryingThisOut' isn't registered for v3_0" in control.completed["message"]
