import io
from datetime import datetime, timedelta, timezone

import pytest
from rich.console import Console

from pipeline.output import LoggingOutputManager, RichReplayOutputManager
from pipeline.replay_progress import ReplayProgress, format_duration, format_speed
from pipeline.timing import TimingStats
from tests.infrastructure.conftest import FakeControl

START = datetime(2024, 7, 16, 15, 0, tzinfo=timezone.utc)
END = START + timedelta(hours=1)


class FakeMonotonic:
    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock():
    return FakeMonotonic()


class TestReplayProgress:
    def test_starts_at_zero_with_unknown_speed(self, clock):
        progress = ReplayProgress(START, END, monotonic=clock)

        assert progress.fraction == 0.0
        assert progress.speed is None
        assert progress.eta_s is None

    def test_speed_and_eta_from_average_rate(self, clock):
        progress = ReplayProgress(START, END, monotonic=clock)

        clock.now = 10.0
        progress.update(START + timedelta(minutes=15))

        assert progress.fraction == 0.25
        assert progress.speed == 90.0  # 900 simulated seconds in 10 wall seconds
        assert progress.eta_s == pytest.approx(30.0)

    def test_timestamps_are_clamped_to_the_window(self, clock):
        progress = ReplayProgress(START, END, monotonic=clock)

        progress.update(END + timedelta(seconds=5))
        assert progress.fraction == 1.0

        progress.update(START - timedelta(seconds=5))
        assert progress.fraction == 0.0

    def test_snapshot_matches_the_server_schema(self, clock):
        schemas = pytest.importorskip("server.schemas")
        progress = ReplayProgress(START, END, monotonic=clock)
        clock.now = 2.0
        progress.update(START + timedelta(minutes=30))

        parsed = schemas.ReplayProgressStats(**progress.snapshot())

        assert parsed.fraction == 0.5
        assert set(progress.snapshot()) == set(schemas.ReplayProgressStats.model_fields)


class TestFormatting:
    @pytest.mark.parametrize(("seconds", "expected"), [
        (None, "--"), (45.4, "45s"), (192, "3m 12s"), (3720, "1h 02m"),
    ])
    def test_format_duration(self, seconds, expected):
        assert format_duration(seconds) == expected

    def test_format_speed(self):
        assert format_speed(None) == "×--"
        assert format_speed(1234.4) == "×1,234"


class TestLoggingOutputManagerReplay:
    def test_reports_replay_progress_with_the_timing(self, clock):
        control = FakeControl()
        reported = []
        control.report_metrics = reported.append
        progress = ReplayProgress(START, END, monotonic=clock)
        manager = LoggingOutputManager(TimingStats({}), control, report_interval_s=0.0, replay=progress)

        clock.now = 1.0
        with manager:
            manager.on_tick(START + timedelta(minutes=6))

        assert reported[-1]["replay"]["fraction"] == pytest.approx(0.1)
        assert "idle_pct" in reported[-1]

    def test_reports_more_often_than_it_logs(self, clock, caplog):
        control = FakeControl()
        reported = []
        control.report_metrics = reported.append
        progress = ReplayProgress(START, END, monotonic=clock)
        manager = LoggingOutputManager(
            TimingStats({}), control, report_interval_s=0.0, log_interval_s=5.0, replay=progress, monotonic=clock,
        )

        with caplog.at_level("INFO", logger="sunbeam.worker"):
            for second in range(10):
                clock.now = float(second)
                manager.on_tick(START + timedelta(minutes=second))

        assert len(reported) == 10
        # One replay line and one timing line at t=0 and t=5
        assert len([r for r in caplog.records if r.message.startswith("replay")]) == 2

    def test_reporting_only_never_logs(self, clock, caplog):
        control = FakeControl()
        reported = []
        control.report_metrics = reported.append
        manager = LoggingOutputManager(
            TimingStats({}), control, report_interval_s=0.0, log_interval_s=None,
            replay=ReplayProgress(START, END, monotonic=clock), monotonic=clock,
        )

        with caplog.at_level("INFO", logger="sunbeam.worker"):
            manager.on_tick(START + timedelta(minutes=1))

        assert len(reported) == 1
        assert caplog.records == []

    def test_realtime_reports_have_no_replay_field(self):
        control = FakeControl()
        reported = []
        control.report_metrics = reported.append
        manager = LoggingOutputManager(TimingStats({}), control, report_interval_s=0.0)

        manager.on_tick(START)

        assert "replay" not in reported[-1]


class TestRichReplayOutputManager:
    def test_bar_follows_ticks_and_finishes_at_the_end(self, clock):
        output = io.StringIO()
        progress = ReplayProgress(START, END, monotonic=clock)
        manager = RichReplayOutputManager(
            progress,
            writer_stats=lambda: {"queue_depth": 3, "queue_capacity": 10_000},
            console=Console(file=output, width=160),
            monotonic=clock,
        )

        with manager:
            for minute in range(0, 61, 10):
                clock.now += 1.0
                manager.on_tick(START + timedelta(minutes=minute))

        task = manager._progress.tasks[0]
        assert task.completed == task.total == 3600.0
        assert task.finished

        rendered = output.getvalue()
        assert "100%" in rendered
        assert "16:00:00 / 16:00:00 UTC" in rendered
        assert "queue 3/10,000" in rendered

    def test_redraws_are_throttled(self, clock):
        progress = ReplayProgress(START, END, monotonic=clock)
        manager = RichReplayOutputManager(progress, console=Console(file=io.StringIO()), monotonic=clock)

        with manager:
            manager.on_tick(START + timedelta(minutes=1))  # First tick always redraws
            manager.on_tick(START + timedelta(minutes=2))  # Same instant: skipped
            assert manager._progress.tasks[0].completed == 60.0

            clock.now = 1.0
            manager.on_tick(START + timedelta(minutes=3))
            assert manager._progress.tasks[0].completed == 180.0


class TestCombinedOutputManager:
    def test_enters_ticks_and_exits_every_manager(self):
        from pipeline.output import CombinedOutputManager
        from tests.infrastructure.conftest import FakeOutputManager

        first, second = FakeOutputManager(), FakeOutputManager()

        with CombinedOutputManager(first, second) as combined:
            assert first.entered and second.entered
            combined.on_tick(START)

        assert first.ticks == second.ticks == 1
        assert first.exited and second.exited


class TestExecutorOutputSelection:
    """ Which display (and whether server reporting) each kind of run gets. """

    def _executor(self, *, orchestrated: bool, interactive: bool):
        from orchestration.control import OrchestratedWorkerControl
        from pipeline.executor import Executor
        from tests.infrastructure.conftest import RecordingWriter

        control = OrchestratedWorkerControl(client=None) if orchestrated else None
        return Executor([], [], RecordingWriter(), control, interactive=interactive)

    def _replay(self):
        return ReplayProgress(START, END)

    def test_replay_in_a_terminal_draws_the_bar_and_reports(self):
        from pipeline.output import CombinedOutputManager

        manager = self._executor(orchestrated=True, interactive=True)._resolve_output_manager(self._replay())

        assert isinstance(manager, CombinedOutputManager)
        display, reporter = manager._managers
        assert isinstance(display, RichReplayOutputManager)
        assert isinstance(reporter, LoggingOutputManager)
        assert reporter._log_interval_s is None
        assert reporter._report_interval_s == 0.5

    def test_headless_replay_logs_and_reports(self):
        manager = self._executor(orchestrated=True, interactive=False)._resolve_output_manager(self._replay())

        assert isinstance(manager, LoggingOutputManager)
        assert manager._report_interval_s == 0.5
        assert manager._log_interval_s == 5.0

    def test_orchestrated_realtime_logs_and_reports_every_5_s(self):
        manager = self._executor(orchestrated=True, interactive=True)._resolve_output_manager()

        assert isinstance(manager, LoggingOutputManager)
        assert manager._report_interval_s == 5.0

    def test_serverless_realtime_draws_the_timing_table(self):
        from pipeline.output import RichOutputManager

        manager = self._executor(orchestrated=False, interactive=True)._resolve_output_manager()

        assert isinstance(manager, RichOutputManager)
