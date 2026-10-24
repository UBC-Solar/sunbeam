import time
from datetime import datetime, timedelta, timezone

import pytest

from pipeline.executor import Executor, replay_window_for
from tests.infrastructure.conftest import (
    FakeControl,
    FakeOutputManager,
    FakePipeline,
    RecordingWriter,
)


def make_executor(clock, *, compute=None, ingress=None, writer=None, control=None):
    compute = compute if compute is not None else [FakePipeline("compute", 50.0)]
    ingress = ingress if ingress is not None else [FakePipeline("ingress", 50.0)]
    writer = writer or RecordingWriter()
    control = control or FakeControl()

    executor = Executor(
        compute,
        ingress,
        writer,
        control,
        output_manager=FakeOutputManager(),
        monotonic_ns=clock.monotonic_ns,
        sleep=clock.sleep,
    )
    return executor, writer, control


class TestExecutorServerStop:
    def test_stop_requested_by_server(self, fake_clock):
        control = FakeControl()
        writer = RecordingWriter(
            on_write=lambda n: control.request_stop() if n >= 10 else None
        )
        executor, writer, control = make_executor(
            fake_clock, writer=writer, control=control
        )

        executor.run()

        assert control.completions == [(False, "Worker stopped by server.")]
        assert control.started
        assert control.stopped
        assert writer.closed
        assert len(writer.frames) >= 10

    def test_frames_and_stages_flow_through_output_handler(self, fake_clock):
        control = FakeControl()
        writer = RecordingWriter(
            on_write=lambda n: control.request_stop() if n >= 5 else None
        )
        compute = [FakePipeline("compute", 50.0)]
        executor, writer, control = make_executor(
            fake_clock, compute=compute, writer=writer, control=control
        )

        executor.run()

        # Every written frame was preceded by set_stage(pipeline.name).
        assert len(control.stages) == len(writer.frames)
        assert set(control.stages) <= {"compute", "ingress"}


class TestExecutorIngressCrash:
    def test_ingress_crash_stops_compute_and_reports_failure(self, fake_clock):
        ingress = [FakePipeline("ingress", 50.0, fail_after=1)]
        executor, writer, control = make_executor(fake_clock, ingress=ingress)

        executor.run()

        assert len(control.completions) == 1
        success, message = control.completions[0]
        assert success is False
        assert message.startswith("Ingress pipeline crashed:")
        assert writer.closed


class TestExecutorComputeCrash:
    def test_compute_exception_reports_failure_and_reraises(self, fake_clock):
        compute = [FakePipeline("compute", 50.0, fail_after=2)]
        executor, writer, control = make_executor(fake_clock, compute=compute)

        with pytest.raises(RuntimeError, match="compute failed"):
            executor.run()

        assert len(control.completions) == 1
        success, message = control.completions[0]
        assert success is False
        assert message.startswith("Worker failed:")
        assert control.stopped
        assert writer.closed

    def test_crash_requests_stop_so_ingress_exits_promptly(self, fake_clock):
        compute = [FakePipeline("compute", 50.0, fail_after=2)]
        executor, writer, control = make_executor(fake_clock, compute=compute)

        started = time.monotonic()
        with pytest.raises(RuntimeError):
            executor.run()
        elapsed = time.monotonic() - started

        # Without request_stop, run() would wait out the ingress thread's
        # full 5 s join timeout after a compute crash.
        assert control.should_stop() is True
        assert elapsed < 4.0


class TestExecutorCompletion:
    def test_signal_completion_reports_success(self, fake_clock):
        holder = {}
        writer = RecordingWriter(
            on_write=lambda n: holder["executor"].signal_completion() if n >= 5 else None
        )
        executor, writer, control = make_executor(fake_clock, writer=writer)
        holder["executor"] = executor

        executor.run()

        assert control.completions == [(True, "Pipeline completed.")]
        assert writer.closed
        assert control.stopped


class TestExecutorIngressThreads:
    def test_each_ingress_pipeline_gets_scheduled(self, fake_clock):
        control = FakeControl()
        ingress = [FakePipeline("ingress-a", 50.0), FakePipeline("ingress-b", 50.0)]
        writer = RecordingWriter(
            on_write=lambda n: control.request_stop()
            if all(p.run_count > 0 for p in ingress)
            else None
        )
        executor, writer, control = make_executor(
            fake_clock, ingress=ingress, writer=writer, control=control
        )

        executor.run()

        assert ingress[0].run_count > 0
        assert ingress[1].run_count > 0

    def test_crash_in_one_ingress_thread_fails_the_worker(self, fake_clock):
        ingress = [
            FakePipeline("ingress-ok", 50.0),
            FakePipeline("ingress-bad", 50.0, fail_after=1),
        ]
        executor, writer, control = make_executor(fake_clock, ingress=ingress)

        executor.run()

        assert len(control.completions) == 1
        success, message = control.completions[0]
        assert success is False
        assert message.startswith("Ingress pipeline crashed:")


class TestExecutorDefaults:
    def test_default_control_is_serverless(self, fake_clock):
        from orchestration.control import ServerlessWorkerControl

        executor = Executor(
            [FakePipeline("compute", 50.0)],
            [FakePipeline("ingress", 50.0)],
            RecordingWriter(),
            monotonic_ns=fake_clock.monotonic_ns,
            sleep=fake_clock.sleep,
        )

        assert isinstance(executor._control, ServerlessWorkerControl)


class TimeSeriesIngress:
    """ Replay ingress stand-in: writes one TimeSeries covering [start, start + length_s] into the state. """

    def __init__(self, signal, length_s: float, period_s: float = 0.1):
        self.name = "offline-ingress"
        self.frequency = 1.0
        self._signal = signal
        self._length_s = length_s
        self._period_s = period_s

    def run(self, state, timestamp):
        import numpy as np
        from data_tools.collections import TimeSeries

        from state.frame import Frame

        n = round(self._length_s / self._period_s) + 1
        series = TimeSeries(
            np.ones(n), start_time=timestamp, stop_time=timestamp + timedelta(seconds=self._length_s),
            period=self._period_s, length=self._length_s, units="",
        )
        frame = Frame(timestamp)
        frame.write(self._signal, series)
        state.from_frame(frame, [self._signal])
        yield frame


REPLAY_START = datetime(2024, 7, 16, 15, 0, tzinfo=timezone.utc)


def make_replay_executor(clock, *, ingress_length_s=10.0, writer=None, control=None, compute=None):
    compute = compute if compute is not None else [FakePipeline("compute", 5.0)]
    writer = writer or RecordingWriter()
    control = control or FakeControl()

    executor = Executor(
        compute,
        [TimeSeriesIngress("speed", length_s=ingress_length_s)],
        writer,
        control,
        output_manager=FakeOutputManager(),
        monotonic_ns=clock.monotonic_ns,
        sleep=clock.sleep,
        replay_window=(REPLAY_START, REPLAY_START + timedelta(seconds=10)),
    )
    return executor, writer, control, compute


class TestExecutorReplay:
    def test_replay_runs_the_window_and_reports_success(self, fake_clock):
        executor, writer, control, compute = make_replay_executor(fake_clock)

        executor.run()

        # 5 Hz over the 10 s window, starting at the first whole second after its start
        assert compute[0].run_count == 5 * 9 + 1
        assert compute[0].run_timestamps[0] == REPLAY_START + timedelta(seconds=1)
        assert compute[0].run_timestamps[-1] == REPLAY_START + timedelta(seconds=10)
        assert control.completions == [(True, "Pipeline completed.")]
        assert writer.closed

    def test_replay_is_trimmed_to_the_telemetry(self, fake_clock):
        executor, writer, control, compute = make_replay_executor(fake_clock, ingress_length_s=5.0)

        executor.run()

        assert compute[0].run_timestamps[-1] == REPLAY_START + timedelta(seconds=5)
        assert control.completions == [(True, "Pipeline completed.")]

    def test_server_stop_during_replay_is_not_reported_as_success(self, fake_clock):
        control = FakeControl()
        writer = RecordingWriter(on_write=lambda n: control.request_stop() if n >= 5 else None)
        executor, writer, control, compute = make_replay_executor(fake_clock, writer=writer, control=control)

        executor.run()

        assert control.completions == [(False, "Worker stopped by server.")]
        assert compute[0].run_count < 5 * 9 + 1

    def test_replay_without_telemetry_fails(self, fake_clock):
        control = FakeControl()
        executor = Executor(
            [FakePipeline("compute", 5.0)],
            [FakePipeline("ingress", 1.0)],  # Yields no TimeSeries
            RecordingWriter(),
            control,
            output_manager=FakeOutputManager(),
            monotonic_ns=fake_clock.monotonic_ns,
            sleep=fake_clock.sleep,
            replay_window=(REPLAY_START, REPLAY_START + timedelta(seconds=10)),
        )

        with pytest.raises(RuntimeError, match="No telemetry"):
            executor.run()

        success, message = control.completions[0]
        assert success is False
        assert message.startswith("Worker failed: No telemetry")


class TestReplayWindow:
    def _event(self, ends_at):
        from config.events import EventConfig

        return EventConfig(
            name="e", stages=[], pipeline_edition="v3_0",
            starts_at=REPLAY_START, ends_at=ends_at,
        )

    def test_past_event_is_replayed_over_its_window(self):
        end = REPLAY_START + timedelta(hours=1)

        assert replay_window_for(self._event(end)) == (REPLAY_START, end)

    def test_ongoing_event_runs_live(self):
        assert replay_window_for(self._event(None)) is None

    def test_future_end_runs_live(self):
        future = datetime.now(timezone.utc) + timedelta(days=1)

        assert replay_window_for(self._event(future)) is None
