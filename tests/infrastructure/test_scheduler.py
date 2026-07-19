from datetime import timedelta

import pytest

from pipeline.scheduler import Scheduler
from tests.infrastructure.conftest import FakePipeline


def make_scheduler(pipelines, clock, observer=None):
    return Scheduler(
        pipelines,
        observer=observer,
        monotonic_ns=clock.monotonic_ns,
        sleep=clock.sleep,
    )


class TestSchedulerConstruction:
    def test_rejects_zero_frequency(self, fake_clock):
        with pytest.raises(ValueError, match="frequency must be positive"):
            make_scheduler([FakePipeline("p", 0.0)], fake_clock)

    def test_rejects_negative_frequency(self, fake_clock):
        with pytest.raises(ValueError, match="frequency must be positive"):
            make_scheduler([FakePipeline("p", -1.0)], fake_clock)


class TestSchedulerRun:
    def test_stops_immediately_when_should_stop_is_true(self, fake_clock):
        pipeline = FakePipeline("p", 10.0)
        scheduler = make_scheduler([pipeline], fake_clock)

        scheduler.run_forever(state=None, should_stop=lambda: True)

        assert pipeline.run_count == 0

    def test_single_pipeline_runs_at_its_period(self, fake_clock):
        pipeline = FakePipeline("p", 10.0)
        scheduler = make_scheduler([pipeline], fake_clock)

        scheduler.run_forever(state=None, should_stop=lambda: pipeline.run_count >= 5)

        assert pipeline.run_count == 5

        deltas = [
            b - a
            for a, b in zip(pipeline.run_timestamps, pipeline.run_timestamps[1:])
        ]
        assert all(delta == timedelta(milliseconds=100) for delta in deltas)

    def test_faster_pipeline_runs_proportionally_more(self, fake_clock):
        fast = FakePipeline("fast", 10.0)
        slow = FakePipeline("slow", 5.0)
        scheduler = make_scheduler([fast, slow], fake_clock)

        scheduler.run_forever(state=None, should_stop=lambda: fast.run_count >= 21)

        # Over the same span of (fake) time, the 10 Hz pipeline must have run
        # twice as often as the 5 Hz one, up to boundary effects.
        assert fast.run_count == 21
        assert abs(fast.run_count - 2 * slow.run_count) <= 2

    def test_on_output_receives_every_frame(self, fake_clock):
        pipeline = FakePipeline("p", 10.0, frames_per_run=3)
        scheduler = make_scheduler([pipeline], fake_clock)
        outputs = []

        scheduler.run_forever(
            state=None,
            on_output=lambda p, frame, ts: outputs.append((p, frame, ts)),
            should_stop=lambda: pipeline.run_count >= 2,
        )

        assert len(outputs) == 2 * 3
        assert all(p is pipeline for p, _, _ in outputs)
        # Frames emitted in one run carry that run's scheduled timestamp.
        assert outputs[0][2] == pipeline.run_timestamps[0]

    def test_on_tick_called_once_per_run(self, fake_clock):
        pipeline = FakePipeline("p", 10.0)
        scheduler = make_scheduler([pipeline], fake_clock)
        ticks = []

        scheduler.run_forever(
            state=None,
            on_tick=lambda: ticks.append(1),
            should_stop=lambda: pipeline.run_count >= 4,
        )

        assert len(ticks) == 4

    def test_error_propagates_when_stop_on_error(self, fake_clock):
        pipeline = FakePipeline("p", 10.0, fail_after=2)
        scheduler = make_scheduler([pipeline], fake_clock)

        with pytest.raises(RuntimeError, match="p failed"):
            scheduler.run_forever(
                state=None,
                stop_on_error=True,
                should_stop=lambda: pipeline.run_count >= 100,
            )

        assert pipeline.run_count == 3

    def test_error_swallowed_when_not_stop_on_error(self, fake_clock):
        pipeline = FakePipeline("p", 10.0, fail_after=2)
        scheduler = make_scheduler([pipeline], fake_clock)

        scheduler.run_forever(
            state=None,
            stop_on_error=False,
            should_stop=lambda: pipeline.run_count >= 6,
        )

        assert pipeline.run_count == 6


class RecordingObserver:
    def __init__(self):
        self.idle_ns = 0
        self.starts = []
        self.dones = []
        self.writer_dones = []

    def on_idle(self, ns):
        self.idle_ns += ns

    def on_pipeline_start(self, pipeline_name, late_ns):
        self.starts.append((pipeline_name, late_ns))

    def on_pipeline_done(self, pipeline_name, elapsed_ns):
        self.dones.append((pipeline_name, elapsed_ns))

    def on_writer_done(self, elapsed_ns):
        self.writer_dones.append(elapsed_ns)


class TestSchedulerObserver:
    def test_observer_sees_starts_dones_and_idle(self, fake_clock):
        pipeline = FakePipeline("p", 10.0)
        observer = RecordingObserver()
        scheduler = make_scheduler([pipeline], fake_clock, observer=observer)

        scheduler.run_forever(
            state=None,
            on_output=lambda p, frame, ts: None,
            should_stop=lambda: pipeline.run_count >= 3,
        )

        assert [name for name, _ in observer.starts] == ["p", "p", "p"]
        assert [name for name, _ in observer.dones] == ["p", "p", "p"]
        # One frame per run flows through on_output, so the writer callback
        # fires once per run.
        assert len(observer.writer_dones) == 3
        # The scheduler always waits for the aligned start, so idle time accrues.
        assert observer.idle_ns > 0
