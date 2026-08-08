from datetime import datetime, timezone

import pytest
from data_tools.localization import CanonicalName

from db.telemetrydb.realtime_ingress import DebugTimeProvider
from stage.v3_0.ingress import Ingress, MissingDataTracker
from state.frame import FrameView


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now


class TestMissingDataTracker:
    def test_signal_with_data_is_never_overdue(self):
        clock = FakeClock()
        tracker = MissingDataTracker(grace_s=1.0, monotonic=clock.monotonic)

        tracker.observe("a", has_data=True)
        clock.now = 100.0

        assert tracker.overdue() == []

    def test_missing_signal_within_grace_is_not_overdue(self):
        clock = FakeClock()
        tracker = MissingDataTracker(grace_s=1.0, monotonic=clock.monotonic)

        tracker.observe("a", has_data=False)
        clock.now = 0.9

        assert tracker.overdue() == []

    def test_missing_signal_beyond_grace_is_overdue(self):
        clock = FakeClock()
        tracker = MissingDataTracker(grace_s=1.0, monotonic=clock.monotonic)

        tracker.observe("a", has_data=False)
        tracker.observe("b", has_data=False)
        clock.now = 1.1

        assert tracker.overdue() == ["a", "b"]

    def test_recovery_clears_the_timer(self):
        clock = FakeClock()
        tracker = MissingDataTracker(grace_s=1.0, monotonic=clock.monotonic)

        tracker.observe("a", has_data=False)
        clock.now = 0.5
        tracker.observe("a", has_data=True)

        # Goes missing again: the clock starts over from now, not from t=0.
        tracker.observe("a", has_data=False)
        clock.now = 1.2
        assert tracker.overdue() == []

        clock.now = 1.6
        assert tracker.overdue() == ["a"]


class FakeRealtimeIngress:
    """Stands in for RealtimeIngress; returns whatever the test scripts."""

    def __init__(self, responses: list[dict]):
        self._responses = responses
        self.calls = 0

    def get_last_values(self) -> dict:
        response = self._responses[min(self.calls, len(self._responses) - 1)]
        self.calls += 1
        return response


SPEED = CanonicalName.VehicleSpeed
CURRENT = CanonicalName.PackCurrent

# Field names these signals localize to for the 2024-07-16 tables.
SPEED_FIELD = "VehicleVelocity"
CURRENT_FIELD = "PackCurrent"

TIME_PROVIDER_START = datetime(2024, 7, 16, 14, 0, tzinfo=timezone.utc)


def make_ingress(responses: list[dict], data_grace_s: float = 1.0) -> Ingress:
    return Ingress(
        output_signals=[SPEED, CURRENT],
        frequency=10.0,
        time_provider=DebugTimeProvider(start_time=TIME_PROVIDER_START),
        ingress_client=FakeRealtimeIngress(responses),
        data_grace_s=data_grace_s,
    )


def empty_frame() -> FrameView:
    return FrameView(datetime(2026, 7, 19, 12, 0, 0))


class TestIngressStage:
    def test_values_written_to_output_frame(self):
        ingress = make_ingress(
            [
                {
                    SPEED_FIELD: {"time": None, "value": 12.5},
                    CURRENT_FIELD: {"time": None, "value": -3.0},
                }
            ]
        )

        frame = ingress.run(empty_frame())

        assert frame.read(SPEED) == 12.5
        assert frame.read(CURRENT) == -3.0

    def test_briefly_missing_signal_is_tolerated(self):
        ingress = make_ingress(
            [
                {
                    SPEED_FIELD: {"time": None, "value": 12.5},
                    CURRENT_FIELD: None,
                }
            ],
            data_grace_s=60.0,
        )

        frame = ingress.run(empty_frame())

        assert frame.read(SPEED) == 12.5
        with pytest.raises(KeyError):
            frame.read(CURRENT)

    def test_persistently_missing_signal_raises_after_grace(self):
        import time

        ingress = make_ingress(
            [
                {
                    SPEED_FIELD: {"time": None, "value": 12.5},
                    CURRENT_FIELD: None,
                }
            ],
            data_grace_s=0.05,
        )

        ingress.run(empty_frame())
        time.sleep(0.06)

        with pytest.raises(RuntimeError, match="no data for signal"):
            ingress.run(empty_frame())

    def test_recovered_signal_does_not_raise(self):
        import time

        ingress = make_ingress(
            [
                {SPEED_FIELD: {"time": None, "value": 1.0}, CURRENT_FIELD: None},
                {
                    SPEED_FIELD: {"time": None, "value": 2.0},
                    CURRENT_FIELD: {"time": None, "value": 5.0},
                },
            ],
            data_grace_s=0.05,
        )

        ingress.run(empty_frame())
        time.sleep(0.06)

        frame = ingress.run(empty_frame())

        assert frame.read(CURRENT) == 5.0
