from data_tools.localization import CanonicalName
from state.frame import FrameView
from stage.v3_0.integrated_pack_power import IntegratedPackPower
from datetime import timedelta
import pytest


class TestIntegratedPackPowerStage:

    def test_initial_state(self, integrated_pack_power_stage):
        assert integrated_pack_power_stage.total_energy == 0.0
        assert integrated_pack_power_stage.last_timestamp is None

    def test_first_call_uses_estimated_dt(self, integrated_pack_power_stage, make_frame_view):
        frame = make_frame_view({CanonicalName.PackPower: 3600.0})
        result = integrated_pack_power_stage.run(frame)
        assert result.read(CanonicalName.IntegratedPackPower) == pytest.approx(0.2)

    def test_second_call_uses_actual_dt(self, integrated_pack_power_stage, sample_timestamp):
        t0 = sample_timestamp
        t1 = t0 + timedelta(seconds=1.0)

        frame0 = FrameView(t0, {CanonicalName.PackPower: 3600.0})
        integrated_pack_power_stage.run(frame0)

        frame1 = FrameView(t1, {CanonicalName.PackPower: 3600.0})
        result = integrated_pack_power_stage.run(frame1)
        assert result.read(CanonicalName.IntegratedPackPower) == pytest.approx(1.2)

    def test_accumulation_over_multiple_calls(self, integrated_pack_power_stage, sample_timestamp):
        base = sample_timestamp

        frame0 = FrameView(base, {CanonicalName.PackPower: 1000.0})
        integrated_pack_power_stage.run(frame0)

        frame1 = FrameView(base + timedelta(seconds=1.0), {CanonicalName.PackPower: 1000.0})
        integrated_pack_power_stage.run(frame1)

        frame2 = FrameView(base + timedelta(seconds=2.0), {CanonicalName.PackPower: 1000.0})
        result = integrated_pack_power_stage.run(frame2)

        expected = 1000 * 0.2 / 3600 + 2 * (1000 * 1.0 / 3600)
        assert result.read(CanonicalName.IntegratedPackPower) == pytest.approx(expected)

    def test_zero_power_no_change(self, integrated_pack_power_stage, sample_timestamp):
        base = sample_timestamp

        frame0 = FrameView(base, {CanonicalName.PackPower: 0.0})
        result0 = integrated_pack_power_stage.run(frame0)
        assert result0.read(CanonicalName.IntegratedPackPower) == pytest.approx(0.0)

        frame1 = FrameView(base + timedelta(seconds=1.0), {CanonicalName.PackPower: 0.0})
        result1 = integrated_pack_power_stage.run(frame1)
        assert result1.read(CanonicalName.IntegratedPackPower) == pytest.approx(0.0)

    def test_negative_power_reduces_total(self, integrated_pack_power_stage, sample_timestamp):
        base = sample_timestamp

        frame0 = FrameView(base, {CanonicalName.PackPower: 3600.0})
        integrated_pack_power_stage.run(frame0)

        frame1 = FrameView(base + timedelta(seconds=1.0), {CanonicalName.PackPower: -1800.0})
        result = integrated_pack_power_stage.run(frame1)
        assert result.read(CanonicalName.IntegratedPackPower) == pytest.approx(-0.3)

    def test_different_dt_values(self, integrated_pack_power_stage, sample_timestamp):
        base = sample_timestamp

        frame0 = FrameView(base, {CanonicalName.PackPower: 3600.0})
        integrated_pack_power_stage.run(frame0)

        frame1 = FrameView(base + timedelta(seconds=2.0), {CanonicalName.PackPower: 3600.0})
        result = integrated_pack_power_stage.run(frame1)
        assert result.read(CanonicalName.IntegratedPackPower) == pytest.approx(2.2)

    def test_large_time_gap(self, integrated_pack_power_stage, sample_timestamp):
        base = sample_timestamp

        frame0 = FrameView(base, {CanonicalName.PackPower: 1.0})
        integrated_pack_power_stage.run(frame0)

        frame1 = FrameView(base + timedelta(hours=1.0), {CanonicalName.PackPower: 1.0})
        result = integrated_pack_power_stage.run(frame1)
        expected = 1.0 * 0.2 / 3600 + 1.0 * 3600.0 / 3600.0
        assert result.read(CanonicalName.IntegratedPackPower) == pytest.approx(expected)

    def test_very_small_dt(self, integrated_pack_power_stage, sample_timestamp):
        base = sample_timestamp

        frame0 = FrameView(base, {CanonicalName.PackPower: 3600.0})
        integrated_pack_power_stage.run(frame0)

        frame1 = FrameView(base + timedelta(microseconds=1), {CanonicalName.PackPower: 3600.0})
        result = integrated_pack_power_stage.run(frame1)
        expected = 0.2 + 1e-6
        assert result.read(CanonicalName.IntegratedPackPower) == pytest.approx(expected)

    def test_state_resets_with_new_instance(self, make_frame_view):
        stage1 = IntegratedPackPower()
        frame = make_frame_view({CanonicalName.PackPower: 3600.0})
        stage1.run(frame)

        stage2 = IntegratedPackPower()
        result = stage2.run(frame)
        assert result.read(CanonicalName.IntegratedPackPower) == pytest.approx(0.2)
        assert stage2.total_energy == pytest.approx(0.2)

    def test_missing_input_raises_key_error(self, integrated_pack_power_stage, make_frame_view):
        frame = make_frame_view({})
        with pytest.raises(KeyError):
            integrated_pack_power_stage.run(frame)

    def test_output_frame_preserves_timestamp(self, sample_timestamp):
        stage = IntegratedPackPower()
        frame = FrameView(sample_timestamp, {CanonicalName.PackPower: 3600.0})
        result = stage.run(frame)
        assert result.timestamp == sample_timestamp

    def test_output_frame_only_contains_output(self, integrated_pack_power_stage, make_frame_view):
        frame = make_frame_view({CanonicalName.PackPower: 3600.0})
        result = integrated_pack_power_stage.run(frame)
        signals = [s for s, _ in result]
        assert signals == [CanonicalName.IntegratedPackPower]
