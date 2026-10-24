from data_tools.localization import CanonicalName
import pytest


class TestPackPowerStage:

    def test_basic_computation(self, pack_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 120.0,
            CanonicalName.PackCurrent: 50.0,
        })
        result = pack_power_stage.run(frame)
        assert result.read(CanonicalName.PackPower) == pytest.approx(6000.0)

    def test_negative_current(self, pack_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 120.0,
            CanonicalName.PackCurrent: -10.0,
        })
        result = pack_power_stage.run(frame)
        assert result.read(CanonicalName.PackPower) == pytest.approx(-1200.0)

    def test_zero_current(self, pack_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 120.0,
            CanonicalName.PackCurrent: 0.0,
        })
        result = pack_power_stage.run(frame)
        assert result.read(CanonicalName.PackPower) == pytest.approx(0.0)

    def test_zero_voltage(self, pack_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 0.0,
            CanonicalName.PackCurrent: 50.0,
        })
        result = pack_power_stage.run(frame)
        assert result.read(CanonicalName.PackPower) == pytest.approx(0.0)

    def test_negative_voltage(self, pack_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: -120.0,
            CanonicalName.PackCurrent: 50.0,
        })
        result = pack_power_stage.run(frame)
        assert result.read(CanonicalName.PackPower) == pytest.approx(-6000.0)

    def test_fractional_values(self, pack_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 121.3,
            CanonicalName.PackCurrent: 47.82,
        })
        expected = 121.3 * 47.82
        result = pack_power_stage.run(frame)
        assert result.read(CanonicalName.PackPower) == pytest.approx(expected)

    def test_large_power(self, pack_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 150.0,
            CanonicalName.PackCurrent: 200.0,
        })
        result = pack_power_stage.run(frame)
        assert result.read(CanonicalName.PackPower) == pytest.approx(30000.0)

    def test_missing_voltage_raises_key_error(self, pack_power_stage, make_frame_view):
        frame = make_frame_view({CanonicalName.PackCurrent: 50.0})
        with pytest.raises(KeyError):
            pack_power_stage.run(frame)

    def test_missing_current_raises_key_error(self, pack_power_stage, make_frame_view):
        frame = make_frame_view({CanonicalName.PackVoltage: 120.0})
        with pytest.raises(KeyError):
            pack_power_stage.run(frame)

    def test_both_signals_missing_raises_key_error(self, pack_power_stage, make_frame_view):
        frame = make_frame_view({})
        with pytest.raises(KeyError):
            pack_power_stage.run(frame)

    def test_output_frame_preserves_timestamp(self, pack_power_stage, make_frame_view, sample_timestamp):
        frame = make_frame_view(
            values={
                CanonicalName.PackVoltage: 120.0,
                CanonicalName.PackCurrent: 50.0,
            },
            timestamp=sample_timestamp,
        )
        result = pack_power_stage.run(frame)
        assert result.timestamp == sample_timestamp

    def test_output_frame_only_contains_output(self, pack_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 120.0,
            CanonicalName.PackCurrent: 50.0,
        })
        result = pack_power_stage.run(frame)
        signals = [s for s, _ in result]
        assert signals == [CanonicalName.PackPower]
