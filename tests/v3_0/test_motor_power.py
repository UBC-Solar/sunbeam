from data_tools.localization import CanonicalName
import pytest


class TestMotorPowerStage:

    def test_driving(self, motor_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 48.0,
            CanonicalName.MotorCurrent: 10.0,
            CanonicalName.MotorCurrentDirection: 0,
        })
        result = motor_power_stage.run(frame)
        assert result.read(CanonicalName.MotorPower) == pytest.approx(480.0)

    def test_regen(self, motor_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 48.0,
            CanonicalName.MotorCurrent: 5.0,
            CanonicalName.MotorCurrentDirection: 1,
        })
        result = motor_power_stage.run(frame)
        assert result.read(CanonicalName.MotorPower) == pytest.approx(-240.0)

    def test_zero_current(self, motor_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 48.0,
            CanonicalName.MotorCurrent: 0.0,
            CanonicalName.MotorCurrentDirection: 0,
        })
        result = motor_power_stage.run(frame)
        assert result.read(CanonicalName.MotorPower) == pytest.approx(0.0)

    def test_zero_voltage(self, motor_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 0.0,
            CanonicalName.MotorCurrent: 10.0,
            CanonicalName.MotorCurrentDirection: 0,
        })
        result = motor_power_stage.run(frame)
        assert result.read(CanonicalName.MotorPower) == pytest.approx(0.0)

    def test_driving_negative_current(self, motor_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 48.0,
            CanonicalName.MotorCurrent: -10.0,
            CanonicalName.MotorCurrentDirection: 0,
        })
        result = motor_power_stage.run(frame)
        assert result.read(CanonicalName.MotorPower) == pytest.approx(-480.0)

    def test_regen_negative_current(self, motor_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 48.0,
            CanonicalName.MotorCurrent: -5.0,
            CanonicalName.MotorCurrentDirection: 1,
        })
        result = motor_power_stage.run(frame)
        assert result.read(CanonicalName.MotorPower) == pytest.approx(240.0)

    def test_fractional_values(self, motor_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 52.3,
            CanonicalName.MotorCurrent: 8.75,
            CanonicalName.MotorCurrentDirection: 0,
        })
        expected = 52.3 * 8.75 * 1
        result = motor_power_stage.run(frame)
        assert result.read(CanonicalName.MotorPower) == pytest.approx(expected)

    def test_regen_fractional(self, motor_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 51.8,
            CanonicalName.MotorCurrent: 12.3,
            CanonicalName.MotorCurrentDirection: 1,
        })
        expected = 51.8 * 12.3 * (-1)
        result = motor_power_stage.run(frame)
        assert result.read(CanonicalName.MotorPower) == pytest.approx(expected)

    def test_missing_voltage_raises_key_error(self, motor_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.MotorCurrent: 10.0,
            CanonicalName.MotorCurrentDirection: 0,
        })
        with pytest.raises(KeyError):
            motor_power_stage.run(frame)

    def test_missing_current_raises_key_error(self, motor_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 48.0,
            CanonicalName.MotorCurrentDirection: 0,
        })
        with pytest.raises(KeyError):
            motor_power_stage.run(frame)

    def test_missing_direction_raises_key_error(self, motor_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 48.0,
            CanonicalName.MotorCurrent: 10.0,
        })
        with pytest.raises(KeyError):
            motor_power_stage.run(frame)

    def test_all_signals_missing_raises_key_error(self, motor_power_stage, make_frame_view):
        frame = make_frame_view({})
        with pytest.raises(KeyError):
            motor_power_stage.run(frame)

    def test_output_frame_preserves_timestamp(self, motor_power_stage, make_frame_view, sample_timestamp):
        frame = make_frame_view(
            values={
                CanonicalName.PackVoltage: 48.0,
                CanonicalName.MotorCurrent: 10.0,
                CanonicalName.MotorCurrentDirection: 0,
            },
            timestamp=sample_timestamp,
        )
        result = motor_power_stage.run(frame)
        assert result.timestamp == sample_timestamp

    def test_output_frame_only_contains_output(self, motor_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.PackVoltage: 48.0,
            CanonicalName.MotorCurrent: 10.0,
            CanonicalName.MotorCurrentDirection: 0,
        })
        result = motor_power_stage.run(frame)
        signals = [s for s, _ in result]
        assert signals == [CanonicalName.MotorPower]
