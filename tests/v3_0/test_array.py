from data_tools.localization import CanonicalName
from state.frame import FrameView
import pytest

class TestArrayStage:
    def test_stage_name(self, array_stage):
        assert array_stage.stage_name == "Array"

    def test_inputs(self, array_stage):
        assert array_stage.inputs == [
            CanonicalName.MPPTInputVoltageA,
            CanonicalName.MPPTInputCurrentA,
            CanonicalName.MPPTInputVoltageB,
            CanonicalName.MPPTInputCurrentB,
        ]

    def test_outputs(self, array_stage):
        assert array_stage.outputs == [CanonicalName.ArrayPower]

    def test_frequency(self, array_stage):
        assert array_stage.frequency == 4

    def test_basic_computation(self, array_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.MPPTInputVoltageA: 100.0,
            CanonicalName.MPPTInputCurrentA: 5.0,
            CanonicalName.MPPTInputVoltageB: 120.0,
            CanonicalName.MPPTInputCurrentB: 3.0,
        })
        result = array_stage.run(frame)
        assert result.read(CanonicalName.ArrayPower) == pytest.approx(860.0)

    def test_only_mppt_a_active(self, array_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.MPPTInputVoltageA: 100.0,
            CanonicalName.MPPTInputCurrentA: 5.0,
            CanonicalName.MPPTInputVoltageB: 120.0,
            CanonicalName.MPPTInputCurrentB: 0.0,
        })
        result = array_stage.run(frame)
        assert result.read(CanonicalName.ArrayPower) == pytest.approx(500.0)

    def test_only_mppt_b_active(self, array_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.MPPTInputVoltageA: 100.0,
            CanonicalName.MPPTInputCurrentA: 0.0,
            CanonicalName.MPPTInputVoltageB: 120.0,
            CanonicalName.MPPTInputCurrentB: 3.0,
        })
        result = array_stage.run(frame)
        assert result.read(CanonicalName.ArrayPower) == pytest.approx(360.0)

    def test_all_zero_values(self, array_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.MPPTInputVoltageA: 0.0,
            CanonicalName.MPPTInputCurrentA: 0.0,
            CanonicalName.MPPTInputVoltageB: 0.0,
            CanonicalName.MPPTInputCurrentB: 0.0,
        })
        result = array_stage.run(frame)
        assert result.read(CanonicalName.ArrayPower) == pytest.approx(0.0)

    def test_fractional_values(self, array_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.MPPTInputVoltageA: 98.7,
            CanonicalName.MPPTInputCurrentA: 4.32,
            CanonicalName.MPPTInputVoltageB: 121.5,
            CanonicalName.MPPTInputCurrentB: 2.89,
        })
        expected = 98.7 * 4.32 + 121.5 * 2.89
        result = array_stage.run(frame)
        assert result.read(CanonicalName.ArrayPower) == pytest.approx(expected)

    def test_large_values(self, array_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.MPPTInputVoltageA: 150.0,
            CanonicalName.MPPTInputCurrentA: 15.0,
            CanonicalName.MPPTInputVoltageB: 150.0,
            CanonicalName.MPPTInputCurrentB: 15.0,
        })
        result = array_stage.run(frame)
        assert result.read(CanonicalName.ArrayPower) == pytest.approx(4500.0)

    def test_small_values(self, array_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.MPPTInputVoltageA: 0.001,
            CanonicalName.MPPTInputCurrentA: 0.0001,
            CanonicalName.MPPTInputVoltageB: 0.001,
            CanonicalName.MPPTInputCurrentB: 0.0001,
        })
        result = array_stage.run(frame)
        assert result.read(CanonicalName.ArrayPower) == pytest.approx(2e-7)

    def test_missing_voltage_a_raises_key_error(self, array_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.MPPTInputCurrentA: 5.0,
            CanonicalName.MPPTInputVoltageB: 120.0,
            CanonicalName.MPPTInputCurrentB: 3.0,
        })
        with pytest.raises(KeyError):
            array_stage.run(frame)

    def test_missing_current_a_raises_key_error(self, array_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.MPPTInputVoltageA: 100.0,
            CanonicalName.MPPTInputVoltageB: 120.0,
            CanonicalName.MPPTInputCurrentB: 3.0,
        })
        with pytest.raises(KeyError):
            array_stage.run(frame)

    def test_missing_voltage_b_raises_key_error(self, array_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.MPPTInputVoltageA: 100.0,
            CanonicalName.MPPTInputCurrentA: 5.0,
            CanonicalName.MPPTInputCurrentB: 3.0,
        })
        with pytest.raises(KeyError):
            array_stage.run(frame)

    def test_missing_current_b_raises_key_error(self, array_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.MPPTInputVoltageA: 100.0,
            CanonicalName.MPPTInputCurrentA: 5.0,
            CanonicalName.MPPTInputVoltageB: 120.0,
        })
        with pytest.raises(KeyError):
            array_stage.run(frame)

    def test_all_signals_missing_raises_key_error(self, array_stage, make_frame_view):
        frame = make_frame_view({})
        with pytest.raises(KeyError):
            array_stage.run(frame)

    def test_output_frame_preserves_timestamp(self, array_stage, make_frame_view, sample_timestamp):
        frame = make_frame_view(
            values={
                CanonicalName.MPPTInputVoltageA: 100.0,
                CanonicalName.MPPTInputCurrentA: 5.0,
                CanonicalName.MPPTInputVoltageB: 120.0,
                CanonicalName.MPPTInputCurrentB: 3.0,
            },
            timestamp=sample_timestamp
        )
        result = array_stage.run(frame)
        assert result.timestamp == sample_timestamp

    def test_output_frame_only_contains_output(self, array_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.MPPTInputVoltageA: 100.0,
            CanonicalName.MPPTInputCurrentA: 5.0,
            CanonicalName.MPPTInputVoltageB: 120.0,
            CanonicalName.MPPTInputCurrentB: 3.0,
        })
        result = array_stage.run(frame)
        signals = [s for s, _ in result]
        assert signals == [CanonicalName.ArrayPower]