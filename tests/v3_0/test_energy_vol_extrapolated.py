from data_tools.localization import CanonicalName
from stage.v3_0.energy_vol_extrapolated import EnergyVOLExtrapolated
from scipy.interpolate import CubicSpline
import pytest


class TestEnergyVOLExtrapolatedStage:
    def test_stage_name(self, energy_vol_extrapolated_stage):
        assert energy_vol_extrapolated_stage.stage_name == "EnergyVOLExtrapolated"

    def test_inputs(self, energy_vol_extrapolated_stage):
        assert energy_vol_extrapolated_stage.inputs == [
            CanonicalName.MinimumModuleVoltage,
        ]

    def test_outputs(self, energy_vol_extrapolated_stage):
        assert energy_vol_extrapolated_stage.outputs == [
            CanonicalName.EnergyVOLExtrapolated,
        ]

    def test_frequency(self, energy_vol_extrapolated_stage):
        assert energy_vol_extrapolated_stage.frequency == 10

    def test_battery_configuration(self, energy_vol_extrapolated_stage):
        assert energy_vol_extrapolated_stage.cells_in_module == 13
        assert energy_vol_extrapolated_stage.modules_in_pack == 32
        assert energy_vol_extrapolated_stage.cells_in_module * energy_vol_extrapolated_stage.modules_in_pack == 416

    def test_cubic_spline_initialized(self, energy_vol_extrapolated_stage):
        assert isinstance(energy_vol_extrapolated_stage.voltage_to_energy, CubicSpline)


    def test_voltage_at_low_end_of_range(self, energy_vol_extrapolated_stage, make_frame_view):
        frame = make_frame_view({CanonicalName.MinimumModuleVoltage: 2.701298701298701})
        result = energy_vol_extrapolated_stage.run(frame)
        expected_per_cell = 0.017736001074258087
        expected_total = expected_per_cell * 13 * 32
        assert result.read(CanonicalName.EnergyVOLExtrapolated) == pytest.approx(expected_total)

    def test_output_is_float(self, energy_vol_extrapolated_stage, make_frame_view):
        frame = make_frame_view({CanonicalName.MinimumModuleVoltage: 3.0})
        result = energy_vol_extrapolated_stage.run(frame)
        assert isinstance(result.read(CanonicalName.EnergyVOLExtrapolated), float)

    def test_computation_formula(self, energy_vol_extrapolated_stage, make_frame_view, monkeypatch):
        monkeypatch.setattr(energy_vol_extrapolated_stage, "voltage_to_energy", lambda x: 1.0)
        frame = make_frame_view({CanonicalName.MinimumModuleVoltage: 3.0})
        result = energy_vol_extrapolated_stage.run(frame)
        assert result.read(CanonicalName.EnergyVOLExtrapolated) == pytest.approx(416.0)

    def test_higher_voltage_gives_more_energy(self):
        stage = EnergyVOLExtrapolated()
        e1 = float(stage.voltage_to_energy(2.8))
        e2 = float(stage.voltage_to_energy(3.0))
        e3 = float(stage.voltage_to_energy(3.2))
        assert e1 <= e2 <= e3

    def test_missing_input_raises_key_error(
        self, energy_vol_extrapolated_stage, make_frame_view
    ):
        frame = make_frame_view({})
        with pytest.raises(KeyError):
            energy_vol_extrapolated_stage.run(frame)

    def test_output_frame_preserves_timestamp(
        self, energy_vol_extrapolated_stage, make_frame_view, sample_timestamp
    ):
        frame = make_frame_view(
            values={CanonicalName.MinimumModuleVoltage: 3.0},
            timestamp=sample_timestamp,
        )
        result = energy_vol_extrapolated_stage.run(frame)
        assert result.timestamp == sample_timestamp

    def test_output_frame_only_contains_output(
        self, energy_vol_extrapolated_stage, make_frame_view
    ):
        frame = make_frame_view({CanonicalName.MinimumModuleVoltage: 3.0})
        result = energy_vol_extrapolated_stage.run(frame)
        signals = [s for s, _ in result]
        assert signals == [CanonicalName.EnergyVOLExtrapolated]
