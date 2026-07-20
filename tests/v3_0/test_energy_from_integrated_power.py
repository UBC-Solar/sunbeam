from data_tools.localization import CanonicalName
from stage.v3_0.energy_from_integrated_power import EnergyFromIntegratedPower
import pytest


class TestEnergyFromIntegratedPowerStage:

    def test_initial_energy_is_none(self, energy_from_integrated_power_stage):
        assert energy_from_integrated_power_stage.initial_energy is None

    def test_first_call_captures_initial_energy(self, energy_from_integrated_power_stage, make_frame_view):
        frame = make_frame_view({
            CanonicalName.IntegratedPackPower: 0.0,
            CanonicalName.EnergyVOLExtrapolated: 1000.0,
        })
        result = energy_from_integrated_power_stage.run(frame)
        assert energy_from_integrated_power_stage.initial_energy == 1000.0
        assert result.read(CanonicalName.EnergyFromIntegratedPower) == pytest.approx(1000.0)

    def test_ipp_negative_increases_energy(self, energy_from_integrated_power_stage, make_frame_view):
        frame0 = make_frame_view({
            CanonicalName.IntegratedPackPower: 0.0,
            CanonicalName.EnergyVOLExtrapolated: 1000.0,
        })
        energy_from_integrated_power_stage.run(frame0)

        frame1 = make_frame_view({
            CanonicalName.IntegratedPackPower: -50.0,
            CanonicalName.EnergyVOLExtrapolated: 1000.0,
        })
        result = energy_from_integrated_power_stage.run(frame1)
        assert result.read(CanonicalName.EnergyFromIntegratedPower) == pytest.approx(1050.0)

    def test_multiple_calls(self, energy_from_integrated_power_stage, make_frame_view):
        frame0 = make_frame_view({
            CanonicalName.IntegratedPackPower: 0.0,
            CanonicalName.EnergyVOLExtrapolated: 1000.0,
        })
        result0 = energy_from_integrated_power_stage.run(frame0)
        assert result0.read(CanonicalName.EnergyFromIntegratedPower) == pytest.approx(1000.0)

        frame1 = make_frame_view({
            CanonicalName.IntegratedPackPower: 10.0,
            CanonicalName.EnergyVOLExtrapolated: 992.0,
        })
        result1 = energy_from_integrated_power_stage.run(frame1)
        assert result1.read(CanonicalName.EnergyFromIntegratedPower) == pytest.approx(990.0)

        frame2 = make_frame_view({
            CanonicalName.IntegratedPackPower: 25.0,
            CanonicalName.EnergyVOLExtrapolated: 977.0,
        })
        result2 = energy_from_integrated_power_stage.run(frame2)
        assert result2.read(CanonicalName.EnergyFromIntegratedPower) == pytest.approx(975.0)

        frame3 = make_frame_view({
            CanonicalName.IntegratedPackPower: 100.0,
            CanonicalName.EnergyVOLExtrapolated: 904.0,
        })
        result3 = energy_from_integrated_power_stage.run(frame3)
        assert result3.read(CanonicalName.EnergyFromIntegratedPower) == pytest.approx(900.0)

    def test_ipp_unchanged_energy_constant(self, energy_from_integrated_power_stage, make_frame_view):
        frame0 = make_frame_view({
            CanonicalName.IntegratedPackPower: 0.0,
            CanonicalName.EnergyVOLExtrapolated: 1000.0,
        })
        energy_from_integrated_power_stage.run(frame0)

        frame1 = make_frame_view({
            CanonicalName.IntegratedPackPower: 50.0,
            CanonicalName.EnergyVOLExtrapolated: 1000.0,
        })
        result1 = energy_from_integrated_power_stage.run(frame1)
        assert result1.read(CanonicalName.EnergyFromIntegratedPower) == pytest.approx(950.0)

        frame2 = make_frame_view({
            CanonicalName.IntegratedPackPower: 50.0,
            CanonicalName.EnergyVOLExtrapolated: 2000.0,
        })
        result2 = energy_from_integrated_power_stage.run(frame2)
        assert result2.read(CanonicalName.EnergyFromIntegratedPower) == pytest.approx(950.0)

    def test_state_resets_with_new_instance(self, make_frame_view):
        stage1 = EnergyFromIntegratedPower()
        frame = make_frame_view({
            CanonicalName.IntegratedPackPower: 10.0,
            CanonicalName.EnergyVOLExtrapolated: 500.0,
        })
        stage1.run(frame)

        stage2 = EnergyFromIntegratedPower()
        result = stage2.run(frame)
        assert result.read(CanonicalName.EnergyFromIntegratedPower) == pytest.approx(490.0)
        assert stage2.initial_energy == 500.0

    def test_missing_ipp_raises_key_error(self, energy_from_integrated_power_stage, make_frame_view):
        frame = make_frame_view({CanonicalName.EnergyVOLExtrapolated: 1000.0})
        with pytest.raises(KeyError):
            energy_from_integrated_power_stage.run(frame)

    def test_missing_eve_raises_key_error(self, energy_from_integrated_power_stage, make_frame_view):
        frame = make_frame_view({CanonicalName.IntegratedPackPower: 0.0})
        with pytest.raises(KeyError):
            energy_from_integrated_power_stage.run(frame)

    def test_both_signals_missing_raises_key_error(
        self, energy_from_integrated_power_stage, make_frame_view
    ):
        frame = make_frame_view({})
        with pytest.raises(KeyError):
            energy_from_integrated_power_stage.run(frame)

    def test_output_frame_preserves_timestamp(
        self, energy_from_integrated_power_stage, make_frame_view, sample_timestamp
    ):
        frame = make_frame_view(
            values={
                CanonicalName.IntegratedPackPower: 0.0,
                CanonicalName.EnergyVOLExtrapolated: 1000.0,
            },
            timestamp=sample_timestamp,
        )
        result = energy_from_integrated_power_stage.run(frame)
        assert result.timestamp == sample_timestamp

    def test_output_frame_only_contains_output(
        self, energy_from_integrated_power_stage, make_frame_view
    ):
        frame = make_frame_view({
            CanonicalName.IntegratedPackPower: 0.0,
            CanonicalName.EnergyVOLExtrapolated: 1000.0,
        })
        result = energy_from_integrated_power_stage.run(frame)
        signals = [s for s, _ in result]
        assert signals == [CanonicalName.EnergyFromIntegratedPower]
