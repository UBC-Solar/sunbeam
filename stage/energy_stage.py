from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from data_tools.collections import TimeSeries
from prefect import task
from scipy.interpolate import CubicSpline
import numpy as np
from typing import Callable
from physics.models.battery import BatteryModelConfig, KalmanFilterConfig, EquivalentCircuitBatteryModel, \
    FilteredBatteryModelConfig


class EnergyStage(Stage):
    @classmethod
    def get_stage_name(cls):
        return "energy"

    @staticmethod
    def dependencies():
        return ["ingress", "power"]

    @staticmethod
    @task(name="Energy")
    def run(
            self,
            voltage_of_least_loader: FileLoader,
            pack_power_loader: FileLoader,
            total_pack_voltage_loader: FileLoader,
            pack_current_loader: FileLoader,
    ) -> tuple[FileLoader, ...]:
        """
        Run the Energy stage, producing battery energy estimates using various techniques.

        1. EnergyVOLExtrapolated
            Battery energy estimated using VoltageofLeast & SANYO NCR18650GA datasheet 2A discharge curve.
            See https://github.com/UBC-Solar/data_analysis/blob/main/soc_analysis/datasheet_voltage_soc/charge_voltage_energy.ipynb for details.


        :param EnergyStage self: an instance of EnergyStage to be run
        :param FileLoader voltage_of_least_loader: loader to VoltageofLeast from Ingress
        :param FileLoader pack_power_loader: loader to Pack Power from PowerStage
        :param FileLoader total_pack_voltage_loader: loader to TotalPackVoltage from Ingress
        :param FileLoader pack_current_loader: loader to PackCurrent from Ingress
        :returns: EnergyVOLExtrapolated, IntegratedPackPower, EnergyFromIntegratedPower, UnfilteredSOC, SOC
        """
        return super().run(
            self,
            voltage_of_least_loader,
            pack_power_loader,
            total_pack_voltage_loader,
            pack_current_loader
        )

    @property
    def event_name(self):
        return self._event_name

    def __init__(self, event_name: str):
        """
        :param str event_name: which event is currently being processed
        """
        super().__init__()

        self._event_name = event_name

    def extract(
            self,
            voltage_of_least_loader: FileLoader,
            pack_power_loader: FileLoader,
            total_pack_voltage_loader: FileLoader,
            pack_current_loader: FileLoader,
    ) -> tuple[
        Result[File], Result[File], Result[dict], Result[File],
        Result[File], Result[BatteryModelConfig], Result[KalmanFilterConfig], Result[dict]
    ]:
        voltage_of_least_result: Result = voltage_of_least_loader()
        pack_power_result: Result = pack_power_loader()
        total_pack_voltage_result: Result = total_pack_voltage_loader()
        pack_current_result: Result = pack_current_loader()

        try:
            battery_configuration_result = Result.Ok(self.stage_data["battery_configuration"])
        except KeyError:
            battery_configuration_result = Result.Err(KeyError(
                "No battery configuration found! \n"
                "Expected at energy/battery_configuration")
            )

        try:
            battery_model_config_data = self.stage_data[self.event_name]["battery_model_config"]
            battery_model_config_result = Result.Ok(BatteryModelConfig(**battery_model_config_data))

            try:
                kalman_filter_config_data = self.stage_data[self.event_name]["kalman_filter_config"]
                kalman_filter_config_result = Result.Ok(KalmanFilterConfig(**kalman_filter_config_data))

            except KeyError:
                kalman_filter_config_result = Result.Err(KeyError(
                    "No Kalman filter config found! \n"
                    f"Expected at energy/{self.event_name}/kalman_filter_config")
                )
            except Exception as e:  # probably check for a Pydantic error
                kalman_filter_config_result = Result.Err(KeyError(f"Could not create Kalman filter configuration: {e}"))

        except KeyError:
            battery_model_config_result = Result.Err(KeyError(
                "No battery model config found! \n"
                f"Expected at energy/{self.event_name}/battery_model_config")
            )
            kalman_filter_config_result = Result.Err(RuntimeError(f"Failed to create Kalman filter configuration since "
                                                                  f"battery model configuration creation failed!"))

        except Exception as e:
            battery_model_config_result = Result.Err(RuntimeError(f"Could not create battery model configuration: {e}"))
            kalman_filter_config_result = Result.Err(RuntimeError(f"Failed to create Kalman filter configuration since "
                                                                  f"battery model configuration creation failed!"))

        try:
            initial_state = Result.Ok(self.stage_data[self.event_name]["initial_state"])

        except KeyError:
            initial_state = Result.Err(KeyError(
                "No initial state found! \n"
                f"Expected at energy/{self.event_name}/initial_state")
            )

        return (
            voltage_of_least_result,
            pack_power_result,
            battery_configuration_result,
            total_pack_voltage_result,
            pack_current_result,
            battery_model_config_result,
            kalman_filter_config_result,
            initial_state
        )

    @staticmethod
    def _compute_integrated_pack_power(pack_power: TimeSeries) -> Result[TimeSeries]:
        seconds_per_hour = 3600
        integrated_pack_power_ts = pack_power.promote(np.cumsum(pack_power) * pack_power.period / seconds_per_hour)
        integrated_pack_power_ts.name = "IntegratedPackPower"
        integrated_pack_power_ts.units = "Wh"

        return integrated_pack_power_ts

    @staticmethod
    def _compute_energy_vol_extrapolated(
            battery_configuration: dict,
            voltage_of_least: TimeSeries
    ) -> Result[TimeSeries]:
        voltage_wh_lookup = battery_configuration["voltage_wh_lookup"]
        cells_in_module = battery_configuration["cells_in_module"]
        modules_in_pack = battery_configuration["modules_in_pack"]

        vol_cell_wh_from_voltage: Callable = CubicSpline(voltage_wh_lookup[0], voltage_wh_lookup[1])
        vol_cell_wh = vol_cell_wh_from_voltage(voltage_of_least)
        energy_vol_extrapolated_ts: TimeSeries = voltage_of_least.promote(
            vol_cell_wh * cells_in_module * modules_in_pack)
        energy_vol_extrapolated_ts.name = "EnergyVOLExtrapolated"
        energy_vol_extrapolated_ts.units = "Wh"
        energy_vol_extrapolated: Result[TimeSeries] = Result.Ok(energy_vol_extrapolated_ts)

        return energy_vol_extrapolated

    @staticmethod
    def _compute_energy_from_integrated_power(
            energy_vol_extrapolated_ts: TimeSeries,
            integrated_pack_power_ts: TimeSeries
    ) -> Result[TimeSeries]:
        initial_energy = energy_vol_extrapolated_ts[0]
        energy_from_integrated_power_ts = energy_vol_extrapolated_ts.promote(
            np.ones(integrated_pack_power_ts.size) * initial_energy - integrated_pack_power_ts)
        energy_from_integrated_power_ts.name = "EnergyFromIntegratedPower"
        energy_from_integrated_power_ts.units = "Wh"
        energy_from_integrated_power = Result.Ok(energy_from_integrated_power_ts)

        return energy_from_integrated_power

    def transform(
            self,
            voltage_of_least_result: Result[File],
            pack_power_result: Result[File],
            battery_configuration_result: Result[dict],
            total_pack_voltage_result: Result[File],
            pack_current_result: Result[File],
            battery_model_config_result: Result[BatteryModelConfig],
            kalman_filter_config_result: Result[KalmanFilterConfig],
            initial_state_result: Result[dict]
    ) -> tuple[Result, Result, Result, Result, Result]:
        # First, we need pack_power to compute integrated_pack_power
        try:
            pack_power: TimeSeries = pack_power_result.unwrap().data
            integrated_pack_power: Result[TimeSeries] = self._compute_integrated_pack_power(pack_power)

        except UnwrappedError as e:
            error_cause = f"Failed to unwrap pack power result! \n {e}"
            self.logger.error(error_cause)
            integrated_pack_power: Result[TimeSeries] = Result.Err(RuntimeError(error_cause))

        # Next, try to use battery_configuration and voltage_of_least to compute energy_vol_extrapolated
        try:
            battery_configuration = battery_configuration_result.unwrap()
            voltage_of_least: TimeSeries = voltage_of_least_result.unwrap().data

            energy_vol_extrapolated: Result[TimeSeries] = self._compute_energy_vol_extrapolated(
                battery_configuration,
                voltage_of_least
            )

        except UnwrappedError as e:
            error_reason = f"Failed to process EnergyVOLExtrapolated! \n {e}"
            self.logger.error(error_reason)
            energy_vol_extrapolated: Result[TimeSeries] = Result.Err(RuntimeWarning(error_reason))

        # Now, if we have both integrated_pack_power and energy_vol_extrapolated, compute energy_from_integrated_power
        try:
            integrated_pack_power_ts: TimeSeries = integrated_pack_power.unwrap()

            try:
                energy_vol_extrapolated_ts: TimeSeries = energy_vol_extrapolated.unwrap()

                energy_from_integrated_power = self._compute_energy_from_integrated_power(
                    energy_vol_extrapolated_ts,
                    integrated_pack_power_ts
                )

            except UnwrappedError as e:
                error_cause = f"Failed to process EnergyFromIntegratedPower! \n {e}"
                self.logger.error(error_cause)
                energy_from_integrated_power = Result.Err(RuntimeError(error_cause))

        except UnwrappedError as e:
            error_reason = f"Failed to process EnergyFromIntegratedPower! \n {e}"
            self.logger.error(error_reason)
            energy_from_integrated_power = Result.Err(RuntimeError(error_reason))

        # To compute SOC, we need total_pack_voltage, pack_current, initial_state, and battery_model_config
        try:
            total_pack_voltage = total_pack_voltage_result.unwrap()
            pack_current = pack_current_result.unwrap()
            battery_model_config = battery_model_config_result.unwrap()

            # If we can't get initial state, grab it using the first voltage measurement
            initial_state = initial_state_result.unwrap()
            unfiltered_soc = None

            # To compute filtered SOC, we also need kalman_filter_config
            try:
                kalman_filter_config = kalman_filter_config_result.unwrap()
                soc = None

            except UnwrappedError as e:
                error_reason = f"Failed to compute filtered SOC! \n {e}"
                self.logger.error(error_reason)
                soc = Result.Err(RuntimeError(error_reason))

        except UnwrappedError as e:
            error_reason = f"Failed to compute SOC! \n {e}"
            self.logger.error(error_reason)
            unfiltered_soc = Result.Err(RuntimeError(error_reason))
            soc = Result.Err(RuntimeError(error_reason))

        return integrated_pack_power, energy_vol_extrapolated, energy_from_integrated_power, unfiltered_soc, soc

    def load(
            self,
            integrated_pack_power,
            energy_vol_extrapolated,
            energy_from_integrated_power,
            unfiltered_soc,
            soc
    ) -> tuple[FileLoader, FileLoader, FileLoader, FileLoader, FileLoader]:
        integrated_pack_power_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=EnergyStage.get_stage_name(),
                name="IntegratedPackPower",
            ),
            file_type=FileType.TimeSeries,
            data=integrated_pack_power.unwrap() if integrated_pack_power else None,
            description="The integral of pack power in watt-hours. Starts at zero for each event."
        )

        energy_vol_extrapolated_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=EnergyStage.get_stage_name(),
                name="EnergyVOLExtrapolated",
            ),
            file_type=FileType.TimeSeries,
            data=energy_vol_extrapolated.unwrap() if energy_vol_extrapolated else None,
            description="Battery energy estimated using VoltageofLeast & SANYO NCR18650GA datasheet 2A discharge curve. "
                        "See https://github.com/UBC-Solar/data_analysis/blob/main/soc_analysis/datasheet_voltage_soc/charge_voltage_energy.ipynb for details."
        )

        energy_from_integrated_power_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=EnergyStage.get_stage_name(),
                name="EnergyFromIntegratedPower",
            ),
            file_type=FileType.TimeSeries,
            data=energy_from_integrated_power.unwrap() if energy_from_integrated_power else None,
            description="Estimated energy in the battery in watt-hours. The starting value is given by the starting"
                        "value of EnergyVOLExtrapolated, then IntegratedPackPower is subtracted from this. "
                        "This technique of determining battery energy is equivalent to 'coulomb counting'. "
                        "Values may be smoother than EnergyVOLExtrapolated and more accurate in the short term, "
                        "but can diverge from the truth over time as systematic error is integrated."
                        "This value is also problematic if telemetry is missing values, as this will "
                        "appear as if no power was used during this time."
        )

        unfiltered_soc_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=EnergyStage.get_stage_name(),
                name="UnfilteredSOC",
            ),
            file_type=FileType.TimeSeries,
            data=unfiltered_soc.unwrap() if unfiltered_soc else None,
            description="Estimated SOC of our battery, in dimensionless units in the range [0, 1], by modeling our"
                        "battery as a first-order Thevenin equivalent circuit. The accuracy of this SOC estimation"
                        "is limited by the accuracy of the current sensor used to produce the model inputs."
        )

        soc_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=EnergyStage.get_stage_name(),
                name="SOC",
            ),
            file_type=FileType.TimeSeries,
            data=soc.unwrap() if soc else None,
            description="Estimated SOC of our battery, in dimensionless units in the range [0, 1], by modeling our"
                        "battery as a first-order Thevenin equivalent circuit and then filtering the model predictions"
                        "with an Extended Kalman Filter using terminal voltage measurements to correct for current"
                        "sensor error. Sensitive to variations in the battery state of health since testing used"
                        "to generate the model parameters."
        )

        integrated_pack_power_loader = self.context.data_source.store(integrated_pack_power_file)
        self.logger.info(f"Successfully loaded IntegratedPackPower!")

        energy_vol_extrapolated_loader = self.context.data_source.store(energy_vol_extrapolated_file)
        self.logger.info(f"Successfully loaded EnergyVOLExtrapolated!")

        energy_from_integrated_power_loader = self.context.data_source.store(energy_from_integrated_power_file)
        self.logger.info(f"Successfully loaded EnergyFromIntegratedPower!")

        unfiltered_soc_loader = self.context.data_source.store(unfiltered_soc_file)
        self.logger.info(f"Successfully loaded UnfilteredSOC!")

        soc_loader = self.context.data_source.store(soc_file)
        self.logger.info(f"Successfully loaded SOC!")

        return (
            integrated_pack_power_loader,
            energy_vol_extrapolated_loader,
            energy_from_integrated_power_loader,
            unfiltered_soc_loader,
            soc_loader
        )


stage_registry.register_stage(EnergyStage.get_stage_name(), EnergyStage)
