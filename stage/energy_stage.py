from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath, Event
from data_tools.collections import TimeSeries
from prefect import task
from scipy.interpolate import CubicSpline
import numpy as np
from numpy.typing import NDArray
from typing import Callable
from physics.models.battery import BatteryModelConfig, KalmanFilterConfig, EquivalentCircuitBatteryModel, \
    FilteredBatteryModel


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
        2. IntegratedPackPower
            The integral of pack power in watt-hours. Starts at zero for each event.
        3. EnergyFromIntegratedPower
            Estimated energy in the battery in watt-hours. The starting value is given by the startingvalue of
            EnergyVOLExtrapolated, then IntegratedPackPower is subtracted from this. This technique of determining
            battery energy is equivalent to 'coulomb counting'. Values may be smoother than EnergyVOLExtrapolated and
            more accurate in the short term, but can diverge from the truth over time as systematic error is integrated.
            This value is also problematic if telemetry is missing values, as this will appear as if no power was used
            during this time.


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
        return self._event.name

    def __init__(self, event: Event):
        """
        :param Event event: which event is currently being processed
        """
        super().__init__()

        self._event = event

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
                kalman_filter_config_data.update({"battery_model_config": battery_model_config_result.unwrap()})

                kalman_filter_config_data["state_covariance_matrix"] = np.array(kalman_filter_config_data["state_covariance_matrix"])
                kalman_filter_config_data["process_noise_matrix"] = np.array(kalman_filter_config_data["process_noise_matrix"])
                kalman_filter_config_data["measurement_noise_vector"] = np.array([kalman_filter_config_data["measurement_noise_vector"]])

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
            integrated_pack_power: Result[TimeSeries] = Result.Ok(self._compute_integrated_pack_power(pack_power))

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

                energy_from_integrated_power = Result.Ok(self._compute_energy_from_integrated_power(
                    energy_vol_extrapolated_ts,
                    integrated_pack_power_ts
                ))

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
            total_pack_voltage: TimeSeries = total_pack_voltage_result.unwrap().data
            raw_pack_current: TimeSeries = pack_current_result.unwrap().data

            current_error = np.polyval([-0.00388, 1547], raw_pack_current * 1000.0)
            pack_current = raw_pack_current - (current_error / 1000)

            battery_model_config = battery_model_config_result.unwrap()

            total_pack_voltage, pack_current = TimeSeries.align(total_pack_voltage, pack_current)

            # If we can't get initial state, grab it using the first voltage measurement using
            # the assumption that when the car starts up the battery will be pretty relaxed,
            # so Uoc is approximated by the terminal voltage
            if not initial_state_result:
                Uoc_from_SOC = battery_model_config.get_Uoc
                initial_soc = binary_search_inverse(
                    Uoc_from_SOC,
                    total_pack_voltage[0],
                    a=0.0, b=1.1,  # 0.0 < SOC < 1.1, since we may be underestimating battery health
                    max_iter=50
                )

            else:
                initial_state = initial_state_result.unwrap()
                initial_soc = initial_state["state"]["initial_soc"]

            battery_model = EquivalentCircuitBatteryModel(battery_model_config, initial_soc)
            unfiltered_soc_array, _ = battery_model.update_array(pack_current.period, current_array=-pack_current)
            unfiltered_soc = Result.Ok(into_soc_timeseries(unfiltered_soc_array, pack_current))

            # To compute filtered SOC, we also need kalman_filter_config
            try:
                kalman_filter_config = kalman_filter_config_result.unwrap()
                filtered_battery_model = FilteredBatteryModel(kalman_filter_config, initial_soc)

                SOC_array = np.zeros_like(pack_current)
                dt = total_pack_voltage.period

                self.logger.info("Computing filtered SOC...")

                for i, (voltage, current) in enumerate(zip(total_pack_voltage, pack_current)):
                    filtered_battery_model.predict_then_update(voltage, current, dt)

                    SOC_array[i] = filtered_battery_model.SOC

                self.logger.info(f"Finished computing filtered SOC! Final SOC: {SOC_array[-1]:.3f}")
                soc = Result.Ok(into_soc_timeseries(SOC_array, pack_current))

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


def into_soc_timeseries(values: NDArray, reference: TimeSeries) -> TimeSeries:
    values_ts = reference.promote(values)
    values_ts.units = ""
    values_ts.meta["field"] = "SOC"

    return values_ts


def binary_search_inverse(
    f: Callable[[float], float],
    target: float,
    a: float = 0.0,
    b: float = 1.0,
    rel_tol: float =1e-3,
    max_iter: int = 50
):
    """
    Solve f(x) = target for x ∈ [a,b], assuming f is strictly decreasing.
    Stop when |f(mid) - target| / |target| <= rel_tol or after max_iter.
    Uses a binary search.
    """
    fa = f(a)
    fb = f(b)
    if not (fb >= target >= fa):
        raise ValueError("Bracket [a, b] does not contain target because "
                         "f(a), f(b) are not on opposite sides of target.")

    low, high = a, b
    for i in range(max_iter):
        mid = 0.5 * (low + high)
        fmid = f(mid)

        # Check relative error in y
        if abs(fmid - target) <= rel_tol * abs(target):
            return mid

        # Because f is decreasing: f(low) > f(high).
        # If f(mid) > target, mid is "too low" -> move low up.
        if fmid < target:
            low = mid
        else:
            high = mid

    # If we exit loop, return the midpoint anyway
    return 0.5 * (low + high)


stage_registry.register_stage(EnergyStage.get_stage_name(), EnergyStage)
