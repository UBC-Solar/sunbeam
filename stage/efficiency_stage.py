from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from data_tools.collections import TimeSeries
from prefect import task
import numpy as np

MIN_AVG_METERS_PER_SEC = 2
MAX_AVG_METERS_PER_SEC = 50
MIN_AVG_WATTS = 0
MAX_AVG_WATTS = 10_000

ncm_lap_len_m = 5040.


def windowed_mean(arr: np.ndarray, factor: int, allow_truncate=False) -> np.ndarray:
    """Returns a new array representing the windowed mean of ``arr``.

    :param ndarray arr: The array for which to compute a windowed mean.
    :param int factor: The number of indices grouped into each window.
        If ``allow_truncate`` is False, ``arr.size`` must be divisible by ``factor``.
    :param bool allow_truncate: If true, allow this function to trim the end of ``arr``
        until it is a multiple of ``factor``.
    :return: A new array representing the windowed mean of ``arr``.
    """
    assert arr.ndim == 1, "can only down-sample 1-d array"
    if allow_truncate:
        arr = arr[:-(arr.size % factor)]
    else:
        assert arr.size % factor == 0, "array length must be a multiple of down-sampling factor"
    reshaped = np.reshape(arr, (-1, factor))
    return np.nanmean(reshaped, axis=1)


class EfficiencyStage(Stage):
    @classmethod
    def get_stage_name(cls):
        return "efficiency"

    @staticmethod
    def dependencies():
        return ["ingress", "power"]

    @staticmethod
    @task(name="Efficiency")
    def run(self, vehicle_velocity_loader: FileLoader, motor_power_loader: FileLoader) -> tuple[FileLoader, ...]:
        """
        TODO: writeme

        :param self: an instance of EfficiencyStage to be run
        :param FileLoader vehicle_velocity_loader: loader to VehicleVelocity from Ingest
        :param FileLoader motor_power_loader: loader to Motor Power from PowerStage
        :returns: Efficiency5Minute, Efficiency1Hour, EfficiencyLapDist (FileLoaders pointing to TimeSeries)
        """
        return super().run(self, vehicle_velocity_loader, motor_power_loader)

    @property
    def event_name(self):
        return self._event_name

    def __init__(self, event_name: str):
        """
        :param str event_name: which event is currently being processed
        """
        super().__init__()

        self._event_name = event_name

    def extract(self, vehicle_velocity_loader: FileLoader, motor_power_loader: FileLoader) -> tuple[Result, Result]:
        vehicle_velocity_result: Result = vehicle_velocity_loader()
        motor_power_result: Result = motor_power_loader()

        return vehicle_velocity_result, motor_power_result

    @staticmethod
    def get_periodic_efficiency(
            vehicle_velocity_aligned: TimeSeries,
            motor_power_aligned: TimeSeries,
            period_seconds: float
    ) -> TimeSeries:
        # (seconds/window) / (seconds/index) == indices/window
        downsample_factor = int(period_seconds / vehicle_velocity_aligned.period)
        vehicle_velocity_averaged: np.ndarray = windowed_mean(
            np.array(vehicle_velocity_aligned),
            downsample_factor,
            allow_truncate=True
        )

        motor_power_averaged: np.ndarray = windowed_mean(
            np.array(motor_power_aligned),
            downsample_factor,
            allow_truncate=True
        )

        efficiency_array = motor_power_averaged / vehicle_velocity_averaged  # (J/s) / (m/s) = J/m

        # clean bad values by setting them to zero
        bad_values_mask = EfficiencyStage.get_anomaly_mask(motor_power_averaged, vehicle_velocity_averaged)
        efficiency_array[bad_values_mask] = np.nan
        efficiency = vehicle_velocity_aligned.promote(efficiency_array)

        efficiency.meta['period'] = period_seconds  # important: update the period for this TimeSeries
        efficiency.units = "J/m"
        return efficiency

    @staticmethod
    def get_anomaly_mask(motor_power_averaged: np.ndarray, vehicle_velocity_averaged: np.ndarray) -> np.ndarray:
        bad_values_mask: np.ndarray = ((vehicle_velocity_averaged > MAX_AVG_METERS_PER_SEC)
                                       | (vehicle_velocity_averaged < MIN_AVG_METERS_PER_SEC)
                                       | (motor_power_averaged < MIN_AVG_WATTS)
                                       | (motor_power_averaged > MAX_AVG_WATTS))
        return bad_values_mask

    @staticmethod
    def get_lap_dist_efficiency(vehicle_velocity_aligned, motor_power_aligned, lap_len_m) -> np.ndarray:
        integrated_velocity_m = np.cumsum(vehicle_velocity_aligned) * vehicle_velocity_aligned.period
        lap_index: list = [int(dist_m // lap_len_m) for dist_m in integrated_velocity_m]
        efficiency_lap_dist = np.zeros(max(lap_index) + 1)
        vv_aligned_arr = np.array(vehicle_velocity_aligned)
        mp_aligned_arr = np.array(motor_power_aligned)

        sum_power = 0
        sum_velocity = 0
        num_vals = 0
        prev_lap_idx = 0
        for array_index, lap_idx in enumerate(lap_index):
            if lap_idx > prev_lap_idx:
                # start of a new lap
                avg_power = sum_power / num_vals
                avg_velocity = sum_velocity / num_vals
                if ((avg_velocity > MAX_AVG_METERS_PER_SEC) | (avg_velocity < MIN_AVG_METERS_PER_SEC)
                        | (avg_power < MIN_AVG_WATTS) | (avg_power > MAX_AVG_WATTS)):
                    efficiency_lap_dist[lap_idx] = np.nan  # invalid data
                else:
                    efficiency_lap_dist[lap_idx] = avg_power / avg_velocity
                sum_power = 0
                sum_velocity = 0
                num_vals = 0
                prev_lap_idx = lap_idx
            sum_power += mp_aligned_arr[array_index]
            sum_velocity += vv_aligned_arr[array_index]
            num_vals += 1
        return np.array(efficiency_lap_dist)

    def transform(self, vehicle_velocity_result, motor_power_result) -> tuple[Result, Result, Result]:
        try:
            vehicle_velocity_ts: TimeSeries = vehicle_velocity_result.unwrap().data
            motor_power_ts: TimeSeries = motor_power_result.unwrap().data
            vehicle_velocity_aligned, motor_power_aligned = TimeSeries.align(
                vehicle_velocity_ts, motor_power_ts)

            efficiency_5min: TimeSeries = self.get_periodic_efficiency(
                vehicle_velocity_aligned,
                motor_power_aligned,
                300
            )

            efficiency_5min.name = "Efficiency5Minute"
            efficiency_5min_result = Result.Ok(efficiency_5min)

            efficiency_1h: TimeSeries = self.get_periodic_efficiency(
                vehicle_velocity_aligned,
                motor_power_aligned,
                3600
            )

            efficiency_1h.name = "Efficiency1Hour"
            efficiency_1h_result = Result.Ok(efficiency_1h)

            efficiency_lap_dist: np.ndarray = self.get_lap_dist_efficiency(
                vehicle_velocity_aligned,
                motor_power_aligned,
                ncm_lap_len_m
            )

            efficiency_lap_dist_result = Result.Ok(efficiency_lap_dist)

        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            efficiency_5min_result = Result.Err(RuntimeError("Failed to process Efficiency5Minute!"))
            efficiency_1h_result = Result.Err(RuntimeError("Failed to process Efficiency1Hour!"))
            efficiency_lap_dist_result = Result.Err(RuntimeError("Failed to process EfficiencyLapDist!"))

        return efficiency_5min_result, efficiency_1h_result, efficiency_lap_dist_result

    def load(self,
             efficiency_5min_result,
             efficiency_1h_result,
             efficiency_lap_dist_result
             ) -> tuple[FileLoader, FileLoader, FileLoader]:
        efficiency_5min_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="Efficiency5Minute",
            ),
            file_type=FileType.TimeSeries,
            data=efficiency_5min_result.unwrap() if efficiency_5min_result else None,
            description="Driving efficiency in J/m, computed as avg_motor_power / avg_vehicle_velocity with values "
                        "averaged over 5-minute periods. Values are np.nan where mean velocity is outside "
                        "the range [2, 50] m/s or if mean power is outside the range [0, 10] kW."
        )

        efficiency_1h_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="Efficiency1Hour",
            ),
            file_type=FileType.TimeSeries,
            data=efficiency_1h_result.unwrap() if efficiency_1h_result else None,
            description="Driving efficiency in J/m, computed as avg_motor_power / avg_vehicle_velocity with values "
                        "averaged over 1-hour periods. Values are np.nan where mean velocity is outside "
                        "the range [2, 50] m/s or if mean power is outside the range [0, 10] kW."
        )

        efficiency_lap_dist_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="EfficiencyLapDist",
            ),
            file_type=FileType.TimeSeries,  # actually an ndarray but this is not yet supported
            data=efficiency_lap_dist_result.unwrap() if efficiency_lap_dist_result else None,
            description="Driving efficiency in J/m, computed as avg_motor_power / avg_vehicle_velocity with values "
                        "averaged over each lap. The lap splitting calculation starts by integrating VehicelVelocity to"
                        " get total distance as a function over time. Then, the values are split into lengths of 5.04km"
                        " as this is the length of the FSGP track. Then, avg_motor_power & avg_vehicle_velocity are "
                        " taken over the timespan for each given lap. Since there is some error in VehicleVelocity and "
                        "not all distance travelled is along the track, this should not be relied upon to exactly align"
                        " with real lap times. However, it is a decent estimate: it predicts 48 laps for FSGP day 1 "
                        "where the real number was 46. Values are np.nan where mean velocity is outside "
                        "the range [2, 50] m/s or if mean power is outside the range [0, 10] kW."
        )

        efficiency_5min_loader = self.context.data_source.store(efficiency_5min_file)
        self.logger.info(f"Successfully loaded Efficiency1Hour!")

        efficiency_1h_loader = self.context.data_source.store(efficiency_1h_file)
        self.logger.info(f"Successfully loaded Efficiency5Minute!")

        efficiency_lap_dist_loader = self.context.data_source.store(efficiency_lap_dist_file)
        self.logger.info(f"Successfully loaded EfficiencyLapDist!")

        return efficiency_5min_loader, efficiency_1h_loader, efficiency_lap_dist_loader


stage_registry.register_stage(EfficiencyStage.get_stage_name(), EfficiencyStage)
