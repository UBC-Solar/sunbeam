from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from data_tools.collections import TimeSeries
from prefect import task
import numpy as np

min_avg_meters_per_sec = 2
max_avg_meters_per_sec = 50
min_avg_watts = 0
max_avg_watts = 10_000

def windowed_mean(arr: np.ndarray, factor:int, allow_truncate=False) -> np.ndarray:
    """Returns a new array representing the windowed mean of ``arr``.

    :param ndarray arr: The array for which to compute a windowed mean.
    :param int factor: The number of indices grouped into each window.
        If ``allow_truncate`` is False, ``arr.size`` must be divisible by ``factor``.
    :param bool allow_truncate: If true, allow this function to trim the end of ``arr``
        until it is a multiple of ``factor``.
    :return: A new array representing the windowed mean of ``arr``.
    """
    assert arr.ndim == 1, "can only down-sample 1-d array"
    if allow_truncate: arr = arr[:-(arr.size % factor)]
    else: assert arr.size % factor == 0, "array length must be a multiple of down-sampling factor"
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
    def get_periodic_efficiency(vehicle_velocity_aligned: TimeSeries, motor_power_aligned: TimeSeries,
                                period_seconds: float) -> TimeSeries:
        # (seconds/window) / (seconds/index) == indices/window
        downsample_factor = int(period_seconds / vehicle_velocity_aligned.period)
        vehicle_velocity_averaged: np.ndarray = windowed_mean(
            np.array(vehicle_velocity_aligned), downsample_factor, allow_truncate=True)
        motor_power_averaged: np.ndarray = windowed_mean(
            np.array(motor_power_aligned), downsample_factor, allow_truncate=True)
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
        bad_values_mask: np.ndarray = ((vehicle_velocity_averaged > max_avg_meters_per_sec)
                                       | (vehicle_velocity_averaged < min_avg_meters_per_sec)
                                       | (motor_power_averaged < min_avg_watts)
                                       | (motor_power_averaged > max_avg_watts))
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
                if ((avg_velocity > max_avg_meters_per_sec) | (avg_velocity < min_avg_meters_per_sec)
                        | (avg_power < min_avg_watts) | (avg_power > max_avg_watts)):
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

            efficiency_5min: TimeSeries = self.get_periodic_efficiency(vehicle_velocity_aligned, motor_power_aligned, 300)
            efficiency_5min.name = "Efficiency5Minute"
            efficiency_5min_result = Result.Ok(efficiency_5min)

            efficiency_1h: TimeSeries = self.get_periodic_efficiency(vehicle_velocity_aligned, motor_power_aligned, 3600)
            efficiency_1h.name = "Efficiency1Hour"
            efficiency_1h_result = Result.Ok(efficiency_1h)

            ncm_lap_len_m = 5040.  # TODO: get based on from config?
            efficiency_lap_dist: np.ndarray = self.get_lap_dist_efficiency(vehicle_velocity_aligned, motor_power_aligned, ncm_lap_len_m)
            efficiency_lap_dist_result = Result.Ok(efficiency_lap_dist)
        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            efficiency_5min_result = Result.Err(RuntimeError("Failed to process Efficiency5Minute!"))
            efficiency_1h_result = Result.Err(RuntimeError("Failed to process Efficiency1Hour!"))
            efficiency_lap_dist_result = Result.Err(RuntimeError("Failed to process EfficiencyLapDist!"))


        return efficiency_5min_result, efficiency_1h_result, efficiency_lap_dist_result

    def load(self, efficiency_5min_result, efficiency_1h_result, efficiency_lap_dist_result) -> tuple[FileLoader, FileLoader, FileLoader]:
        efficiency_5min_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="Efficiency5Minute",
            ),
            file_type=FileType.TimeSeries,
            data=efficiency_5min_result.unwrap() if efficiency_5min_result else None,
            description="" # TODO
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
            description=""  # TODO
        )

        efficiency_lap_dist_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="EfficiencyLapDist",
            ),
            file_type=FileType.TimeSeries,
            data=efficiency_lap_dist_result.unwrap() if efficiency_lap_dist_result else None,
            description=""  # TODO
        )

        efficiency_5min_loader = self.context.data_source.store(efficiency_5min_file)
        self.logger.info(f"Successfully loaded Efficiency1Hour!")

        efficiency_1h_loader = self.context.data_source.store(efficiency_1h_file)
        self.logger.info(f"Successfully loaded Efficiency5Minute!")

        efficiency_lap_dist_loader = self.context.data_source.store(efficiency_lap_dist_file)
        self.logger.info(f"Successfully loaded EfficiencyLapDist!")

        return efficiency_5min_loader, efficiency_1h_loader, efficiency_lap_dist_loader


stage_registry.register_stage(EfficiencyStage.get_stage_name(), EfficiencyStage)
