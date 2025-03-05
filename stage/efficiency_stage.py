from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from data_tools.collections import TimeSeries
from prefect import task
import numpy as np


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
        :returns: Efficiency5Minute, Efficiency1Hour (FileLoaders pointing to TimeSeries)
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

    def get_periodic_efficiency(self, vehicle_velocity: TimeSeries, motor_power: TimeSeries, period_seconds: float) -> TimeSeries:
        vehicle_velocity_aligned, motor_power_aligned = TimeSeries.align(
            vehicle_velocity, motor_power)

        # (seconds/window) / (seconds/index) == indices/window
        downsample_factor = int(period_seconds / vehicle_velocity_aligned.period)
        vehicle_velocity_averaged: np.ndarray = windowed_mean(
            np.array(vehicle_velocity_aligned), downsample_factor, allow_truncate=True)
        motor_power_averaged: np.ndarray = windowed_mean(
            np.array(motor_power_aligned), downsample_factor, allow_truncate=True)
        efficiency_array = motor_power_averaged / vehicle_velocity_averaged

        # clean bad values by setting them to zero
        bad_values_mask = self.get_anomaly_mask(motor_power_averaged, vehicle_velocity_averaged)
        efficiency_array[bad_values_mask] = 0
        efficiency = vehicle_velocity_aligned.promote(efficiency_array)

        efficiency.meta['period'] = period_seconds  # important: update the period for this TimeSeries
        efficiency.units = "J/m"
        return efficiency

    def get_anomaly_mask(self, motor_power_averaged: np.ndarray, vehicle_velocity_averaged: np.ndarray) -> np.ndarray:
        min_avg_meters_per_sec = 0
        max_avg_meters_per_sec = 50
        min_avg_watts = 0
        max_avg_watts = 10_000
        bad_values_mask: np.ndarray = ((vehicle_velocity_averaged > max_avg_meters_per_sec)
                                       | (vehicle_velocity_averaged < min_avg_meters_per_sec)
                                       | (motor_power_averaged < min_avg_watts)
                                       | (motor_power_averaged > max_avg_watts))
        self.logger.info(f"motor_power_averaged: "
                          f"min: {motor_power_averaged.min()} at index {motor_power_averaged.argmin()}"
                          f"max: {motor_power_averaged.max()} at index {motor_power_averaged.argmax()}")
        self.logger.info(f"vehicle_velocity_averaged: "
                          f"min: {vehicle_velocity_averaged.min()} at index {vehicle_velocity_averaged.argmin()}"
                          f"max: {vehicle_velocity_averaged.max()} at index {vehicle_velocity_averaged.argmax()}")
        return bad_values_mask

    def transform(self, vehicle_velocity_result, motor_power_result) -> tuple[Result, Result]:
        try:
            vehicle_velocity_ts: TimeSeries = vehicle_velocity_result.unwrap().data
            motor_power_ts: TimeSeries = motor_power_result.unwrap().data

            efficiency_5min = self.get_periodic_efficiency(vehicle_velocity_ts, motor_power_ts, 300)
            efficiency_5min.name = "Efficiency5Minute"
            efficiency_5min_result = Result.Ok(efficiency_5min)

            efficiency_1h = self.get_periodic_efficiency(vehicle_velocity_ts, motor_power_ts, 3600)
            efficiency_1h.name = "Efficiency1Hour"
            efficiency_1h_result = Result.Ok(efficiency_1h)
        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            efficiency_5min_result = Result.Err(RuntimeError("Failed to process Efficiency5Minute!"))
            efficiency_1h_result = Result.Err(RuntimeError("Failed to process Efficiency1Hour!"))

        return efficiency_5min_result, efficiency_1h_result

    def load(self, efficiency_5min_result, efficiency_1h_result) -> tuple[FileLoader, FileLoader]:
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

        efficiency_5min_loader = self.context.data_source.store(efficiency_5min_file)
        self.logger.info(f"Successfully loaded Efficiency1Hour!")

        efficiency_1h_loader = self.context.data_source.store(efficiency_1h_file)
        self.logger.info(f"Successfully loaded Efficiency5Minute!")

        return efficiency_5min_loader, efficiency_1h_loader


stage_registry.register_stage(EfficiencyStage.get_stage_name(), EfficiencyStage)
