from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath, Event
from data_tools.collections import TimeSeries
from prefect import task
import numpy as np

MIN_AVG_METERS_PER_SEC = 2
MAX_AVG_METERS_PER_SEC = 50
MIN_AVG_WATTS = 0
MAX_AVG_WATTS = 10_000

NCM_LAP_LEN_M = 5040.


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
    def run(self,
            speed_mps_loader: FileLoader,
            motor_power_loader: FileLoader,
            lap_index_loader: FileLoader) -> tuple[FileLoader, ...]:
        """
        Run the efficiency stage, which computes motor energy used per unit distance. Values are computed over various
        time slices.

        :param self: an instance of EfficiencyStage to be run
        :param FileLoader speed_mps_loader: loader to SpeedMPS from CleanupStage
        :param FileLoader motor_power_loader: loader to Motor Power from PowerStage
        :param FileLoader lap_index_loader: loader to the best available lap index data from LocalizationStage
        :returns: Efficiency5Minute, Efficiency1Hour, EfficiencyLapDistance (FileLoaders pointing to TimeSeries)
        """
        return super().run(self, speed_mps_loader, motor_power_loader, lap_index_loader)

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
            speed_mps_loader: FileLoader,
            motor_power_loader: FileLoader,
            lap_index_loader: FileLoader
    ) -> tuple[Result, Result, Result]:
        speed_mps_result: Result = speed_mps_loader()
        motor_power_result: Result = motor_power_loader()
        lap_index_result: Result = lap_index_loader()

        return speed_mps_result, motor_power_result, lap_index_result

    @staticmethod
    def get_periodic_efficiency(
            speed_mps_aligned: TimeSeries,
            motor_power_aligned: TimeSeries,
            period_seconds: float
    ) -> TimeSeries:
        # (seconds/window) / (seconds/index) == indices/window
        downsample_factor = int(period_seconds / speed_mps_aligned.period)
        speed_mps_averaged: np.ndarray = windowed_mean(
            np.array(speed_mps_aligned),
            downsample_factor,
            allow_truncate=True
        )

        motor_power_averaged: np.ndarray = windowed_mean(
            np.array(motor_power_aligned),
            downsample_factor,
            allow_truncate=True
        )

        efficiency_array = motor_power_averaged / speed_mps_averaged  # (J/s) / (m/s) = J/m

        # clean bad values by setting them to zero
        bad_values_mask = EfficiencyStage.get_anomaly_mask(motor_power_averaged, speed_mps_averaged)
        efficiency_array[bad_values_mask] = np.nan
        efficiency = speed_mps_aligned.promote(efficiency_array)

        efficiency.meta['period'] = period_seconds  # important: update the period for this TimeSeries
        efficiency.units = "J/m"
        return efficiency

    @staticmethod
    def get_anomaly_mask(motor_power_averaged: np.ndarray, speed_mps_averaged: np.ndarray) -> np.ndarray:
        bad_values_mask: np.ndarray = ((speed_mps_averaged > MAX_AVG_METERS_PER_SEC)
                                       | (speed_mps_averaged < MIN_AVG_METERS_PER_SEC)
                                       | (motor_power_averaged < MIN_AVG_WATTS)
                                       | (motor_power_averaged > MAX_AVG_WATTS))
        return bad_values_mask

    @staticmethod
    def get_lap_dist_efficiency(speed_mps_aligned, motor_power_aligned, lap_index_aligned) -> np.ndarray:
        """Produces an array which represents efficiency indexed by lap.

        For example, the efficiency of the third lap is given by efficiency_lap_distance[2].
        The lap index is approximate, as it is obtained by tiling arrays over the distance of the track.

        :param TimeSeries speed_mps_aligned: SpeedMPS, aligned with motor_power_aligned
        :param TimeSeries motor_power_aligned: MotorPower, aligned with speed_mps_aligned
        :param TimeSeries lap_index_aligned: Index of the lap as a function of time.
        :return: efficiency_lap_distance
        """
        lap_index = np.array(lap_index_aligned, dtype=int)
        vv_aligned_arr = np.array(speed_mps_aligned)
        mp_aligned_arr = np.array(motor_power_aligned)
        efficiency_lap_distance = np.zeros(int(np.max(lap_index) + 1))

        # iterate over the time indices, and update efficiency_lap_distance every time we roll into a new lap
        sum_power = 0
        sum_speed = 0
        num_vals = 0
        prev_lap_idx = 0
        for array_index, lap_idx in enumerate(lap_index):
            if lap_idx > prev_lap_idx:
                # start of a new lap

                # determine avg power and speed over the last lap
                avg_power = sum_power / num_vals
                avg_speed = sum_speed / num_vals

                # set value to:
                #     np.nan if the speed or power are outside the acceptable range
                #     otherwise the efficiency for the last lap
                if ((avg_speed > MAX_AVG_METERS_PER_SEC) | (avg_speed < MIN_AVG_METERS_PER_SEC)
                        | (avg_power < MIN_AVG_WATTS) | (avg_power > MAX_AVG_WATTS)):
                    efficiency_lap_distance[lap_idx] = np.nan  # invalid data
                else:
                    efficiency_lap_distance[lap_idx] = avg_power / avg_speed

                # reset the accumulating variables
                sum_power = 0
                sum_speed = 0
                num_vals = 0
                prev_lap_idx = lap_idx

            # accumulate values so that they can be averaged later
            sum_power += mp_aligned_arr[array_index]
            sum_speed += vv_aligned_arr[array_index]
            num_vals += 1

        return np.array(efficiency_lap_distance)

    def transform(self, speed_mps_result, motor_power_result, lap_index_result) -> tuple[Result, Result, Result]:
        try:
            speed_mps_ts: TimeSeries = speed_mps_result.unwrap().data
            motor_power_ts: TimeSeries = motor_power_result.unwrap().data
            speed_mps_aligned, motor_power_aligned = TimeSeries.align(
                speed_mps_ts, motor_power_ts)

            efficiency_5min: TimeSeries = self.get_periodic_efficiency(
                speed_mps_aligned,
                motor_power_aligned,
                300
            )

            efficiency_5min.name = "Efficiency5Minute"
            efficiency_5min_result = Result.Ok(efficiency_5min)

            efficiency_1h: TimeSeries = self.get_periodic_efficiency(
                speed_mps_aligned,
                motor_power_aligned,
                3600
            )

            efficiency_1h.name = "Efficiency1Hour"
            efficiency_1h_result = Result.Ok(efficiency_1h)

            try:
                lap_index = lap_index_result.unwrap().data
                lap_index_aligned, speed_mps_aligned, motor_power_aligned = TimeSeries.align(
                    lap_index, speed_mps_ts, motor_power_ts)

                efficiency_lap_distance: np.ndarray = self.get_lap_dist_efficiency(
                    speed_mps_aligned,
                    motor_power_aligned,
                    lap_index_aligned
                )
                efficiency_lap_distance_result = Result.Ok(efficiency_lap_distance)
            except UnwrappedError as e:
                self.logger.error(f"Failed to unwrap result! \n {e}")
                efficiency_lap_distance_result = Result.Err(RuntimeError("Failed to process EfficiencyLapDistance!"))

        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            efficiency_5min_result = Result.Err(RuntimeError("Failed to process Efficiency5Minute!"))
            efficiency_1h_result = Result.Err(RuntimeError("Failed to process Efficiency1Hour!"))
            efficiency_lap_distance_result = Result.Err(RuntimeError("Failed to process EfficiencyLapDistance!"))

        return efficiency_5min_result, efficiency_1h_result, efficiency_lap_distance_result

    def load(self,
             efficiency_5min_result,
             efficiency_1h_result,
             efficiency_lap_distance_result
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
            description="Driving efficiency in J/m, computed as avg_motor_power / avg_speed_mps with values "
                        "averaged over 5-minute periods. Values are np.nan where mean speed is outside "
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
            description="Driving efficiency in J/m, computed as avg_motor_power / avg_speed_mps with values "
                        "averaged over 1-hour periods. Values are np.nan where mean speed is outside "
                        "the range [2, 50] m/s or if mean power is outside the range [0, 10] kW."
        )

        efficiency_lap_distance_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="EfficiencyLapDistance",
            ),
            file_type=FileType.TimeSeries,  # actually an ndarray but this is not yet supported
            data=efficiency_lap_distance_result.unwrap() if efficiency_lap_distance_result else None,
            description="Driving efficiency in J/m, computed as avg_motor_power / avg_speed_mps with values "
                        "averaged over each lap. The lap splitting calculation starts by integrating SpeedMPS to"
                        " get total distance as a function over time. Then, the values are split into lengths of 5.04km"
                        " as this is the length of the FSGP track. Then, avg_motor_power & avg_speed_mps are "
                        " taken over the timespan for each given lap. Since there is some error in SpeedMPS and "
                        "not all distance travelled is along the track, this should not be relied upon to exactly align"
                        " with real lap times. However, it is a decent estimate: it predicts 48 laps for FSGP day 1 "
                        "where the real number was 46. Values are np.nan where mean speed is outside "
                        "the range [2, 50] m/s or if mean power is outside the range [0, 10] kW."
        )

        efficiency_5min_loader = self.context.data_source.store(efficiency_5min_file)
        self.logger.info(f"Successfully loaded Efficiency1Hour!")

        efficiency_1h_loader = self.context.data_source.store(efficiency_1h_file)
        self.logger.info(f"Successfully loaded Efficiency5Minute!")

        efficiency_lap_distance_loader = self.context.data_source.store(efficiency_lap_distance_file)
        self.logger.info(f"Successfully loaded EfficiencyLapDist!")

        return efficiency_5min_loader, efficiency_1h_loader, efficiency_lap_distance_loader

    def skip_stage(self):
        efficiency_5min_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="Efficiency5Minute",
            ),
            file_type=FileType.TimeSeries,
            data=None,
        )

        efficiency_1h_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="Efficiency1Hour",
            ),
            file_type=FileType.TimeSeries,
            data=None,
        )

        efficiency_lap_distance_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="EfficiencyLapDistance",
            ),
            file_type=FileType.TimeSeries,
            data=None,
        )

        efficiency_5min_loader = self.context.data_source.store(efficiency_5min_file)
        efficiency_1h_loader = self.context.data_source.store(efficiency_1h_file)
        efficiency_lap_distance_loader = self.context.data_source.store(efficiency_lap_distance_file)

        return efficiency_5min_loader, efficiency_1h_loader, efficiency_lap_distance_loader


stage_registry.register_stage(EfficiencyStage.get_stage_name(), EfficiencyStage)
