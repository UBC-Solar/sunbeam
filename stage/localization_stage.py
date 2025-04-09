from asyncpg.protocol.coreproto import RESULT_OK
from data_tools.schema import FileLoader
from data_tools import Event
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from data_tools.collections import TimeSeries
from data_tools.lap_tools import FSGPDayLaps
from prefect import task
import numpy as np


NCM_LAP_LEN_M = 5040.


class LocalizationStage(Stage):
    @classmethod
    def get_stage_name(cls):
        return "localization"

    @staticmethod
    def dependencies():
        return ["ingress"]

    @staticmethod
    @task(name="Localization")
    def run(self, vehicle_velocity_loader: FileLoader) -> tuple[FileLoader, ...]:
        """
        Run the localization stage, which computes various metrics relating to the car's location in a track race.

        Outputs will be FileLoaders pointing to None for non-track events, or if the prerequisite data is unavailable.
        1. LapIndexIntegratedSpeed
            Do not use this unless it is the only option. Integrates speed and tiles by track length to approximate
            the lap index we are on at any given time. Lap index is the integer number of laps we have completed
            around the track (starting at zero).
        2. LapIndexSpreadsheet
            Uses data from the FSGP timing spreadsheet (via FSGPDayLaps) to determine lap index.
            Lap index is the integer number of laps we have completed around the track
            at any given time (starting at zero).
        3. TrackIndexSpreadsheet
            Integrates speed within a lap to determine the car's coordinate within the track. We use a list of
            lat/lon pairs to represent a track's indices, and round to the nearest one. References the FSGP timing
            spreadsheet (via FSGPDayLaps) for lap start/stop times.

        :param self: an instance of LocalizationStage to be run
        :param FileLoader vehicle_velocity_loader: loader to VehicleVelocity from Ingress
        :returns: LapIndexIntegratedSpeed, LapIndexSpreadsheet, TrackIndexSpreadsheet (FileLoaders pointing to TimeSeries)
        """
        return super().run(self, vehicle_velocity_loader)

    @property
    def event_name(self):
        return self._event_name

    def __init__(self, event: Event):
        """
        :param event: the event currently being processed
        """
        super().__init__()

        self._event = event
        self._event_name = event.name

    def extract(self, vehicle_velocity_loader: FileLoader) -> tuple[Result]:
        vehicle_velocity_result: Result = vehicle_velocity_loader()
        return (vehicle_velocity_result,)

    @staticmethod
    def _get_lap_index_integrated_speed(vehicle_velocity_ts: TimeSeries) -> Result[TimeSeries]:
        integrated_velocity_m = np.cumsum(vehicle_velocity_ts) * vehicle_velocity_ts.period
        lap_index_integrated_speed = vehicle_velocity_ts.promote(
            np.array([int(dist_m // NCM_LAP_LEN_M) for dist_m in integrated_velocity_m]))
        lap_index_integrated_speed.name = "LapIndexIntegratedSpeed"
        lap_index_integrated_speed.units = "Laps"
        return Result.Ok(lap_index_integrated_speed)

    def transform(self, vehicle_velocity_result) -> tuple[Result]:

        if "ncm_motorsports_park" not in self._event.flags:
            lap_index_integrated_speed_result = Result.Ok(None)
        if not np.all([
            flag in self._event.flags for flag in ["ncm_motorsports_park", "has_spreadsheet"]
        ]):


        try:
            vehicle_velocity_ts: TimeSeries = vehicle_velocity_result.unwrap().data
            lap_index_integrated_speed_result = self._get_lap_index_integrated_speed(vehicle_velocity_ts)
        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            lap_index_integrated_speed_result = Result.Err(RuntimeError("Failed to process LapIndexIntegratedSpeed!"))

        return (lap_index_integrated_speed_result,)

    def load(self, lap_index_integrated_speed_result) -> tuple[FileLoader]:

        lap_index_integrated_speed_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="LapIndexIntegratedSpeed",
            ),
            file_type=FileType.TimeSeries,
            data=lap_index_integrated_speed_result.unwrap() if lap_index_integrated_speed_result else None,
            description="Estimate of the FSGP lap index in this event as a function of time. "
                        "Value is estimated by integrating VehicleVelocity and tiling the result over the FSGP lap "
                        f"length of {NCM_LAP_LEN_M} meters."
        )


        lap_index_integrated_speed_loader = self.context.data_source.store(lap_index_integrated_speed_file)
        self.logger.info(f"Successfully loaded LapIndexIntegratedSpeed!")

        return (lap_index_integrated_speed_loader,)


stage_registry.register_stage(LocalizationStage.get_stage_name(), LocalizationStage)
