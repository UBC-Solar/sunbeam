from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from data_tools.collections import TimeSeries
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
        Run the localization stage, which computes various metrics relating to the car's location over time.

        :param self: an instance of LocalizationStage to be run
        :param FileLoader vehicle_velocity_loader: loader to VehicleVelocity from Ingress
        :returns: LapIndexIntegratedSpeed (FileLoader pointing to TimeSeries)
        """
        return super().run(self, vehicle_velocity_loader)

    @property
    def event_name(self):
        return self._event_name

    def __init__(self, event_name: str):
        """
        :param str event_name: which event is currently being processed
        """
        super().__init__()

        self._event_name = event_name

    def extract(self, vehicle_velocity_loader: FileLoader) -> tuple[Result]:
        vehicle_velocity_result: Result = vehicle_velocity_loader()
        return (vehicle_velocity_result,)

    def transform(self, vehicle_velocity_result) -> tuple[Result]:
        try:
            vehicle_velocity_ts: TimeSeries = vehicle_velocity_result.unwrap().data
            integrated_velocity_m = np.cumsum(vehicle_velocity_ts) * vehicle_velocity_ts.period
            lap_index_integrated_speed = vehicle_velocity_ts.promote(
                np.array([int(dist_m // NCM_LAP_LEN_M) for dist_m in integrated_velocity_m]))
            lap_index_integrated_speed.name = "LapIndexIntegratedSpeed"
            lap_index_integrated_speed.units = "Laps"
            lap_index_integrated_speed_result = Result.Ok(lap_index_integrated_speed)

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
