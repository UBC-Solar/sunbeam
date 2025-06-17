from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, File, FileType, CanonicalPath
from data_tools.collections import TimeSeries
from data_tools import Event
import copy
from prefect import task


class CleanupStage(Stage):
    @classmethod
    def get_stage_name(cls):
        return "cleanup"

    @staticmethod
    def dependencies():
        return ["ingress"]

    @staticmethod
    @task(name="Cleanup")
    def run(self, vehicle_velocity_loader: FileLoader, motor_rotating_speed_loader: FileLoader) -> tuple[FileLoader ,...]:
        """
        Run the cleanup stage to fix any inconsistencies with data on InfluxDB

        :param self: an instance of CleanupStage to be run
        :param FileLoader vehicle_velocity_loader: loader to VehicleVelocity from ingress
        :param FileLoader motor_rotating_speed_loader: loader to MotorRotatingSpeed from ingress
        :returns: speed_mps (FileLoader pointing to TimeSeries)
        """
        return super().run(self, vehicle_velocity_loader, motor_rotating_speed_loader)

    @property
    def event_name(self) -> str:
        return self._event.name

    @property
    def event(self) -> Event:
        """Get a copy of this stage's event"""
        return copy.deepcopy(self._event)

    def __init__(self, event: Event):
        """
        :param event: the event currently being processed
        """
        super().__init__()
        self._event = event

    def extract(self, vehicle_velocity_loader: FileLoader, motor_rotating_speed_loader: FileLoader) -> tuple[Result, Result]:
        vehicle_velocity_result: Result = vehicle_velocity_loader()
        motor_rotating_speed_result: Result = motor_rotating_speed_loader()

        return (vehicle_velocity_result, motor_rotating_speed_result)

    def transform(self, vehicle_velocity_result, motor_rotating_speed_result) -> tuple[Result]:

        if not vehicle_velocity_result and not motor_rotating_speed_result:
            speed_mps_result = Result.Err(RuntimeError("Failed to process SpeedMPS!"))
            return speed_mps_result
        elif vehicle_velocity_result:
            vehicle_velocity_ts: TimeSeries = vehicle_velocity_result.unwrap().data
            speed_mps_ts = vehicle_velocity_ts
        else: # motor_rotating_speed_result must be valid
            motor_rotating_speed_ts: TimeSeries = motor_rotating_speed_result.unwrap().data
            speed_mps_ts = motor_rotating_speed_ts / 3.6
        speed_mps_ts.units = "m/s"
        speed_mps_ts.name = "SpeedMPS"
        return Result.Ok(speed_mps_ts)

    def load(self, speed_mps_result) -> tuple[FileLoader]:
        speed_mps_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=CleanupStage.get_stage_name(),
                name="SpeedMPS",
            ),
            file_type=FileType.TimeSeries,
            data=speed_mps_result.unwrap() if speed_mps_result else None,
            description="Speed of the car in meters per second. Combines VehicleVelocity (legacy, m/s) with MotorRotatingSpeed (km/h)."
        )

        speed_mps_loader = self.context.data_source.store(speed_mps_file)
        self.logger.info(f"Successfully loaded SpeedMPS!")

        return speed_mps_loader,


stage_registry.register_stage(CleanupStage.get_stage_name(), CleanupStage)
