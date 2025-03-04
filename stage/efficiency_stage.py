from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from data_tools.collections import TimeSeries
from prefect import task
import numpy as np


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
        :returns: InstantaneousEfficiency, IntegratedEfficiency (FileLoaders pointing to TimeSeries)
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

    def transform(self, vehicle_velocity_result, motor_power_result) -> tuple[Result, Result]:
        try:
            vehicle_velocity_ts: TimeSeries = vehicle_velocity_result.unwrap().data
            motor_power_ts: TimeSeries = motor_power_result.unwrap().data
            vehicle_velocity_aligned, motor_power_aligned = TimeSeries.align(
                vehicle_velocity_ts, motor_power_ts)
            instantaneous_efficiency_ts = vehicle_velocity_aligned.promote(vehicle_velocity_aligned / motor_power_aligned)
            instantaneous_efficiency_ts.units = "J/m"
            instantaneous_efficiency_ts.name = "Instantaneous Efficiency"
            instantaneous_efficiency_result = Result.Ok(instantaneous_efficiency_ts)
        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            instantaneous_efficiency_result = Result.Err(RuntimeError("Failed to process instantaneous efficiency!"))

        try:
            instantaneous_efficiency_ts: TimeSeries = instantaneous_efficiency_result.unwrap()
            integrated_efficiency_ts: TimeSeries = instantaneous_efficiency_ts.promote(
                np.cumsum(instantaneous_efficiency_ts)
            )
            integrated_efficiency_ts.units = "J/m"
            integrated_efficiency_ts.name = "Integrated Efficiency"
            integrated_efficiency_result = Result.Ok(integrated_efficiency_ts)
        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            integrated_efficiency_result = Result.Err(RuntimeError("Failed to process integrated efficiency!"))

        return instantaneous_efficiency_result, integrated_efficiency_result

    def load(self, instantaneous_efficiency_result, integrated_efficiency_result) -> tuple[FileLoader, FileLoader]:
        instantaneous_efficiency_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="InstantaneousEfficiency",
            ),
            file_type=FileType.TimeSeries,
            data=instantaneous_efficiency_result.unwrap() if instantaneous_efficiency_result else None,
            description="" # TODO
        )

        integrated_efficiency_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="IntegratedEfficiency",
            ),
            file_type=FileType.TimeSeries,
            data=integrated_efficiency_result.unwrap() if integrated_efficiency_result else None,
            description=""  # TODO
        )

        instantaneous_efficiency_loader = self.context.data_source.store(instantaneous_efficiency_file)
        self.logger.info(f"Successfully loaded InstantaneousEfficiency!")

        integrated_efficiency_loader = self.context.data_source.store(integrated_efficiency_file)
        self.logger.info(f"Successfully loaded IntegratedEfficiency!")

        return instantaneous_efficiency_loader, integrated_efficiency_loader


stage_registry.register_stage(EfficiencyStage.get_stage_name(), EfficiencyStage)
