from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from data_tools.collections import TimeSeries
from prefect import task


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
        :returns: InstantaneousEfficiency (FileLoader pointing to TimeSeries)
        """
        return super().run(
            self,
            total_pack_voltage_loader,
            pack_current_loader,
            motor_voltage_loader,
            motor_current_loader,
            motor_current_direction_loader
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

    def extract(self,
            total_pack_voltage_loader: FileLoader,
            pack_current_loader: FileLoader,
            motor_voltage_loader: FileLoader,
            motor_current_loader: FileLoader,
            motor_current_direction_loader: FileLoader) -> tuple[Result, Result, Result, Result, Result]:
        total_pack_voltage_result: Result = total_pack_voltage_loader()
        pack_current_result: Result = pack_current_loader()
        motor_voltage_result: Result = motor_voltage_loader()
        motor_current_result: Result = motor_current_loader()
        motor_current_direction_result: Result = motor_current_direction_loader()

        return (total_pack_voltage_result, pack_current_result,
                motor_voltage_result, motor_current_result, motor_current_direction_result)

    def transform(self,
                  total_pack_voltage_result,
                  pack_current_result,
                  motor_voltage_result,
                  motor_current_result,
                  motor_current_direction_result) -> tuple[Result, Result]:
        try:
            motor_voltage: TimeSeries = motor_voltage_result.unwrap().data
            motor_current: TimeSeries = motor_current_result.unwrap().data
            motor_current_direction: TimeSeries = motor_current_direction_result.unwrap().data

            # motor_current_direction is 1 if negative (regen) and 0 if positive (driving)
            # the linear function -2x + 1 maps 1 to -1 and 0 to 1,
            # resulting in a number that represents the sign/direction of the current
            motor_current_sign = motor_current_direction * -2 + 1

            motor_current, motor_voltage, motor_current_sign = TimeSeries.align(
                motor_current, motor_voltage, motor_current_sign
            )
            motor_power = motor_current.promote(motor_current * motor_voltage * motor_current_sign)
            motor_power.units = "W"
            motor_power.name = "Motor Power"

            motor_power = Result.Ok(motor_power)

        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            motor_power = Result.Err(RuntimeError("Failed to process motor power!"))

        try:
            total_pack_voltage: TimeSeries = total_pack_voltage_result.unwrap().data
            pack_current: TimeSeries = pack_current_result.unwrap().data

            total_pack_voltage, pack_current = TimeSeries.align(total_pack_voltage, pack_current)
            pack_power = total_pack_voltage.promote(total_pack_voltage * pack_current)
            pack_power.units = "W"
            pack_power.name = "Pack Power"

            pack_power = Result.Ok(pack_power)

        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            pack_power = Result.Err(RuntimeError("Failed to process pack power!"))

        return pack_power, motor_power

    def load(self, pack_power, motor_power) -> tuple[FileLoader, FileLoader]:
        pack_power_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=PowerStage.get_stage_name(),
                name="PackPower",
            ),
            file_type=FileType.TimeSeries,
            data=pack_power.unwrap() if pack_power else None
        )

        motor_power_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=PowerStage.get_stage_name(),
                name="MotorPower",
            ),
            file_type=FileType.TimeSeries,
            data=motor_power.unwrap() if motor_power else None
        )

        pack_power_loader = self.context.data_source.store(pack_power_file)
        self.logger.info(f"Successfully loaded PackPower!")

        motor_power_loader = self.context.data_source.store(motor_power_file)
        self.logger.info(f"Successfully loaded MotorPower!")

        return pack_power_loader, motor_power_loader


stage_registry.register_stage(PowerStage.get_stage_name(), PowerStage)
