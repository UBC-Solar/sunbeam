from data_tools.schema import FileLoader
from data_pipeline.stage.stage import Stage, StageResult
from data_pipeline.stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from data_pipeline.context import Context
from data_tools.collections import TimeSeries


class PowerStage(Stage):
    def run(self, total_pack_voltage_loader: FileLoader, pack_current_loader: FileLoader, motor_current_loader: FileLoader, motor_voltage_loader: FileLoader) -> StageResult:
        """
        Run the power stage, converting voltage and current data into power.
        :param FileLoader total_pack_voltage_loader: loader to TotalPackVoltage from Ingest
        :param FileLoader pack_current_loader: loader to PackCurrent from Ingest
        :param FileLoader motor_current_loader: loader to MotorCurrent from Ingest
        :param FileLoader motor_voltage_loader: loader to MotorVoltage from Ingest
        :returns: PackPower (TimeSeries), MotorPower (TimeSeries)
        """
        return super().run(total_pack_voltage_loader, pack_current_loader, motor_current_loader, motor_voltage_loader)

    @classmethod
    def get_stage_name(cls):
        return "power"

    @staticmethod
    def dependencies():
        return ["ingress"]

    @property
    def event_name(self):
        return self._event_name

    def __init__(self, context: Context, event_name: str):
        """
        :param Context context:
        :param str event_name: which event is currently being processed
        """
        super().__init__(context)

        self._event_name = event_name

        self.declare_output("pack_power")
        self.declare_output("motor_power")

    def extract(self, total_pack_voltage_loader: FileLoader, pack_current_loader: FileLoader, motor_current_loader: FileLoader, motor_voltage_loader: FileLoader) -> tuple[Result, Result, Result, Result]:
        total_pack_voltage_result: Result = total_pack_voltage_loader()
        pack_current_result: Result = pack_current_loader()
        motor_current_result: Result = motor_current_loader()
        motor_voltage_result: Result = motor_voltage_loader()

        return total_pack_voltage_result, pack_current_result, motor_current_result, motor_voltage_result

    def transform(self, total_pack_voltage_result, pack_current_result, motor_current_result, motor_voltage_result) -> tuple[Result, Result]:
        try:
            motor_current: TimeSeries = motor_current_result.unwrap()
            motor_voltage: TimeSeries = motor_voltage_result.unwrap()

            motor_current, motor_voltage = TimeSeries.align(motor_current, motor_voltage)
            motor_power = motor_current.promote(motor_current * motor_voltage)
            motor_power.units = "W"
            motor_power.name = "Motor Power"

            motor_power = Result.Ok(motor_power)

        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            motor_power = Result.Err(RuntimeError("Failed to process motor power!"))

        try:
            total_pack_voltage: TimeSeries = total_pack_voltage_result.unwrap()
            pack_current: TimeSeries = pack_current_result.unwrap()

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
