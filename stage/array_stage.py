from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath, Event
from data_tools.collections import TimeSeries
from prefect import task


class ArrayStage(Stage):
    @classmethod
    def get_stage_name(cls):
        return "array"

    @staticmethod
    def dependencies():
        return ["ingress"]

    @staticmethod
    @task(name="Array")
    def run(self,
            output_voltage_a_loader: FileLoader,
            output_voltage_b_loader: FileLoader,
            output_voltage_c_loader: FileLoader,
            output_current_a_loader: FileLoader,
            output_current_b_loader: FileLoader,
            output_current_c_loader: FileLoader
            ) -> tuple[FileLoader, ...]:
        """
        Run the array stage, converting voltage and current data into power.

        :param self: An instance of ArrayStage to be run
        :returns: ArrayPower (FileLoader pointing to TimeSeries)
        """
        return super().run(
            self,
            output_voltage_a_loader,
            output_voltage_b_loader,
            output_voltage_c_loader,
            output_current_a_loader,
            output_current_b_loader,
            output_current_c_loader
        )

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
            output_voltage_a_loader: FileLoader,
            output_voltage_b_loader: FileLoader,
            output_voltage_c_loader: FileLoader,
            output_current_a_loader: FileLoader,
            output_current_b_loader: FileLoader,
            output_current_c_loader: FileLoader
    ) -> tuple[Result, Result, Result, Result, Result, Result]:
        output_voltage_a_result: Result = output_voltage_a_loader()
        output_voltage_b_result: Result = output_voltage_b_loader()
        output_voltage_c_result: Result = output_voltage_c_loader()
        output_current_a_result: Result = output_current_a_loader()
        output_current_b_result: Result = output_current_b_loader()
        output_current_c_result: Result = output_current_c_loader()

        return (output_voltage_a_result, output_voltage_b_result,
                output_voltage_c_result, output_current_a_result,
                output_current_b_result, output_current_c_result
                )

    def transform(self,
                  output_voltage_a_result: Result,
                  output_voltage_b_result: Result,
                  output_voltage_c_result: Result,
                  output_current_a_result: Result,
                  output_current_b_result: Result,
                  output_current_c_result: Result
                  ) -> tuple[Result]:
        try:

            output_voltage_a = output_voltage_a_result.unwrap().data
            output_voltage_b = output_voltage_b_result.unwrap().data
            output_voltage_c = output_voltage_c_result.unwrap().data
            output_current_a = output_current_a_result.unwrap().data
            output_current_b = output_current_b_result.unwrap().data
            output_current_c = output_current_c_result.unwrap().data

            self.logger.error(output_voltage_a._start.tzinfo)

            output_voltage_a, output_current_a = TimeSeries.align(output_voltage_a, output_current_a)
            array_power_a = output_voltage_a * output_current_a

            self.logger.error(output_voltage_a._start.tzinfo)

            output_voltage_b, output_current_b = TimeSeries.align(output_voltage_b, output_current_b)
            array_power_b = output_voltage_b * output_current_b

            output_voltage_c, output_current_c = TimeSeries.align(output_voltage_c, output_current_c)
            array_power_c = output_voltage_c * output_current_c

            array_power_a, array_power_b, array_power_c = TimeSeries.align(array_power_a, array_power_b, array_power_c)

            array_power = array_power_a + array_power_b + array_power_c

            array_power.units = "W"
            array_power.name = "Array Power"

            self.logger.error(array_power._start.tzinfo)

            motor_power = Result.Ok(array_power)

        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            motor_power = Result.Err(RuntimeError("Failed to process array power!"))

        return (motor_power, )

    def load(self, array_power) -> tuple[FileLoader]:
        array_power_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=ArrayStage.get_stage_name(),
                name="ArrayPower",
            ),
            file_type=FileType.TimeSeries,
            data=array_power.unwrap() if array_power else None
        )

        array_power_loader = self.context.data_source.store(array_power_file)
        self.logger.info(f"Successfully loaded ArrayPower!")

        return (array_power_loader, )

    def skip_stage(self):
        self.logger.error(f"{self.get_stage_name()} is being skipped!")

        array_power_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=ArrayStage.get_stage_name(),
                name="ArrayPower",
            ),
            file_type=FileType.TimeSeries,
            data=None
        )

        array_power_loader = self.context.data_source.store(array_power_file)

        return array_power_loader


stage_registry.register_stage(ArrayStage.get_stage_name(), ArrayStage)
