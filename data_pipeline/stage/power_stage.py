from data_tools.schema import FileLoader
from data_pipeline.stage.stage import Stage, StageResult
from data_pipeline.stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType
from data_pipeline.context import Context
from data_tools.collections import TimeSeries

import logging


class PowerStage(Stage):
    @classmethod
    def get_stage_name(cls):
        return "power"

    @staticmethod
    def dependencies():
        return ["ingest"]

    @property
    def event_name(self):
        return self._event_name

    def __init__(self, context: Context, logger: logging.Logger, event_name: str):
        """

        :param Context context:
        :param logging.Logger logger:
        :param str event_name: which event is currently being processed
        """
        super().__init__(context, logger)

        self._event_name = event_name

        self._total_pack_voltage_result = None
        self._pack_current_result = None
        self._pack_power = None

    def extract(self, total_pack_voltage_loader: FileLoader, pack_current_loader: FileLoader):
        self._total_pack_voltage_result: Result = total_pack_voltage_loader()
        self._pack_current_result: Result = pack_current_loader()

    def transform(self) -> None:
        try:
            total_pack_voltage: TimeSeries = self._total_pack_voltage_result.unwrap()
            pack_current: TimeSeries = self._pack_current_result.unwrap()

            total_pack_voltage, pack_current = TimeSeries.align(total_pack_voltage, pack_current)

            pack_power = total_pack_voltage.promote(total_pack_voltage * pack_current)
            pack_power.units = "W"
            pack_power.name = "Pack Power"

            self._pack_power = Result.Ok(pack_power)

        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            self._pack_power = Result.Err(RuntimeError("Failed to process pack power!"))

    def load(self) -> StageResult:
        pack_power_data = self._pack_power.unwrap() if self._pack_power else None
        pack_power_file = File(
            origin=self._context.title,
            path=[self.event_name, PowerStage.get_stage_name()],
            name="PackPower",
            file_type=FileType.TimeSeries,
            data=pack_power_data
        )

        pack_power_loader = self.context.data_source.store(pack_power_file)

        self.logger.info(f"Successfully loaded PackPower!")

        return pack_power_loader


stage_registry.register_stage(PowerStage.get_stage_name(), PowerStage)
