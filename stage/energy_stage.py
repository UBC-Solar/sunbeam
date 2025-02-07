from data_tools.schema import FileLoader
from stage.stage import Stage, StageResult
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from stage.context import Context
from data_tools.collections import TimeSeries
from prefect import task
import numpy as np


class EnergyStage(Stage):
    @classmethod
    def get_stage_name(cls):
        return "energy"

    @staticmethod
    def dependencies():
        return ["power"]

    @staticmethod
    @task(name="Energy")
    def run(self, pack_power_loader: FileLoader) -> StageResult:
        """
        Run the power stage, converting voltage and current data into power.

        :param self: an instance of PowerStage to be run
        :param FileLoader pack_power_loader: loader to PackPower from Power
        :returns: PackEnergy (TimeSeries)
        """
        return super().run(self, pack_power_loader)

    @property
    def event_name(self):
        return self._event_name

    def __init__(self, event_name: str):
        """
        :param str event_name: which event is currently being processed
        """
        super().__init__()

        self._event_name = event_name

        self.declare_output("pack_energy")

    def extract(self, pack_power: FileLoader) -> tuple[Result]:
        pack_power: Result = pack_power()

        return pack_power,

    def transform(self, pack_power_result) -> tuple[Result]:
        try:
            pack_power: TimeSeries = pack_power_result.unwrap().data

            pack_energy = np.cumsum(pack_power)
            pack_energy = pack_power.promote(pack_energy)
            pack_energy.units = "J"
            pack_energy.name = "Pack Energy"

            pack_energy_result = Result.Ok(pack_energy)

        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            pack_energy_result = Result.Err(RuntimeError("Failed to process motor power!"))

        return pack_energy_result,

    def load(self, pack_energy) -> tuple[FileLoader]:
        pack_energy_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=EnergyStage.get_stage_name(),
                name="PackEnergy",
            ),
            file_type=FileType.TimeSeries,
            data=pack_energy.unwrap() if pack_energy else None
        )

        pack_energy_loader = self.context.data_source.store(pack_energy_file)
        self.logger.info(f"Successfully loaded PackEnergy!")

        return pack_energy_loader,


stage_registry.register_stage(EnergyStage.get_stage_name(), EnergyStage)
