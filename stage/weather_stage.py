from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from data_tools.collections import TimeSeries
from data_tools import Event
from prefect import task
import numpy as np
import copy


class WeatherStage(Stage):
    @classmethod
    def get_stage_name(cls):
        return "weather"

    @staticmethod
    def dependencies():
        return []

    @staticmethod
    @task(name="Weather")
    def run(self) -> tuple[FileLoader, ...]:
        """
        Run the weather stage... TODO

        :param self: an instance of WeatherStage to be run
        :returns: TODO
        """
        return super().run(self)

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

    def extract(self) -> None:
        return

    def transform(self) -> tuple[Result, ...]:
        return

    def load(self) -> tuple[FileLoader, FileLoader, FileLoader]:
        return


stage_registry.register_stage(WeatherStage.get_stage_name(), WeatherStage)
