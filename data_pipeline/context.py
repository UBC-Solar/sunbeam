from data_tools.schema import DataSource
from typing import List


class Context:
    def __init__(self, title: str, data_source: DataSource, stages_to_run: List[str]):
        self._title = title
        self._data_source = data_source
        self._stages_to_run = stages_to_run

    @property
    def title(self) -> str:
        return self._title

    @property
    def data_source(self) -> DataSource:
        return self._data_source

    @property
    def stages_to_run(self) -> List[str]:
        return self._stages_to_run
