from data_tools.schema import DataSource


class Context:
    def __init__(self, title: str, data_source: DataSource):
        self._title = title
        self._data_source = data_source

    @property
    def title(self) -> str:
        return self._title

    @property
    def data_source(self) -> DataSource:
        return self._data_source
