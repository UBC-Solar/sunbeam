from data_tools.schema import DataSource


class Overseer:
    def __init__(self, title: str, data_source: DataSource):
        self._title = title
        self._data_source = data_source

    @property
    def title(self):
        return self._title

    @property
    def data_source(self):
        return self._data_source
