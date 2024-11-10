from data_tools import File
from data_tools.schema import DataSource
from data_tools.schema.data_source import FileLoader


class FSDataSource(DataSource):
    def store(self, **kwargs) -> FileLoader:
        pass

    def get(self, canonical_path: str, **kwargs) -> File:
        pass
