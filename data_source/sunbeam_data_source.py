from data_tools.schema import DataSource, FileLoader, Result, CanonicalPath
from data_tools.query import SunbeamClient
from config import SunbeamSourceConfig


class SunbeamDataSource(DataSource):
    def __init__(self, config: SunbeamSourceConfig, *args, **kwargs):
        super().__init__()

        self._sunbeam_client = SunbeamClient(api_url=config.api_url)

    def store(self, **kwargs) -> FileLoader:
        raise NotImplementedError("`store` method is not allowed for SunbeamDataSource!")

    def get(self, canonical_path: CanonicalPath, start: str = None, stop: str = None) -> Result:
        try:
            return Result.Ok(self._sunbeam_client.get_file(path=canonical_path))

        except Exception as e:
            return Result.Err(e)
