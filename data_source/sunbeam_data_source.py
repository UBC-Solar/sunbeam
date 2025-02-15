from data_tools.schema import DataSource, FileLoader, Result, CanonicalPath
from data_tools.query import SunbeamClient
from config import SunbeamSourceConfig


class SunbeamDataSource(DataSource):
    def __init__(self, config: SunbeamSourceConfig, *args, **kwargs):
        super().__init__()

        self._sunbeam_client = SunbeamClient(api_url=config.api_url)
        self._origin = config.ingress_origin

    def store(self, **kwargs) -> FileLoader:
        raise NotImplementedError("`store` method is not allowed for SunbeamDataSource!")

    def get(self, canonical_path: CanonicalPath, start: str = None, stop: str = None) -> Result:
        try:
            _, event, source, name = canonical_path.unwrap()
            return Result.Ok(self._sunbeam_client.get_file(
                origin=self._origin,
                event=event,
                source=source,
                name=name
               ).unwrap()
            )

        except Exception as e:
            return Result.Err(e)
