from data_tools import File
from data_tools.schema import DataSource
from data_tools.schema.data_source import FileLoader
from data_tools.query import DBClient
from data_pipeline.influx_credentials import InfluxCredentials, INFLUXDB_CREDENTIAL_BLOCK_NAME


class InfluxDBDataSource(DataSource):
    def __init__(self, *args, **kwargs):
        super().__init__()

        influxdb_credentials = InfluxDBDataSource._acquire_credentials()

        influxdb_token = influxdb_credentials.influxdb_api_token
        influxdb_org = influxdb_credentials.influxdb_org

        self._influxdb_client = DBClient(influxdb_token, influxdb_org)

    @staticmethod
    def _acquire_credentials() -> InfluxCredentials:
        influxdb_credentials: InfluxCredentials = InfluxCredentials.load(INFLUXDB_CREDENTIAL_BLOCK_NAME)

        return influxdb_credentials

    def store(self, **kwargs) -> FileLoader:
        raise NotImplementedError("`store` method is not implemented for InfluxDBDataSource "
                                  "as InfluxDB is read-only for Sunbeam!")

    def get(self, canonical_path: str, **kwargs) -> File:
        pass

