from data_tools.schema import File, Result
from data_tools.schema import DataSource
from data_tools.schema.data_source import FileLoader
from data_tools.query import DBClient
from data_tools.utils import parse_iso_datetime
from data_pipeline.influx_credentials import InfluxCredentials, INFLUXDB_CREDENTIAL_BLOCK_NAME
from datetime import datetime
import os


class InfluxDBDataSource(DataSource):
    def __init__(self, start: datetime, stop: datetime, *args, **kwargs):
        super().__init__()

        self._start = start
        self._stop = stop

        # influxdb_credentials = InfluxDBDataSource._acquire_credentials()
        #
        # influxdb_token = influxdb_credentials.influxdb_api_token
        # influxdb_org = influxdb_credentials.influxdb_org
        influxdb_token = os.getenv("INFLUX_TOKEN")
        influxdb_org = os.getenv("INFLUX_ORG")

        self._influxdb_client = DBClient(influxdb_org, influxdb_token)

    @staticmethod
    def _acquire_credentials() -> InfluxCredentials:
        influxdb_credentials: InfluxCredentials = InfluxCredentials.load(INFLUXDB_CREDENTIAL_BLOCK_NAME)

        return influxdb_credentials

    def store(self, **kwargs) -> FileLoader:
        raise NotImplementedError("`store` method is not implemented for InfluxDBDataSource "
                                  "as InfluxDB is read-only for Sunbeam!")

    def get(self, canonical_path: str, **kwargs) -> Result:
        bucket, car, measurement, start, stop, field = File.unwrap_canonical_path(canonical_path)
        start_dt: datetime = parse_iso_datetime(start)
        stop_dt: datetime = parse_iso_datetime(stop)

        try:
            return Result.Ok(
                self._influxdb_client.query_series(
                    start=start_dt,
                    stop=stop_dt,
                    bucket=bucket,
                    car=car,
                    measurement=measurement,
                    field=field,
                )
            )

        except Exception as e:
            return Result.Err(e)
