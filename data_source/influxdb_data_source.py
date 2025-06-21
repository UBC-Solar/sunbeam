from data_tools.schema import DataSource, FileLoader, Result, CanonicalPath
from data_tools.query import DBClient
from data_tools.utils import parse_iso_datetime
from datetime import datetime, timedelta
from config import InfluxDBDataSourceConfig
import os


class InfluxDBDataSource(DataSource):
    def __init__(self, config: InfluxDBDataSourceConfig, *args, **kwargs):
        super().__init__()

        self._start = config.start
        self._stop = config.stop
        self._url = config.url

        influxdb_token = os.getenv("INFLUX_TOKEN")
        influxdb_org = os.getenv("INFLUX_ORG")

        self._influxdb_client = DBClient(influxdb_org, influxdb_token, url=self._url)

    def store(self, **kwargs) -> FileLoader:
        raise NotImplementedError("`store` method is not implemented for InfluxDBDataSource "
                                  "as InfluxDB is read-only for Sunbeam!")

    def get(
            self,
            canonical_path: CanonicalPath,
            start: str = None,
            stop: str = None,
            offset: str = None
    ) -> Result:
        offset_dt = timedelta(hours=float(offset)) if offset else timedelta(seconds=0)

        bucket, measurement, car, field = canonical_path.unwrap()
        start_dt: datetime = parse_iso_datetime(start) + offset_dt
        stop_dt: datetime = parse_iso_datetime(stop) + offset_dt

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
