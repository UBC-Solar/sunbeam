from data_tools.schema import DataSource, FileLoader, Result, CanonicalPath
from data_tools.query import DBClient
from data_tools.utils import parse_iso_datetime
from datetime import datetime
import os


class InfluxDBDataSource(DataSource):
    def __init__(self, start: datetime, stop: datetime, *args, **kwargs):
        super().__init__()

        self._start = start
        self._stop = stop

        influxdb_token = os.getenv("INFLUX_TOKEN")
        influxdb_org = os.getenv("INFLUX_ORG")

        self._influxdb_client = DBClient(influxdb_org, influxdb_token)

    def store(self, **kwargs) -> FileLoader:
        raise NotImplementedError("`store` method is not implemented for InfluxDBDataSource "
                                  "as InfluxDB is read-only for Sunbeam!")

    def get(self, canonical_path: CanonicalPath, start: str = None, stop: str = None) -> Result:
        bucket, measurement, car, field = canonical_path.unwrap()
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
