from enum import StrEnum
from typing import Type
from data_tools.schema import DataSource
from data_pipeline.data_source import fs_data_source, influxdb_data_source


class DataSourceType(StrEnum):
    FS = "FSDataSource"
    InfluxDB = "InfluxDBDataSource"


class DataSourceFactory:
    @staticmethod
    def build(data_source_type, *args, **kwargs) -> DataSource:
        match data_source_type:
            case DataSourceType.FS:
                return fs_data_source.FSDataSource(*args, **kwargs)

            case DataSourceType.InfluxDB:
                return influxdb_data_source.InfluxDBDataSource(*args, **kwargs)
