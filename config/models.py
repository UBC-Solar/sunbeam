from pydantic import BaseModel, Field


class DataSourceConfig(BaseModel):
    data_source_type: str


class FSDataSourceConfig(DataSourceConfig):
    fs_root: str


class MongoDBDataSourceConfig(DataSourceConfig):
    ingress_origin: str = Field(default=str)


class InfluxDBDataSourceConfig(DataSourceConfig):
    start: str
    stop: str


class SunbeamConfig(BaseModel):
    events_description_file: str
    ingress_description_file: str
    stages_to_run: list[str]


class DataSourceConfigFactory:
    @staticmethod
    def build(data_source_type: str, data_source_config: dict) -> DataSourceConfig:
        unified_config = data_source_config[data_source_type]
        unified_config.update(data_source_config)

        match data_source_type:
            case "FSDataSource":
                return FSDataSourceConfig(**unified_config)

            case "MongoDBDataSource":
                return MongoDBDataSourceConfig(**unified_config)

            case "InfluxDBDataSource":
                return InfluxDBDataSourceConfig(**unified_config)

            case _:
                raise AssertionError(f"Unrecognized DataSourceType in sunbeam.toml: {data_source_type}!")

