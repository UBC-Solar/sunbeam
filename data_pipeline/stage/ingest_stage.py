from argparse import FileType

from data_pipeline.stage.stage import Stage
from data_pipeline.data_source import InfluxDBDataSource
from data_pipeline.stage.stage_registry import stage_registry
from data_tools.schema import File, Result, FileLoader, FileType
from data_tools.utils import parse_iso_datetime
from data_pipeline.overseer import Overseer
from data_tools.query.influxdb_query import TimeSeriesTarget
from data_tools.collections.time_series import TimeSeries
from typing import List, Dict
import logging


class IngestStage(Stage):
    """
    Ingest raw time series data from InfluxDB and marshal it for use in the data pipeline.
    """

    @classmethod
    def get_stage_name(cls):
        return "ingest"

    @staticmethod
    def dependencies():
        return []

    def __init__(self, overseer: Overseer, logger: logging.Logger, config: dict):
        super().__init__(overseer, logger)

        self._ingest_data_source = InfluxDBDataSource(
            parse_iso_datetime(config["start"]),
            parse_iso_datetime(config["stop"])
        )

        self._extracted_time_series_data: Dict[str, Result] = {}
        self._processed_time_series_data: Dict[str, Result] = {}

    # noinspection PyMethodOverriding
    def extract(self, logger: logging.Logger, targets: List[TimeSeriesTarget]):
        """
        Extract raw data and marshall it for use in the data pipeline.

        :param targets: the targets that will be acquired from InfluxDB
        :param logger: logger for the extraction stage
        """
        for target in targets:
            try:
                queried_data = self._ingest_data_source.get(File.make_canonical_path(
                    origin=target.bucket,
                    path=[target.car, target.measurement],
                    name=target.field
                )).unwrap()

                self._extracted_time_series_data[target.name] = Result.Ok({
                    "data": queried_data,
                    "units": target.units,
                    "granularity": 1 / target.frequency
                })

                logger.info(f"Successfully extracted time series data for {target.name}")

            except Exception as e:
                self._extracted_time_series_data[target.name] = Result.Err(e)
                logger.error(f"Failed to extract time series data for {target.name}")

    # noinspection PyMethodOverriding
    def transform(self, logger: logging.Logger) -> None:
        """
        Process raw time series data into

        :param logger:
        :param args:
        :param kwargs:
        :return:
        """
        for name, result in self._extracted_time_series_data.items():
            # Check if we're going to get an error
            if result:

                # If not, try to process the data
                try:
                    target = result.unwrap()

                    data = target["data"]
                    units = target["units"]
                    granularity = target["granularity"]

                    self._processed_time_series_data[name] = Result.Ok(TimeSeries.from_query_dataframe(
                        query_df=data,
                        granularity=granularity,
                        field=name,
                        units=units
                    ))

                    logger.info(f"Successfully processed time series data {name}.")

                # Oops, wrap the error
                except Exception as e:
                    self._processed_time_series_data[name] = Result.Err(e)
                    logger.error(f"Failed to process time series data {name}!")

            # If we're going to get an error, forward it along
            else:
                self._processed_time_series_data[name] = result

    def load(self, logger: logging.Logger, *args, **kwargs) -> Dict[str, FileLoader]:
        result_dict = {}

        for name, result in self._processed_time_series_data.items():
            file = File(
                origin=self._overseer.title,
                path=self.get_stage_name(),
                name=name,
                data=result.unwrap() if result else None,
                file_type=FileType.TimeSeries
            )

            result_dict[name] = self._overseer.data_source.store(file)

        return result_dict


stage_registry.register_stage(IngestStage.get_stage_name(), IngestStage)
