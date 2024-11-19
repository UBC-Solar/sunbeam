from argparse import FileType
from data_pipeline.stage.stage import Stage
from data_pipeline.data_source import InfluxDBDataSource
from data_pipeline.stage.stage_registry import stage_registry
from data_tools.schema import File, Result, FileLoader, FileType, Event, UnwrappedError
from data_tools.utils import parse_iso_datetime
from data_pipeline.context import Context
from data_tools.query.influxdb_query import TimeSeriesTarget
from data_tools.collections.time_series import TimeSeries
from typing import List, Dict
import logging
import traceback


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

    def __init__(self, context: Context, logger: logging.Logger, config: dict):
        super().__init__(context, logger)

        self._ingest_data_source = InfluxDBDataSource(
            parse_iso_datetime(config["start"]),
            parse_iso_datetime(config["stop"])
        )

        self._extracted_time_series_data: Dict[str, Dict[str, Result]] = {}
        self._processed_time_series_data: Dict[str, Dict[str, Result]] = {}

    def extract(self, targets: List[TimeSeriesTarget], events: List[Event]):
        """
        Extract raw data and marshall it for use in the data pipeline.

        :param events: the events that the raw time series data will be divided up into
        :param targets: the targets that will be acquired from InfluxDB
        """
        for event in events:
            self._extracted_time_series_data[event.name] = {}

            for target in targets:
                try:
                    queried_data = self._ingest_data_source.get(File.make_canonical_path(
                        origin=target.bucket,
                        path=[target.car, target.measurement, event.start_as_iso_str, event.stop_as_iso_str],
                        name=target.field
                    )).unwrap()

                    self._extracted_time_series_data[event.name][target.name] = Result.Ok({
                        "data": queried_data,
                        "units": target.units,
                        "granularity": 1 / target.frequency
                    })

                    self.logger.info(f"Successfully extracted time series data for {target.name} for {event.name}!")

                except UnwrappedError as e:
                    self._extracted_time_series_data[event.name][target.name] = Result.Err(e)
                    self.logger.error(f"Failed to extract time series data for {target.name} for {event.name}: "
                                      f"{traceback.format_exc()}")

    def transform(self) -> None:
        """
        Process raw time series data into
        """
        for event_name, event_values in self._extracted_time_series_data.items():
            self._processed_time_series_data[event_name] = {}

            for name, result in event_values.items():
                # Check if we're going to get an error
                if result:

                    # If not, try to process the data
                    try:
                        target = result.unwrap()

                        data = target["data"]
                        units = target["units"]
                        granularity = target["granularity"]

                        self._processed_time_series_data[event_name][name] = Result.Ok(TimeSeries.from_query_dataframe(
                            query_df=data,
                            granularity=granularity,
                            field=name,
                            units=units
                        ))

                        self.logger.info(f"Successfully processed time series data {name}.")

                    # Oops, wrap the error
                    except Exception as e:
                        self._processed_time_series_data[event_name][name] = Result.Err(e)
                        self.logger.error(f"Failed to process time series data {name}: {traceback.format_exc()}")

                # If we're going to get an error, forward it along
                else:
                    self._processed_time_series_data[event_name][name] = result

    def load(self) -> Dict[str, Dict[str, FileLoader]]:
        result_dict: Dict[str, Dict[str, FileLoader]] = {}

        for event_name, event_items in self._processed_time_series_data.items():
            result_dict[event_name] = {}

            for name, result in event_items.items():
                file = File(
                    origin=self.context.title,
                    path=[event_name, self.get_stage_name()],
                    name=name,
                    data=result.unwrap() if result else None,
                    file_type=FileType.TimeSeries
                )

                result_dict[event_name][name] = self.context.data_source.store(file)

                self.logger.info(f"Successfully loaded {name} for {event_name}!")

        return result_dict


stage_registry.register_stage(IngestStage.get_stage_name(), IngestStage)
