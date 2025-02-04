from data_pipeline.config import DataSourceConfig
from data_pipeline.stage.stage import Stage, StageResult, StageError
from data_pipeline.data_source import InfluxDBDataSource, FSDataSource, DataSourceType, MongoDBDataSource
from data_pipeline.stage.stage_registry import stage_registry
from data_tools.schema import File, Result, FileLoader, FileType, Event, UnwrappedError, CanonicalPath
from data_tools.utils import parse_iso_datetime
from data_pipeline.context import Context
from data_tools.query.influxdb_query import TimeSeriesTarget
from data_tools.collections.time_series import TimeSeries
from typing import List, Dict
import traceback


class IngressStage(Stage):
    """
    Ingest raw time series data from InfluxDB and marshal it for use in the data pipeline, or load pre-existing data
    from a local filesystem.
    """

    def run(self, targets: List[TimeSeriesTarget], events: List[Event]) -> StageResult:
        """
        Ingest raw time series data from InfluxDB and marshal it for use in the data pipeline, or load pre-existing data
        from a local filesystem.

        :param targets: a list of m TimeSeriesTarget models which will be queried
        :param events: a list of n Event models specifying how the raw data should be temporally partitioned
        :return: a StageResult [n][m] which can be indexed first by event, then by target name.
        """
        return super().run(targets, events)

    @classmethod
    def get_stage_name(cls):
        return "ingress"

    @staticmethod
    def dependencies():
        return []

    def __init__(self, context: Context, config: DataSourceConfig):
        super().__init__(context)

        match config.data_source_type:
            case DataSourceType.FS:
                self._ingress_data_source = FSDataSource(config)

                self._ingress_origin = self.context.title

                self._extract_method = self._extract_existing
                self._transform_method = self._transform_existing
                self._load_method = self._load_existing

            case DataSourceType.InfluxDB:
                self._ingress_data_source = InfluxDBDataSource(
                    parse_iso_datetime(config.start),
                    parse_iso_datetime(config.start)
                )

                self._extract_method = self._extract_influxdb
                self._transform_method = self._transform_influxdb
                self._load_method = self._load_influxdb

            case DataSourceType.MongoDB:
                self._ingress_data_source = MongoDBDataSource()

                self._ingress_origin = config.ingress_origin

                self._extract_method = self._extract_existing
                self._transform_method = self._transform_existing
                self._load_method = self._load_existing

            case _:
                raise StageError(self.get_stage_name(), f"Did not recognize {config["fs"]} as a valid Ingress "
                                                        f"stage data source!")

        self.declare_output("marshaled_ingest_data")

    def _extract_existing(self, targets: List[TimeSeriesTarget], events: List[Event]) -> tuple[Dict[str, Dict[str, Result]]]:
        extracted_time_series_data = {}

        for event in events:
            extracted_time_series_data[event.name] = {}

            for target in targets:
                try:
                    queried_data: Result = self._ingress_data_source.get(CanonicalPath(
                        origin=self._ingress_origin,
                        source=self.get_stage_name(),
                        event=event.name,
                        name=target.field
                    )).unwrap()

                    extracted_time_series_data[event.name][target.name] = Result.Ok(queried_data)

                except UnwrappedError as e:
                    extracted_time_series_data[event.name][target.name] = Result.Err(e)
                    self.logger.error(f"Failed to find cached time series data for {target.name} for {event.name}: "
                                      f"{traceback.format_exc()}")

        return (extracted_time_series_data,)

    def _transform_existing(self, extracted_time_series_data: Dict[str, Dict[str, Result]]) -> tuple[Dict[str, Dict[str, Result]]]:
        return (extracted_time_series_data,)

    def _load_existing(self, processed_time_series_data: Dict[str, Dict[str, Result]]) -> tuple[Dict[str, Dict[str, FileLoader]]]:
        result_dict: Dict[str, Dict[str, FileLoader]] = {}

        for event_name, event_items in processed_time_series_data.items():
            result_dict[event_name] = {}

            for name, result in event_items.items():
                file = File(
                    data=result.unwrap() if result else None,
                    canonical_path=CanonicalPath(
                        origin=self.context.title,
                        source=self.get_stage_name(),
                        event=event_name,
                        name=name
                    ),
                    file_type=FileType.TimeSeries
                )

                result_dict[event_name][name] = FileLoader(
                    lambda x: self.context.data_source.get(x),
                    file.canonical_path
                )

                self.logger.info(f"Successfully loaded {name} for {event_name}!")

        return (result_dict,)

    def extract(self, targets: List[TimeSeriesTarget], events: List[Event]) -> tuple[dict[str, dict[str, Result]]]:
        return self._extract_method(targets, events)

    def transform(self, extracted_time_series_data: Dict[str, Dict[str, Result]]) -> tuple[Dict[str, Dict[str, Result]]]:
        return self._transform_method(extracted_time_series_data)

    def load(self, processed_time_series_data: Dict[str, Dict[str, Result]]) -> tuple[Dict[str, Dict[str, FileLoader]]]:
        return self._load_method(processed_time_series_data)

    def _extract_influxdb(self, targets: List[TimeSeriesTarget], events: List[Event]) -> tuple[
        Dict[str, Dict[str, Result]]]:
        """
        Extract raw data and marshall it for use in the data pipeline.

        :param events: the events that the raw time series data will be divided up into
        :param targets: the targets that will be acquired from InfluxDB
        """
        extracted_time_series_data = {}

        for event in events:
            extracted_time_series_data[event.name] = {}

            for target in targets:
                try:
                    queried_data = self._ingress_data_source.get(
                        CanonicalPath(
                            origin=target.bucket,
                            source=target.car,
                            event=target.measurement,
                            name=target.field
                        ),
                        start=event.start_as_iso_str,
                        stop=event.stop_as_iso_str
                    ).unwrap()

                    extracted_time_series_data[event.name][target.name] = Result.Ok({
                        "data": queried_data,
                        "units": target.units,
                        "period": 1 / target.frequency
                    })

                    self.logger.info(f"Successfully extracted time series data for {target.name} for {event.name}!")

                except UnwrappedError as e:
                    extracted_time_series_data[event.name][target.name] = Result.Err(e)
                    self.logger.error(f"Failed to extract time series data for {target.name} for {event.name}: "
                                      f"{traceback.format_exc()}")

        return (extracted_time_series_data,)

    def _transform_influxdb(self, extracted_time_series_data: Dict[str, Dict[str, Result]]) -> tuple[
        Dict[str, Dict[str, Result]]]:
        """
        Process raw time series data into
        """
        processed_time_series_data = {}

        for event_name, event_values in extracted_time_series_data.items():
            processed_time_series_data[event_name] = {}

            for name, result in event_values.items():
                # Check if we're going to get an error
                if result:

                    # If not, try to process the data
                    try:
                        target = result.unwrap()

                        data = target["data"]
                        units = target["units"]
                        period = target["period"]

                        processed_time_series_data[event_name][name] = Result.Ok(TimeSeries.from_query_dataframe(
                            query_df=data,
                            granularity=period,
                            field=name,
                            units=units
                        ))

                        self.logger.info(f"Successfully processed time series data {name}.")

                    # Oops, wrap the error
                    except Exception as e:
                        processed_time_series_data[event_name][name] = Result.Err(e)
                        self.logger.error(f"Failed to process time series data {name}: {traceback.format_exc()}")

                # If we're going to get an error, forward it along
                else:
                    processed_time_series_data[event_name][name] = result

        return (processed_time_series_data,)

    def _load_influxdb(self, processed_time_series_data: Dict[str, Dict[str, Result]]) -> tuple[
        Dict[str, Dict[str, FileLoader]]]:
        result_dict: Dict[str, Dict[str, FileLoader]] = {}

        for event_name, event_items in processed_time_series_data.items():
            result_dict[event_name] = {}

            for name, result in event_items.items():
                file = File(
                    data=result.unwrap() if result else None,
                    canonical_path=CanonicalPath(
                        origin=self.context.title,
                        source=self.get_stage_name(),
                        event=event_name,
                        name=name
                    ),
                    file_type=FileType.TimeSeries
                )

                result_dict[event_name][name] = self.context.data_source.store(file)

                self.logger.info(f"Successfully loaded {name} for {event_name}!")

        return (result_dict,)


stage_registry.register_stage(IngressStage.get_stage_name(), IngressStage)
