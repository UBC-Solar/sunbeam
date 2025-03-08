from config import DataSourceConfig
from stage.stage import Stage, StageError
from data_source import InfluxDBDataSource, FSDataSource, DataSourceType, MongoDBDataSource, SunbeamDataSource
from stage.stage_registry import stage_registry
from data_tools.schema import File, Result, FileLoader, FileType, Event, UnwrappedError, CanonicalPath
from data_tools.query.influxdb_query import TimeSeriesTarget
from data_tools.collections.time_series import TimeSeries
from typing import List, Dict
import traceback
from prefect import task
import datetime
import concurrent.futures


class IngressStage(Stage):
    """
    Ingest raw time series data from InfluxDB and marshal it for use in the data pipeline, or load pre-existing data
    from a local filesystem.
    """
    @classmethod
    def get_stage_name(cls):
        return "ingress"

    @staticmethod
    def dependencies():
        return []

    @staticmethod
    @task(name="Ingress")
    def run(self, targets: List[TimeSeriesTarget], events: List[Event]) -> Dict[str, Dict[str, FileLoader]]:
        """
        Ingest raw time series data from InfluxDB and marshal it for use in the data pipeline, or load pre-existing data
        from a local filesystem.

        :param self: an instance of IngressStage to be run
        :param targets: a list of m TimeSeriesTarget models which will be queried
        :param events: a list of n Event models specifying how the raw data should be temporally partitioned
        :return: a dictionary which can be indexed first by event, then by target name.
        """
        return super().run(self, targets, events)

    def __init__(self, config: DataSourceConfig):
        super().__init__()

        match config.data_source_type:
            case DataSourceType.FS:
                self._ingress_data_source = FSDataSource(config)

                self._ingress_origin = self.context.title

                self._extract_method = self._extract_existing
                self._transform_method = self._transform_existing
                self._load_method = self._load_and_store

            case DataSourceType.Sunbeam:
                self._ingress_data_source = SunbeamDataSource(config)
                self._ingress_origin = config.ingress_origin

                self._extract_method = self._extract_existing
                self._transform_method = self._transform_existing
                self._load_method = self._load_and_store

            case DataSourceType.InfluxDB:
                self._ingress_data_source = InfluxDBDataSource(config)

                self._extract_method = self._extract_influxdb
                self._transform_method = self._transform_influxdb
                self._load_method = self._load_and_store

            case DataSourceType.MongoDB:
                self._ingress_data_source = MongoDBDataSource()

                self._ingress_origin = config.ingress_origin

                assert self._ingress_origin != self.context.title, (f"You are trying to ingress from "
                                                                    f"{self._ingress_origin} and output to "
                                                                    f"{self.context.title} which is not permitted "
                                                                    f"for MongoDBDataSource. They must be "
                                                                    f"different locations!")

                self._extract_method = self._extract_existing
                self._transform_method = self._transform_existing
                self._load_method = self._load_and_store

            case _:
                raise StageError(self.get_stage_name(), f"Did not recognize {config["fs"]} as a valid Ingress "
                                                        f"stage data source!")

    def _fetch_from_influxdb(self, event, target):
        try:
            queried_data: Result = self._ingress_data_source.get(CanonicalPath(
                origin=self._ingress_origin,
                source=self.get_stage_name(),
                event=event.name,
                name=target.field
            )).unwrap()

            return event.name, target.name, Result.Ok(queried_data)

        except UnwrappedError as e:
            self.logger.error(f"Failed to find cached time series data for {target.name} for {event.name}: "
                              f"{traceback.format_exc()}")

            return event.name, target.name, Result.Err(e)

    def _extract_existing(self, targets: List[TimeSeriesTarget], events: List[Event]) -> tuple[Dict[str, Dict[str, Result]]]:
        extracted_time_series_data = {}

        for event in events:
            extracted_time_series_data[event.name] = {}

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(self._fetch_data, event, target): target for target in targets}

            for future in concurrent.futures.as_completed(futures):
                event_name, target_name, result = future.result()
                extracted_time_series_data[event_name][target_name] = result

        return (extracted_time_series_data,)

    def _transform_existing(self, extracted_time_series_data: Dict[str, Dict[str, Result]]) -> tuple[Dict[str, Dict[str, Result]]]:
        return (extracted_time_series_data,)

    def _load_existing(self, processed_time_series_data: Dict[str, Dict[str, Result]]) -> tuple[Dict[str, Dict[str, FileLoader]]]:
        result_dict: Dict[str, Dict[str, FileLoader]] = {}

        for event_name, event_items in processed_time_series_data.items():
            result_dict[event_name] = {}

            for name, result in event_items.items():
                canonical_path = CanonicalPath(
                        origin=self.context.title,
                        source=self.get_stage_name(),
                        event=event_name,
                        name=name
                    )

                result_dict[event_name][name] = FileLoader(
                    lambda x: self.context.data_source.get(x),
                    canonical_path
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
                    fake_start_time = "2024-07-16T17:00:00Z"
                    now = datetime.datetime.now(datetime.UTC)
                    timestamp = datetime.datetime.strptime(fake_start_time, "%Y-%m-%dT%H:%M:%SZ")
                    updated_timestamp = timestamp.replace(minute=now.minute, second=now.second)
                    updated_timestamp_str = updated_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

                    queried_data = self._ingress_data_source.get(
                        CanonicalPath(
                            origin=target.bucket,
                            source=target.car,
                            event=target.measurement,
                            name=target.field
                        ),
                        start=fake_start_time,
                        stop=updated_timestamp_str
                    ).unwrap()

                    extracted_time_series_data[event.name][target.name] = Result.Ok({
                        "data": queried_data,
                        "units": target.units,
                        "period": 1 / target.frequency,
                        "description": target.description
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

                        time_series = TimeSeries.from_query_dataframe(
                            query_df=data,
                            granularity=period,
                            field=name,
                            units=units
                        )

                        time_series.meta.update({"description": target["description"]})

                        file = File(
                            canonical_path=CanonicalPath(
                                origin=self.context.title,
                                source=self.get_stage_name(),
                                event=event_name,
                                name=name
                            ),
                            description=target["description"],
                            file_type=FileType.TimeSeries,
                            data=time_series
                        )

                        processed_time_series_data[event_name][name] = Result.Ok(file)

                        self.logger.info(f"Successfully processed time series data {name}.")

                    # Oops, wrap the error
                    except Exception as e:
                        processed_time_series_data[event_name][name] = Result.Err(e)
                        self.logger.error(f"Failed to process time series data {name}: {traceback.format_exc()}")

                # If we're going to get an error, forward it along
                else:
                    processed_time_series_data[event_name][name] = result

        return (processed_time_series_data,)

    def _load_and_store(self, processed_time_series_data: Dict[str, Dict[str, Result]]) -> Dict[str, Dict[str, FileLoader]]:
        result_dict: Dict[str, Dict[str, FileLoader]] = {}

        for event_name, event_items in processed_time_series_data.items():
            result_dict[event_name] = {}

            for name, result in event_items.items():
                canonical_path = CanonicalPath(
                    origin=self.context.title,
                    source=self.get_stage_name(),
                    event=event_name,
                    name=name
                )

                if result:
                    existing_file = result.unwrap()
                    existing_file.canonical_path = canonical_path
                    result_dict[event_name][name] = self.context.data_source.store(existing_file)

                    self.logger.info(f"Successfully loaded {name} for {event_name}!")

                else:
                    result_dict[event_name][name] = self.context.data_source.store(
                        File(
                            data=None,
                            canonical_path=canonical_path,
                            file_type=FileType.TimeSeries
                        )
                    )

                    self.logger.info(f"Failed to load {name} for {event_name}!")

        return result_dict


stage_registry.register_stage(IngressStage.get_stage_name(), IngressStage)
