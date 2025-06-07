from config import DataSourceConfig
from stage.stage import Stage, StageError
from data_source import InfluxDBDataSource, FSDataSource, DataSourceType, MongoDBDataSource, SunbeamDataSource
from stage.stage_registry import stage_registry
from data_tools.schema import File, Result, FileLoader, FileType, Event, UnwrappedError, CanonicalPath
from data_tools.query.influxdb_query import TimeSeriesTarget
from data_tools.collections.time_series import TimeSeries
from typing import List, Dict, cast
import traceback
from prefect import task
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from typing import Dict, Tuple, Any


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
        ingress_dict, = super().run(self, targets, events)
        return cast(Dict[str, Dict[str, FileLoader]], ingress_dict)

    def __init__(self, config: DataSourceConfig):
        super().__init__()

        match config.data_source_type:
            case DataSourceType.FS:
                self._ingress_data_source = FSDataSource(config)

                self._ingress_origin = self.context.title

                self._extract_method = self._extract_transform_load_existing

            case DataSourceType.Sunbeam:
                self._ingress_data_source = SunbeamDataSource(config)
                self._ingress_origin = config.ingress_origin

                self._extract_method = self._extract_transform_load_existing

            case DataSourceType.InfluxDB:
                self._ingress_data_source = InfluxDBDataSource(config)
                self._ingress_origin = self.context.title

                self._extract_method = self._extract_transform_load_influxdb

            case DataSourceType.MongoDB:
                self._ingress_data_source = MongoDBDataSource()

                self._ingress_origin = config.ingress_origin

                assert self._ingress_origin != self.context.title, (f"You are trying to ingress from "
                                                                    f"{self._ingress_origin} and output to "
                                                                    f"{self.context.title} which is not permitted "
                                                                    f"for MongoDBDataSource. They must be "
                                                                    f"different locations!")

                self._extract_method = self._extract_transform_load_existing

            case _:
                raise StageError(self.get_stage_name(), f"Did not recognize {config["fs"]} as a valid Ingress "
                                                        f"stage data source!")

    def extract(
            self,
            targets: List[TimeSeriesTarget],
            events: List[Event]
    ) -> tuple[Dict[str, Dict[str, FileLoader]]]:
        return (self._extract_method(targets, events), )

    def transform(
            self,
            extracted_time_series_data: Dict[str, Dict[str, FileLoader]]
    ) -> tuple[Dict[str, Dict[str, FileLoader]]]:
        return (extracted_time_series_data, )

    def load(
            self,
            processed_time_series_data: Dict[str, Dict[str, FileLoader]]
    ) -> Dict[str, Dict[str, FileLoader]]:
        return processed_time_series_data

    def _extract_transform_load_influxdb(
            self,
            targets: List[TimeSeriesTarget],
            events: List[Event]
    ) -> tuple[Dict[str, Dict[str, FileLoader]]]:
        """
        Extract raw data and marshall it for use in the data pipeline.

        :param events: the events that the raw time series data will be divided up into
        :param targets: the targets that will be acquired from InfluxDB
        """
        result_dict: Dict[str, Dict[str, FileLoader]] = {event.name: {} for event in events}

        with ProcessPoolExecutor(max_workers=10) as executor:
            future_to_key: Dict[Any, Tuple[str, str]] = {}

            for event in events:
                for target in targets:
                    fut = executor.submit(self._process_one, event, target)
                    future_to_key[fut] = (event.name, target.name)

            for fut in as_completed(future_to_key):
                ev_name, tgt_name, loader = fut.result()
                result_dict[ev_name][tgt_name] = loader

        return (result_dict,)

    def _process_one(self, event, target) -> Tuple[str, str, FileLoader]:
        extracted = self._fetch_from_influxdb(event, target)
        timeseries = self._transform_into_timeseries(extracted, event.name, target.name)
        loader = self._load_timeseries(timeseries, event.name, target.name)

        return event.name, target.name, loader

    def _extract_transform_load_existing(
            self,
            targets: List[TimeSeriesTarget],
            events: List[Event]
    ) -> tuple[Dict[str, Dict[str, FileLoader]]]:
        """
        Extract raw data and marshall it for use in the data pipeline.

        :param events: the events that the raw time series data will be divided up into
        :param targets: the targets that will be acquired from InfluxDB
        """
        result_dict: Dict[str, Dict[str, FileLoader]] = {}

        for event in events:
            result_dict[event.name] = {}

            for target in targets:
                result = self._fetch_from_existing(event, target)
                result_dict[event.name][target.name] = self._load_timeseries(result, event.name, target.name)

        return (result_dict, )

    def _fetch_from_existing(self, event, target):
        try:
            queried_data: Result = self._ingress_data_source.get(CanonicalPath(
                origin=self._ingress_origin,
                source=self.get_stage_name(),
                event=event.name,
                name=target.field
            )).unwrap()

            result = Result.Ok(queried_data)
        except UnwrappedError as e:
            result = Result.Err(e)
            self.logger.error(f"Failed to find cached time series data for {target.name} for {event.name}: "
                              f"{traceback.format_exc()}")

        return result

    def _fetch_from_influxdb(self, event: Event, target: TimeSeriesTarget) -> Result[dict]:
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

            result = Result.Ok({
                "data": queried_data,
                "units": target.units,
                "period": 1 / target.frequency,
                "description": target.description
            })

            # self.logger.info(f"Successfully extracted time series data for {target.name} for {event.name}!")

        except UnwrappedError as e:
            result = Result.Err(e)
            # self.logger.error(f"Failed to extract time series data for {target.name} for {event.name}: "
            #                   f"{traceback.format_exc()}")

        return result

    def _transform_into_timeseries(self, result: Result[dict], event_name: str, name: str) -> Result[File]:
        if result:  # Check if we're going to get an error
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

                time_series_result = Result.Ok(file)

                # self.logger.info(f"Successfully processed time series data {name}.")

            # Oops, wrap the error
            except Exception as e:
                time_series_result = Result.Err(e)
                # self.logger.error(f"Failed to process time series data {name}: {traceback.format_exc()}")

        # If we're going to get an error, forward it along
        else:
            time_series_result = result

        return time_series_result

    def _load_timeseries(self, result: Result[File], event_name: str, name: str) -> FileLoader:
        canonical_path = CanonicalPath(
            origin=self.context.title,
            source=self.get_stage_name(),
            event=event_name,
            name=name
        )

        if result:
            existing_file = result.unwrap()
            updated_file = File(
                data=existing_file.data,
                file_type=existing_file.file_type,
                canonical_path=canonical_path,
                metadata=existing_file.metadata,
                description=existing_file.description
            )
            file_loader = self.context.data_source.store(updated_file)

            # self.logger.info(f"Successfully loaded {name} for {event_name}!")

        else:
            file_loader = self.context.data_source.store(
                File(
                    data=None,
                    canonical_path=canonical_path,
                    file_type=FileType.TimeSeries
                )
            )

            # self.logger.info(f"Failed to load {name} for {event_name}!")

        return file_loader


stage_registry.register_stage(IngressStage.get_stage_name(), IngressStage)