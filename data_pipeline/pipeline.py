import pathlib

import pymongo.collection
from data_tools import Event, FileType, DataSource, FileLoader
import toml as tomllib
from pydantic import BaseModel
from typing import List, Dict, Union
import logging
import traceback
from tqdm import tqdm
from datetime import datetime
import os
from prefect import flow, task
from pymongo import MongoClient
import networkx as nx
import pprint
from data_pipeline.logs import log_directory
from data_tools.utils import configure_logger

from data_tools.query.influxdb_query import TimeSeriesTarget
from data_pipeline.data_source import DataSourceFactory, DataSourceType
from data_pipeline.context import Context
from data_pipeline.stage.power_stage import PowerStage
from data_pipeline.stage.stage import StageResult, StageError, Stage
from data_pipeline.stage.ingest_stage import IngestStage
from data_pipeline.stage.stage_registry import StageRegistry, stage_registry

type PathLike = Union[str, pathlib.Path]


CONFIG_PATH = os.getenv("SUNBEAM_PIPELINE_CONFIG_PATH", pathlib.Path(__file__).parent / "config" / "sunbeam.toml")
REQUIRED_CONFIG = ["events_description_file", "ingest_description_file", "stage_data_source"]
ROOT = pathlib.Path(__file__).parent

logger = logging.getLogger("sunbeam")
configure_logger(logger, log_directory / "sunbeam.log")

# Test logging
logger.info("This is an INFO message")  # This should now appear
logger.error("This is an ERROR message")  # This will also appear

code_hash = None


class DatumMeta(BaseModel):
    name: str
    start_time: datetime
    end_time: datetime
    granularity: float
    meta: Dict
    units: str
    car: str
    bucket: str


class Datum(BaseModel):
    meta: DatumMeta
    data: list


@task
def collect_targets(ingest_config: dict) -> List[TimeSeriesTarget]:
    targets = []
    seen_names = set()

    for target in ingest_config["target"]:
        target_type = FileType(target["type"])

        match target_type:
            case FileType.TimeSeries:
                if not target["name"] in seen_names:
                    try:
                        targets.append(
                            TimeSeriesTarget(
                                name=target["name"],
                                field=target["field"],
                                measurement=target["measurement"],
                                frequency=target["frequency"],
                                units=target["units"],
                                car=target["car"],
                                bucket=target["bucket"],
                            )
                        )

                    except KeyError:
                        logger.error(f"Missing key in target! \n {traceback.format_exc()}")

                else:
                    raise ValueError(f"Target names must be unique! {target['name']} "
                                     f"is already the name of another target.")

            case FileType.Scalar:
                raise NotImplementedError("Ingestion of `Scalar` type not implemented!")

    assert len(targets) > 0, "Unable to identify any targets in 'ingest.toml'!"

    return targets


class Config(BaseModel):
    start: str
    end: str


def read_config() -> Config:
    with open(pathlib.Path(__file__).parent / "ingest.toml") as config_file:
        ingest_config = tomllib.load(config_file)["config"]

    start = ingest_config["start"]
    end = ingest_config["end"]

    config = Config(
        start=start,
        end=end
    )

    return config


@task
def collect_events(events_description_filepath: PathLike) -> List[Event]:
    with open(ROOT / events_description_filepath) as events_description_file:
        events_descriptions = tomllib.load(events_description_file)["event"]

    events = [Event.from_dict(event) for event in events_descriptions]
    return list(events)


@task
def load_data(data: List[Datum]):
    client = MongoClient("mongodb://mongodb:27017/")

    db = client.sunbeam_db
    time_series_collection: pymongo.collection.Collection = db.time_series_data

    for datum in data:
        time_series_collection.insert_one(
            {
                "event": "FSGP",
                "code_hash": code_hash,
                "field": datum.meta.name,
                "data": datum.data,
                "meta": datum.meta.model_dump()
            }
        )

    time_series_collection.create_index([("event", 1), ("code_hash", 1), ("field", 1)], unique=True)


@task
def collect_sunbeam_config():
    logger.info(f"Trying to find config at {CONFIG_PATH}...")
    with open(CONFIG_PATH) as config_file:
        config = tomllib.load(config_file)["config"]

    logger.info(f"Acquired config from {CONFIG_PATH}:\n")
    logger.info(tomllib.dumps(config) + "\n")

    for required_config in REQUIRED_CONFIG:
        assert required_config in config, f"Missing Required Config: {required_config}\n"

    return config


@task
def collect_config(config_filepath):
    with open(ROOT / f"{config_filepath}") as config_file:
        config = tomllib.load(config_file)

    return config


@flow(log_prints=True)
def pipeline(git_tag="pipeline"):
    pipeline_name = git_tag

    local_pipeline(pipeline_name, ["power"])


def add_dependencies(dependency_graph: nx.Graph, stage_id: str):
    dependency_graph.add_node(stage_id)

    stage_cls = stage_registry.get_stage(stage_id)
    for dep_id in stage_cls.dependencies():
        add_dependencies(dependency_graph, dep_id)


def local_pipeline(pipeline_name: str, stages_to_run: List[str]):
    # Build a dependency graph
    dependency_graph = nx.DiGraph()

    # Add nodes and edges based on dependencies
    for stage_id in stages_to_run:
        add_dependencies(dependency_graph, stage_id)

    # Check for cycles and determine execution order
    stages_to_run = list(nx.topological_sort(dependency_graph))
    stages_to_run.reverse()  # The topological sort will be in the inverse order, so we need to just reverse it

    logger.info(f"Executing stages in order: {" -> ".join(stages_to_run)}")

    sunbeam_config: dict = collect_sunbeam_config()
    data_source_config: dict = collect_config(sunbeam_config["stage_data_source_description_file"])
    ingest_config: dict = collect_config(sunbeam_config["ingest_description_file"])
    targets: List[TimeSeriesTarget] = collect_targets(ingest_config)
    events: List[Event] = collect_events(sunbeam_config["events_description_file"])

    data_source: DataSource = DataSourceFactory.build(sunbeam_config["stage_data_source"], data_source_config["config"])
    context: Context = Context(pipeline_name, data_source, stages_to_run)

    ingest_stage: IngestStage = IngestStage(context, ingest_config["config"])
    ingest_outputs: StageResult = ingest_stage.run(targets, events)

    # We will process each event separately.
    for event in events:
        event_name = event.name

        power_stage: PowerStage = PowerStage(context, event_name)
        pack_power, motor_power = power_stage.run(
            ingest_outputs[event_name]["TotalPackVoltage"],
            ingest_outputs[event_name]["PackCurrent"],
            ingest_outputs[event_name]["BatteryCurrent"],
            ingest_outputs[event_name]["BatteryVoltage"],
        )


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os

    load_dotenv()

    pipeline()
