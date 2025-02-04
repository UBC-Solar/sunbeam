import pathlib

from data_tools import Event, FileType, DataSource
import toml as tomllib
from pydantic import BaseModel
from typing import List, Dict, Union
import logging
import traceback
from datetime import datetime
import os
from prefect import flow
import networkx as nx

from data_pipeline.config import DataSourceConfigFactory
from data_pipeline.logs import log_directory
from data_tools.utils import configure_logger

from data_tools.query.influxdb_query import TimeSeriesTarget
from data_pipeline.data_source import DataSourceFactory
from data_pipeline.context import Context
from data_pipeline.stage.power_stage import PowerStage
from data_pipeline.stage.stage import StageResult
from data_pipeline.stage.ingress_stage import IngressStage
from data_pipeline.stage.stage_registry import stage_registry
from data_pipeline.config.models import FSDataSourceConfig, MongoDBDataSourceConfig, InfluxDBDataSourceConfig, SunbeamConfig, DataSourceConfig


CONFIG_PATH = os.getenv("SUNBEAM_PIPELINE_CONFIG_PATH", pathlib.Path(__file__).parent / "config" / "sunbeam.toml")
REQUIRED_CONFIG = ["events_description_file", "ingress_description_file", "stage_data_source"]
ROOT = pathlib.Path(__file__).parent

logger = logging.getLogger("sunbeam")
configure_logger(logger, log_directory / "sunbeam.log")

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


def collect_targets(ingress_config: dict) -> List[TimeSeriesTarget]:
    targets = []
    seen_names = set()

    for target in ingress_config["target"]:
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

            case _:
                raise NotImplementedError(f"Ingress of {target_type} type not implemented!")

    assert len(targets) > 0, "Unable to identify any targets!"

    return targets


def collect_events(events_description_filepath: Union[str, pathlib.Path]) -> List[Event]:
    with open(ROOT / events_description_filepath) as events_description_file:
        events_descriptions = tomllib.load(events_description_file)["event"]

    events = [Event.from_dict(event) for event in events_descriptions]
    return list(events)


def collect_config_file(config_path: pathlib.Path):
    logger.info(f"Trying to find config at {config_path}...")
    with open(config_path) as config_file:
        config = tomllib.load(config_file)

    logger.info(f"Acquired config from {config_path}:\n")
    logger.info(tomllib.dumps(config) + "\n")

    return config


def collect_config():
    config_file: dict = collect_config_file(CONFIG_PATH)
    sunbeam_config = SunbeamConfig(**config_file["config"])

    stage_data_source_type = config_file["stage_data_source"]["data_source_type"]
    ingress_data_source_type = config_file["ingress_data_source"]["data_source_type"]

    data_source_config: DataSourceConfig = DataSourceConfigFactory.build(stage_data_source_type, config_file["stage_data_source"])
    ingress_config: DataSourceConfig = DataSourceConfigFactory.build(ingress_data_source_type, config_file["ingress_data_source"])

    targets_file: dict = collect_config_file(pathlib.Path(sunbeam_config.ingress_description_file).absolute())
    targets: List[TimeSeriesTarget] = collect_targets(targets_file)

    events: List[Event] = collect_events(pathlib.Path(sunbeam_config.events_description_file).absolute())

    return sunbeam_config, data_source_config, ingress_config, targets, events


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

    sunbeam_config, data_source_config, ingress_config, targets, events = collect_config()

    data_source: DataSource = DataSourceFactory.build(data_source_config.data_source_type, data_source_config)
    context: Context = Context(pipeline_name, data_source, stages_to_run)

    ingress_stage: IngressStage = IngressStage(context, ingress_config)
    ingress_outputs: StageResult = ingress_stage.run(targets, events)

    # We will process each event separately.
    for event in events:
        event_name = event.name
        event_ingress_outputs = ingress_outputs[event_name]

        power_stage: PowerStage = PowerStage(context, event_name)
        pack_power, motor_power = power_stage.run(
            event_ingress_outputs["TotalPackVoltage"],
            event_ingress_outputs["PackCurrent"],
            event_ingress_outputs["BatteryCurrent"],
            event_ingress_outputs["BatteryVoltage"],
        )


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os

    load_dotenv()

    pipeline()
