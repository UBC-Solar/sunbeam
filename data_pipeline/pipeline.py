import pathlib

import pymongo.collection
from data_tools import Event, FileType, DataSource
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

from data_tools.query.influxdb_query import TimeSeriesTarget
from data_pipeline.data_source import DataSourceFactory, DataSourceType
from data_pipeline.overseer import Overseer
from data_pipeline.stage.power_stage import PowerStage
from influx_credentials import InfluxCredentials, INFLUXDB_CREDENTIAL_BLOCK_NAME
from data_pipeline.stage.ingest_stage import IngestStage


type PathLike = Union[str, pathlib.Path]


CONFIG_PATH = os.getenv("SUNBEAM_PIPELINE_CONFIG_PATH", pathlib.Path(__file__).parent / "config" / "sunbeam.toml")
REQUIRED_CONFIG = ["events_description_file", "ingest_data_source", "ingest_description_file", "stage_data_source"]
ROOT = pathlib.Path(__file__).parent


logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger()

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


# @task
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


# @task
def collect_events(events_description_filepath: PathLike) -> List[Event]:
    with open(ROOT / events_description_filepath) as events_description_file:
        events_descriptions = tomllib.load(events_description_file)["event"]

    events = [Event.from_dict(event) for event in events_descriptions]
    return list(events)


# @task
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
                "meta": datum.meta.dict()
            }
        )

    time_series_collection.create_index([("event", 1), ("code_hash", 1), ("field", 1)], unique=True)


# @task
def collect_sunbeam_config():
    logger.info(f"Trying to find config at {CONFIG_PATH}...")
    with open(CONFIG_PATH) as config_file:
        config = tomllib.load(config_file)["config"]

    logger.info(f"Acquired config from {CONFIG_PATH}:\n")
    logger.info(tomllib.dumps(config) + "\n")

    for required_config in REQUIRED_CONFIG:
        assert required_config in config, f"Missing Required Config: {required_config}\n"

    return config


# @task
def collect_config(config_filepath):
    with open(ROOT / f"{config_filepath}") as config_file:
        config = tomllib.load(config_file)

    return config


@flow(log_prints=True)
def pipeline(git_tag):
    pipeline_name = git_tag if git_tag is not None else "pipeline"

    local_pipeline(pipeline_name)


def local_pipeline(pipeline_name):
    sunbeam_config = collect_sunbeam_config()
    data_source_config = collect_config(sunbeam_config["stage_data_source_description_file"])
    ingest_config = collect_config(sunbeam_config["ingest_description_file"])
    targets = collect_targets(ingest_config)
    events = collect_events(sunbeam_config["events_description_file"])

    data_source = DataSourceFactory.build(sunbeam_config["stage_data_source"], data_source_config)
    overseer = Overseer(pipeline_name, data_source)

    ingest_stage = IngestStage(overseer, logger, ingest_config["config"])

    ingest_stage.extract(logger, targets)
    ingest_stage.transform(logger)
    ingest_outputs = ingest_stage.load(logger)

    power_stage = PowerStage(overseer, logger)
    power_stage.extract(logger, ingest_outputs["TotalPackVoltage"], ingest_outputs["PackCurrent"])
    power_stage.transform(logger)
    pack_power = power_stage.load(logger)


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os

    load_dotenv()

    # InfluxCredentials(
    #     influxdb_api_token=os.getenv("INFLUX_TOKEN"),
    #     influxdb_org=os.getenv("INFLUX_ORG")
    # ).save(INFLUXDB_CREDENTIAL_BLOCK_NAME, overwrite=True)

    local_pipeline("pipeline")
