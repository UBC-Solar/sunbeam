import pathlib

import pymongo.collection
from data_tools import DBClient, FluxQuery
import numpy as np
import toml as tomllib
from pydantic import BaseModel, Field
from typing import List, Dict
import logging
import traceback
from tqdm import tqdm
from datetime import datetime
from prefect import flow, task
from pymongo import MongoClient
from influx_credentials import InfluxCredentials, INFLUXDB_CREDENTIAL_BLOCK_NAME


logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger()

code_hash = None


class Target(BaseModel):
    field: str
    measurement: str
    frequency: float = Field(gt=0)
    units: str


class DatumMeta(BaseModel):
    name: str
    start_time: datetime
    end_time: datetime
    granularity: float
    meta: Dict
    units: str


class Datum(BaseModel):
    meta: DatumMeta
    data: list


@task
def collect_targets() -> List[Target]:
    print(os.listdir(os.getcwd()))
    with open(pathlib.Path(__file__).parent / "ingest.toml") as config_file:
        ingest_config = tomllib.load(config_file)

    targets = []
    for target in ingest_config["target"]:
        try:
            targets.append(
                Target(
                    field=target["field"],
                    measurement=target["measurement"],
                    frequency=target["frequency"],
                    units=target["units"]
                )
            )

        except KeyError:
            logger.error(f"Missing key in target! \n {traceback.format_exc()}")

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
def ingest_data(targets: List[Target], influxdb_credentials: InfluxCredentials) -> List[Datum]:
    influxdb_token = influxdb_credentials.influxdb_api_token
    influxdb_org = influxdb_credentials.influxdb_org

    config = read_config()
    client = DBClient(influxdb_org, influxdb_token)

    data: List[Datum] = []
    with tqdm(desc="Querying targets", total=len(targets), position=0) as pbar:
        for target in targets:
            try:
                pbar.update(1)
                query = FluxQuery()\
                    .from_bucket("CAN_log")\
                    .range(start=config.start, stop=config.end)\
                    .filter(field=target.field, measurement=target.measurement)\
                    .car("Brightside")
                query_df = client.query_dataframe(query)

                print(query_df.columns.tolist())
                queried_data = query_df[target.field].tolist()

                data.append(Datum(
                    data=queried_data,
                    meta=DatumMeta(
                        name=target.field,
                        start_time=config.start,
                        end_time=config.end,
                        granularity=1.0 / target.frequency,
                        units=target.units,
                        meta={
                            "field": target.field,
                            "measurement": target.measurement,
                            "car": "Brightside"
                        }
                    )
                ))
                logger.info(f"Processed target: {target.field}!")

            except (ValueError, KeyError):
                logger.error(f"Failed to query target: {target}! \n {traceback.format_exc()}")

    return data


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
                "meta": datum.meta.dict()
            }
        )

    time_series_collection.create_index([("event", 1), ("code_hash", 1), ("field", 1)], unique=True)


@task
def init_environment() -> InfluxCredentials:
    influxdb_credentials: InfluxCredentials = InfluxCredentials.load(INFLUXDB_CREDENTIAL_BLOCK_NAME)

    print(f"INFLUX_TOKEN: {influxdb_credentials.influxdb_api_token}\n")
    print(f"INFLUX_ORG: {influxdb_credentials.influxdb_org}")

    return influxdb_credentials


@flow(log_prints=True)
def pipeline(git_tag):
    global code_hash
    code_hash = git_tag

    influxdb_credentials = init_environment()
    targets = collect_targets()
    data = ingest_data(targets, influxdb_credentials)
    load_data(data)


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os

    load_dotenv()

    InfluxCredentials(
        influxdb_api_token=os.getenv("INFLUX_TOKEN"),
        influxdb_org=os.getenv("INFLUX_ORG")
    ).save(INFLUXDB_CREDENTIAL_BLOCK_NAME, overwrite=True)

    pipeline()
