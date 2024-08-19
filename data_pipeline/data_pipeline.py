from data_tools.influx_client import InfluxClient, FluxQuery
import numpy as np
import toml as tomllib
from pydantic import BaseModel, Field
from typing import List, Dict
import logging
import traceback
from tqdm import tqdm
from datetime import datetime
import pickle
from prefect import flow, task
import os
from pymongo import MongoClient


logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger()


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
    with open("app/ingest.toml") as config_file:
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
    with open("ingest.toml") as config_file:
        ingest_config = tomllib.load(config_file)["config"]

    start = ingest_config["start"]
    end = ingest_config["end"]

    config = Config(
        start=start,
        end=end
    )

    return config


@task
def ingest_data(targets: List[Target]) -> List[Datum]:
    config = read_config()
    client = InfluxClient()

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
                logger.debug(f"Processed target: {target.field}!")

            except ValueError:
                logger.error(f"Failed to query target: {target}! \n {traceback.format_exc()}")

    return data


@task
def load_data(data: List[Datum]):
    # Connect to the local MongoDB server
    client = MongoClient("mongodb://localhost:27017/")

    # Access or create a database
    db = client["mydatabase"]

    # Access or create a collection (similar to a table in SQL)
    collection = db["people"]

    # Example objects
    person1 = {"age": 30, "gender": "male", "nationality": "USA"}
    person2 = {"age": 25, "gender": "female", "nationality": "Canada"}

    # Insert the objects into the collection
    collection.insert_one(person1)
    collection.insert_one(person2)

    # Ensure unique index on the combination of fields
    collection.create_index([("age", 1), ("gender", 1), ("nationality", 1)], unique=True)

    # if not os.path.isdir("data"):
    #     os.makedirs("data")
    #
    # file_sizes = {}
    #
    # for datum in data:
    #     name = datum.meta.name
    #     file_size = 0
    #
    #     with open(f'data/{name}.npy', "wb") as data_file:
    #         np.save(data_file, datum.data)
    #         file_size += data_file.tell()
    #
    #     with open(f'data/{name}.json', 'w') as meta_file:
    #         meta_file.write(datum.meta.model_dump_json())
    #         file_size += meta_file.tell()
    #
    #     logger.info(f"Wrote {file_size} bytes for {name}!")
    #     file_sizes[name] = file_size
    #
    # with open("data/file_sizes.pkl", "wb") as file_size_cache:
    #     pickle.dump(file_sizes, file_size_cache)


@flow(log_prints=True)
def pipeline():
    targets = collect_targets()
    data = ingest_data(targets)
    load_data(data)


if __name__ == "__main__":
    pipeline()
