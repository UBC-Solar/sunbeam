from config import VehicleManager, EventManager, SignalManager
from config.context import Context, ServiceType
from stage.stage_library import StageLibrary
from sqlalchemy import create_engine
from dataclasses import dataclass
from broker.server import Server
from db import create_schema
from typing import Mapping
import tomllib
import pathlib
import config
import docker


PROJECT_ROOT = pathlib.Path(__file__).parent.parent.absolute()


@dataclass
class PipelineWorker:
    pipeline_edition: str
    docker_image: str


def build_workers() -> Mapping[str, PipelineWorker]:
    library = StageLibrary()
    pipeline_editions = library.pipeline_editions
    client = docker.from_env()

    worker_versions = {}
    for pipeline_edition in pipeline_editions:
        tag = f"sunbeam-worker:{pipeline_edition}"
        client.images.build(
            path=str(PROJECT_ROOT),
            dockerfile="dockerfiles/worker.Dockerfile",
            tag=tag,
            rm=True,
            buildargs={
                "PIPELINE_EDITION": pipeline_edition,
            },
        )

        worker_versions[pipeline_edition] = PipelineWorker(pipeline_edition, tag)

    return worker_versions


class Orchestrator:
    def __init__(self):
        database_url = Context().sunbeam_db.build_url()

        self._engine = create_engine(database_url, echo=False)
        create_schema(self._engine)
        print("SunbeamDB initialized.")

        self._vehicle_manager = VehicleManager()
        self._events_manager = EventManager()

    def __del__(self):
        self._engine.dispose()

    def start(self):
        print("==== Syncing vehicles and events ==== ")
        self._vehicle_manager.sync_vehicles(self._engine)
        self._events_manager.sync_events(self._engine)
        print("==== Vehicles and events synced. ==== \n ")

        print("==== Syncing signals ==== ")
        SignalManager.sync_signals(self._engine)
        print("==== Signals synced ==== \n ")

        print("==== Building pipeline workers ==== ")
        build_workers()
        print("==== Pipeline workers built ==== ")


if __name__ == "__main__":
    with open(config.CONTEXT_PATH, "rb") as f:
        config_dict = tomllib.load(f)
        Context.from_config(config_dict, ServiceType.Broker)

    orchestrator = Orchestrator()
    orchestrator.start()

    broker_server = Server()