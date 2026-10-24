import tomllib
from datetime import UTC, datetime

from sqlalchemy import create_engine

import config
from config import EventManager, SignalManager, VehicleManager
from config.context import Context, ServiceType
from db import create_schema
from pipeline import Executor


class Sunbeam:
    def __init__(self):
        database_url = Context().sunbeam_db.build_url()

        self._engine = create_engine(database_url, echo=False)
        create_schema(self._engine)
        print("SunbeamDB initialized.")

        self._vehicle_manager = VehicleManager()
        self._events_manager = EventManager()
        self._vehicles = None
        self._events = None
        self._signals = None

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

    def run(self, event_name, reprocess: bool = False, debug: bool = False, debug_time: datetime | None = None):
        executor = Executor(event_name, self._engine, reprocess=reprocess, debug=debug, debug_time=debug_time)
        executor.run()

if __name__ == "__main__":
    with open(config.CONTEXT_PATH, "rb") as f:
        config_dict = tomllib.load(f)
        Context.from_config(config_dict, ServiceType.Client)

    sunbeam = Sunbeam()
    sunbeam.start()
    sunbeam.run("FSGP_2024_Day_1", reprocess=True, debug=False, debug_time=datetime(2024, 7, 16, 14, 10, tzinfo=UTC))
