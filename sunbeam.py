import os
from datetime import datetime, timezone

from sqlalchemy import create_engine
from db import create_schema
from config import VehicleManager, EventManager, SignalManager
from pipeline import Executor


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://telemetry:telemetry@localhost:5432/telemetry",
)


class Sunbeam:
    def __init__(self, sunbeamdb_url: str = None):
        self.sunbeamdb_url = sunbeamdb_url if sunbeamdb_url else DATABASE_URL

        self._engine = create_engine(DATABASE_URL, echo=False)
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

    def run(self, event_name, reprocess: bool = False, debug: bool = False, debug_time: datetime = None):
        executor = Executor(event_name, self._engine, reprocess=reprocess, debug=debug, debug_time=debug_time)
        executor.run()

if __name__ == "__main__":
    sunbeam = Sunbeam()

    sunbeam.start()
    sunbeam.run("realtime", reprocess=True, debug=True, debug_time=datetime(2024, 7, 16, 14, 10, tzinfo=timezone.utc))
