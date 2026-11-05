import tomllib

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from config import VEHICLES_PATH
from db.sunbeamdb.seed_data import get_or_create_vehicle


class VehicleManager:
    def __init__(self):
        with open(VEHICLES_PATH, "rb") as f:
            self._raw_vehicles: list[dict] = tomllib.load(f)["vehicle"]

    def sync_vehicles(self, engine: Engine) -> None:
        with Session(engine) as session:
            for raw_vehicle in self._raw_vehicles:
                get_or_create_vehicle(session, name=raw_vehicle["name"], description=raw_vehicle["description"])
            session.commit()
