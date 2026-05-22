from config import VEHICLES_PATH
from db.sunbeamdb.models import Vehicle
from db.sunbeamdb.seed_data import get_or_create_vehicle
from sqlalchemy.orm import Session
from sqlalchemy import Engine
import tomllib


class VehicleManager:
    def __init__(self):
        with open(VEHICLES_PATH, "rb") as f:
            self._raw_vehicles: list[dict] = tomllib.load(f)["vehicle"]
            self._vehicles: dict[str, Vehicle] = {}

    def sync_vehicles(self, engine: Engine) -> dict[str, Vehicle]:
        with Session(engine) as session:
            for raw_vehicle in self._raw_vehicles:
                vehicle = get_or_create_vehicle(
                    session,
                    name=raw_vehicle["name"],
                    description=raw_vehicle["description"]
                )

                self._vehicles[vehicle.name] = vehicle

            session.commit()

        return self._vehicles
