from fastapi import APIRouter, Depends, Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import Vehicle
from server.deps import get_db
from server.schemas import VehicleCreate, VehicleRead, VehicleUpdate
from server.services import vehicle_service

router = APIRouter(prefix="/vehicles", tags=["vehicles"])


@router.get("", response_model=list[VehicleRead])
def list_vehicles(db: Session = Depends(get_db)):
    """ The vehicles an event can belong to, by name. """
    counts = vehicle_service.event_counts(db)
    return [_vehicle_read(vehicle, counts) for vehicle in db.scalars(select(Vehicle).order_by(Vehicle.name))]


@router.post("", response_model=VehicleRead, status_code=201)
def create_vehicle(payload: VehicleCreate, db: Session = Depends(get_db)):
    """ 409 if the name is taken. """
    vehicle = vehicle_service.create_vehicle(db, **payload.model_dump())
    return _vehicle_read(vehicle, {})


@router.patch("/{vehicle_id}", response_model=VehicleRead)
def update_vehicle(vehicle_id: int, payload: VehicleUpdate, db: Session = Depends(get_db)):
    """ Only the fields sent change. 409 if the new name is taken. """
    vehicle = vehicle_service.update_vehicle(db, vehicle_id, payload.model_dump(exclude_unset=True))
    return _vehicle_read(vehicle, vehicle_service.event_counts(db))


@router.delete("/{vehicle_id}", status_code=204)
def delete_vehicle(vehicle_id: int, db: Session = Depends(get_db)):
    """ 409 while the vehicle has events. """
    vehicle_service.delete_vehicle(db, vehicle_id)
    return Response(status_code=204)


def _vehicle_read(vehicle: Vehicle, counts: dict[int, int]) -> VehicleRead:
    return VehicleRead(
        id=vehicle.id, name=vehicle.name, description=vehicle.description, event_count=counts.get(vehicle.id, 0),
    )
