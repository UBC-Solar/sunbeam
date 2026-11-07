"""
Creating, editing and deleting vehicles. Like events, they're defined only in the database.
"""
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import Event, Vehicle
from server.errors import Conflict


def get_vehicle(db: Session, vehicle_id: int) -> Vehicle:
    vehicle = db.get(Vehicle, vehicle_id)
    if vehicle is None:
        raise LookupError("Vehicle not found")
    return vehicle


def event_counts(db: Session) -> dict[int, int]:
    """ How many events each vehicle has, by vehicle id; vehicles with none are left out. """
    return dict(db.execute(select(Event.vehicle_id, func.count()).group_by(Event.vehicle_id)).tuples().all())


def create_vehicle(db: Session, *, name: str, description: str | None = None) -> Vehicle:
    vehicle = Vehicle()
    _apply(db, vehicle, name=name, description=description)
    db.add(vehicle)
    db.commit()
    db.refresh(vehicle)
    return vehicle


def update_vehicle(db: Session, vehicle_id: int, changes: dict) -> Vehicle:
    """ Only the fields in changes are set. """
    vehicle = get_vehicle(db, vehicle_id)
    if "name" in changes and changes["name"] is None:
        raise ValueError("name can't be empty.")

    _apply(db, vehicle, **{field: changes[field] for field in ("name", "description") if field in changes})
    db.commit()
    db.refresh(vehicle)
    return vehicle


def delete_vehicle(db: Session, vehicle_id: int) -> None:
    """ Only a vehicle with no events can be deleted: delete or move its events first. """
    vehicle = get_vehicle(db, vehicle_id)
    events = event_counts(db).get(vehicle.id, 0)
    if events:
        raise Conflict(f"{vehicle.name} has {events} event{'s' if events != 1 else ''}; delete or move them first.")

    db.delete(vehicle)
    db.commit()


def _apply(db: Session, vehicle: Vehicle, **fields) -> None:
    for field, value in fields.items():
        if value is not None:
            value = value.strip() or None  # blank is the same as none
        setattr(vehicle, field, value)

    if not vehicle.name:
        raise ValueError("A vehicle needs a name.")
    # Checked before flushing, so a taken name is a clear 409 rather than the database's unique constraint
    with db.no_autoflush:
        taken = db.scalar(select(Vehicle).where(Vehicle.name == vehicle.name, Vehicle.id != vehicle.id))
    if taken is not None:
        raise Conflict(f"A vehicle named {vehicle.name!r} already exists.")
