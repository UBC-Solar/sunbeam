from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models import Event, Signal, Vehicle

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://telemetry:telemetry@localhost:5432/telemetry",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_or_create_vehicle(
    session: Session,
    *,
    name: str,
    description: str | None = None,
) -> Vehicle:
    stmt = select(Vehicle).where(Vehicle.name == name)
    vehicle = session.execute(stmt).scalar_one_or_none()
    if vehicle is not None:
        return vehicle

    vehicle = Vehicle(
        name=name,
        description=description,
    )
    session.add(vehicle)
    session.flush()
    return vehicle


def get_or_create_event(
    session: Session,
    *,
    name: str,
    vehicle_id: int,
    starts_at: datetime,
    ends_at: datetime | None,
    status: str,
    description: str | None = None,
) -> Event:
    stmt = select(Event).where(Event.name == name)
    event = session.execute(stmt).scalar_one_or_none()
    if event is not None:
        return event

    event = Event(
        name=name,
        vehicle_id=vehicle_id,
        starts_at=starts_at,
        ends_at=ends_at,
        status=status,
        description=description,
    )
    session.add(event)
    session.flush()
    return event


def get_or_create_signal(
    session: Session,
    *,
    name: str,
    unit: str | None,
    value_type: str,
    source_kind: str,
    nominal_rate_hz: float | None,
    alignment_method: str | None,
    max_age_ms: int | None,
    persist_aligned: bool,
    description: str | None = None,
) -> Signal:
    stmt = select(Signal).where(Signal.name == name)
    signal = session.execute(stmt).scalar_one_or_none()
    if signal is not None:
        return signal

    signal = Signal(
        name=name,
        unit=unit,
        value_type=value_type,
        source_kind=source_kind,
        nominal_rate_hz=nominal_rate_hz,
        alignment_method=alignment_method,
        max_age_ms=max_age_ms,
        persist_aligned=persist_aligned,
        description=description,
    )
    session.add(signal)
    session.flush()
    return signal


def main() -> None:
    engine = create_engine(DATABASE_URL, echo=False)

    with Session(engine) as session:
        vehicle = get_or_create_vehicle(
            session,
            name="Brightside",
            description="UBC Solar's 3rd generation vehicle",
        )

        now = utc_now()
        event = get_or_create_event(
            session,
            name="Bench Test 2026-03-31",
            vehicle_id=vehicle.id,
            starts_at=now,
            ends_at=None,
            status="active",
            description="Example active event for local development",
        )

        signals = [
            {
                "name": "MotorCurrent",
                "unit": "A",
                "value_type": "float8",
                "source_kind": "raw",
                "nominal_rate_hz": 10.0,
                "alignment_method": "zoh",
                "max_age_ms": 100,
                "persist_aligned": True,
                "description": "Motor DC current",
            },
            {
                "name": "VehicleSpeed",
                "unit": "m/s",
                "value_type": "float8",
                "source_kind": "raw",
                "nominal_rate_hz": 10.0,
                "alignment_method": "zoh",
                "max_age_ms": 500,
                "persist_aligned": True,
                "description": "Vehicle speed",
            },
            {
                "name": "PackVoltage",
                "unit": "V",
                "value_type": "float8",
                "source_kind": "raw",
                "nominal_rate_hz": 10.0,
                "alignment_method": "zoh",
                "max_age_ms": 500,
                "persist_aligned": True,
                "description": "Battery pack voltage",
            },
            {
                "name": "MotorPower",
                "unit": "W",
                "value_type": "float8",
                "source_kind": "derived",
                "nominal_rate_hz": 10.0,
                "alignment_method": "zoh",
                "max_age_ms": 100,
                "persist_aligned": True,
                "description": "Derived motor electrical power",
            },
            {
                "name": "MotorEfficiency",
                "unit": "1",
                "value_type": "float8",
                "source_kind": "derived",
                "nominal_rate_hz": 10.0,
                "alignment_method": "zoh",
                "max_age_ms": 500,
                "persist_aligned": True,
                "description": "Derived motor efficiency",
            },
        ]

        created_signals: list[Signal] = []
        for signal_data in signals:
            signal = get_or_create_signal(session, **signal_data)
            created_signals.append(signal)

        session.commit()

        print("Vehicle:")
        print(f"  id={vehicle.id}, name={vehicle.name}")

        print("\nEvent:")
        print(f"  id={event.id}, name={event.name}, status={event.status}")

        print("\nSignals:")
        for signal in created_signals:
            print(
                f"  id={signal.id}, "
                f"name={signal.name}, "
                f"source_kind={signal.source_kind}, "
                f"nominal_rate_hz={signal.nominal_rate_hz}"
            )


if __name__ == "__main__":
    main()