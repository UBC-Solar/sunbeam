from __future__ import annotations

import os
from datetime import datetime, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from db.models import Event, Signal, Vehicle, EventStatus
from data_tools.localization import CanonicalName, SunbeamDBLanguageLocalization


def get_or_create_event(
    session: Session,
    *,
    name: str,
    vehicle_id: int,
    starts_at: datetime,
    ends_at: datetime | None,
    status: EventStatus,
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
        status=EventStatus.UNPROCESSED,
        description=description,
    )
    session.add(event)
    session.flush()
    return event



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


def collect_signal_metadata_for_event(event: Event) -> list[dict]:
    start_time = event.starts_at
    signals = []
    for name in CanonicalName:
        try:
            field, source, units, frequency = SunbeamDBLanguageLocalization.localize(name, start_time.date())
            signals.append(
                {
                    "name": str(name),
                    "unit": units,
                    "frequency": frequency,
                    "source": source,
                    "event_id": event.id
                }
            )
        except (ValueError, UnboundLocalError):
            continue

    return signals

def get_or_create_signal(
    session: Session,
    *,
    name: str,
    event_id: int,
    unit: str | None,
    source: str = None,
    frequency: float | None,
    description: str | None = None,
) -> Signal:
    stmt = select(Signal).where(Signal.name == name, Signal.event_id == event_id)
    signal = session.execute(stmt).scalar_one_or_none()
    if signal is not None:
        return signal

    signal = Signal(
        name=name,
        unit=unit,
        frequency=frequency,
        description=description,
        event_id=event_id,
        source=source
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
            status=EventStatus.ONGOING,
            description="Example active event for local development"
        )

        signals = collect_signal_metadata_for_event(event)

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
                f"frequency={signal.frequency}"
            )


if __name__ == "__main__":
    main()