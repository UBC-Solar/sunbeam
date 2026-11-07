"""
Helpers that create an event's signals from the localization tables (see config/signals.py).
"""
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_tools.localization import CanonicalName, SunbeamDBLanguageLocalization
from db.sunbeamdb.models import Event, Signal


def collect_signal_metadata_for_event(event: Event) -> list[dict]:
    """ Every canonical signal the event's date has a localization for. """
    signals = []
    for name in CanonicalName:
        try:
            _, source, units, frequency = SunbeamDBLanguageLocalization.localize(name, event.starts_at.date())
        except (ValueError, UnboundLocalError):
            continue

        signals.append({
            "name": str(name),
            "unit": units,
            "frequency": frequency,
            "source": source,
            "event_id": event.id,
        })

    return signals


def get_or_create_signal(
    session: Session,
    *,
    name: str,
    event_id: int,
    unit: str | None,
    source: str,
    frequency: float | None,
    description: str | None = None,
) -> Signal:
    signal = session.scalar(select(Signal).where(Signal.name == name, Signal.event_id == event_id))
    if signal is not None:
        return signal

    signal = Signal(
        name=name,
        unit=unit,
        frequency=frequency,
        description=description,
        event_id=event_id,
        source=source,
    )
    session.add(signal)
    session.flush()
    return signal
