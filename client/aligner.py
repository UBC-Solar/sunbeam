from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from data_tools.localization import LanguageLocalization, CanonicalName
from fast_ingress import FastLastValueReader

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


from models import AlignedSample, Event, Signal

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://telemetry:telemetry@localhost:5432/telemetry",
)


@dataclass
class LatestValue:
    value_f64: float
    sample_ts: datetime


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def floor_time_to_period(ts: datetime, period_s: float) -> datetime:
    """
    Floors a timezone-aware datetime to the nearest lower multiple of period_s
    since the Unix epoch.
    """
    epoch = ts.timestamp()
    floored = math.floor(epoch / period_s) * period_s
    return datetime.fromtimestamp(floored, tz=timezone.utc)


def get_active_event(session: Session) -> Event:
    stmt = select(Event).where(Event.status == "active").limit(1)
    event = session.execute(stmt).scalar_one_or_none()
    if event is None:
        raise RuntimeError("No active event found.")
    return event


def get_persisted_aligned_signals(session: Session) -> list[Signal]:
    stmt = select(Signal)
    return list(session.execute(stmt).scalars().all())


def get_signal_name_to_id_map(session: Session) -> dict[str, int]:
    stmt = select(Signal.id, Signal.name)
    rows = session.execute(stmt).all()
    return {name: signal_id for signal_id, name in rows}


def query_latest_values(
    *,
    session: Session,
    event_id: int,
    signal_names: dict[str, int],
    frame_ts: datetime,
    reader: FastLastValueReader
) -> Dict[int, Optional[LatestValue]]:
    """
    Placeholder integration point.

    This function is assumed to exist per your request.
    It should return, for each signal_id, the latest raw value at or before frame_ts.

    Return format:
        {
            signal_id: LatestValue(...),
            signal_id: None,  # if no data exists yet
        }
    """
    values = {}
    ret = {}
    for signal_name in signal_names.keys():
        if signal_name not in CanonicalName:
            continue

        canonical_name = CanonicalName(signal_name)
        values[canonical_name] = None

    last_values = reader.get_last_values_before()

    for canonical_name in values.keys():
        query_name, _, _ = LanguageLocalization.localize(canonical_name, datetime(2024, 7, 20, 0, 0).date())

        value = last_values[query_name]

        if value is not None:
            ret[signal_names[canonical_name]] = LatestValue(value['value'], value['time'])

    return ret

def build_aligned_rows(
    *,
    event_id: int,
    frame_ts: datetime,
    signals: list[Signal],
    latest_values: Dict[int, Optional[LatestValue]],
) -> list[AlignedSample]:
    rows: list[AlignedSample] = []

    for signal in signals:
        latest = latest_values.get(signal.id)

        if latest is None:
            row = AlignedSample(
                event_id=event_id,
                ts=frame_ts,
                signal_id=signal.id,
                value_f64=None,
            )
            rows.append(row)
            continue

        row = AlignedSample(
            event_id=event_id,
            ts=frame_ts,
            signal_id=signal.id,
            value_f64=latest.value_f64,
        )
        rows.append(row)

    return rows


def run_aligner(rate_hz: float = 20.0) -> None:
    """
    Run a simple zero-order-hold aligner on a fixed clock.

    One tick:
      - find active event
      - find signals with persist_aligned = true
      - query latest values as of frame_ts
      - insert one aligned row per signal
    """
    period_s = 1.0 / rate_hz
    engine = create_engine(DATABASE_URL, echo=False)
    reader = FastLastValueReader()
    print(f"Starting aligner at {rate_hz} Hz")

    with Session(engine) as session:
        event = get_active_event(session)
        signals = get_persisted_aligned_signals(session)
        signal_names = get_signal_name_to_id_map(session)

        if not signals:
            raise RuntimeError("No signals configured with persist_aligned = true.")

        signal_ids = [s.id for s in signals]

    next_tick = floor_time_to_period(utc_now(), period_s) + timedelta(seconds=period_s)

    while True:
        now = utc_now()
        sleep_s = (next_tick - now).total_seconds()
        if sleep_s > 0.0:
            time.sleep(sleep_s)

        frame_ts = next_tick

        with Session(engine) as session:
            event = get_active_event(session)
            signals = get_persisted_aligned_signals(session)

            latest_values = query_latest_values(
                session=session,
                event_id=event.id,
                signal_names=signal_names,
                frame_ts=frame_ts,
                reader=reader
            )

            rows = build_aligned_rows(
                event_id=event.id,
                frame_ts=frame_ts,
                signals=signals,
                latest_values=latest_values,
            )

            session.add_all(rows)
            session.commit()

        print(f"[{frame_ts.isoformat()}] wrote {len(rows)} aligned rows")

        next_tick = next_tick + timedelta(seconds=period_s)


if __name__ == "__main__":
    run_aligner(rate_hz=20.0)