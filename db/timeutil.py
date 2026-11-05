"""
Time helpers shared by the worker and server. Everything is UTC; SQLite (the default test suite) drops tzinfo from
stored values, so naive datetimes are taken to be UTC.
"""
from datetime import UTC, datetime


def utcnow() -> datetime:
    return datetime.now(UTC)


def as_utc(dt: datetime) -> datetime:
    return dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)


def is_past(ends_at: datetime | None) -> bool:
    """ True if an event with this end has ended; an event without one is ongoing. """
    return ends_at is not None and as_utc(ends_at) < utcnow()
