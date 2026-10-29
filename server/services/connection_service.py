import logging
from collections.abc import Callable
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import Connection, ConnectionKind, Event
from db.telemetrydb.connection_check import CheckResult, check_influx, event_window

logger = logging.getLogger("sunbeam.server")

EDITABLE_FIELDS = ("name", "url", "organization", "bucket", "token")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ConnectionTestFailed(Exception):
    """ A connection failed its test, so nothing was launched with it. """

    def __init__(self, connection: Connection, result: CheckResult):
        super().__init__(f"Connection {connection.name!r} failed its test: {result.detail}")
        self.result = result


class ConnectionService:
    """
    The InfluxDB connections workers can be launched with. Exactly one per
    kind is the default whenever any exist.
    """

    def __init__(self, checker: Callable[..., CheckResult] = check_influx) -> None:
        self._checker = checker

    def list(self, db: Session) -> list[Connection]:
        return list(db.scalars(select(Connection).order_by(Connection.name)))

    def get(self, db: Session, connection_id: int) -> Connection | None:
        return db.get(Connection, connection_id)

    def default(self, db: Session, kind: ConnectionKind = ConnectionKind.INFLUXDB) -> Connection | None:
        return db.scalar(select(Connection).where(Connection.kind == kind, Connection.is_default.is_(True)))

    def resolve(self, db: Session, connection_id: int | None) -> Connection:
        """ The given connection, or the default when None. """
        if connection_id is None:
            connection = self.default(db)
            if connection is None:
                raise ValueError("No default InfluxDB connection; create one on the dashboard, or pick one.")
            return connection

        connection = self.get(db, connection_id)
        if connection is None:
            raise ValueError(f"No connection exists with id={connection_id}")
        return connection

    def _check_name_free(self, db: Session, name: str, exclude_id: int | None = None) -> None:
        existing = db.scalar(select(Connection).where(Connection.name == name))
        if existing is not None and existing.id != exclude_id:
            raise ValueError(f"A connection named {name!r} already exists")

    def _make_default(self, db: Session, connection: Connection) -> None:
        for other in db.scalars(select(Connection).where(Connection.kind == connection.kind)):
            other.is_default = other.id == connection.id

    def create(self, db: Session, *, is_default: bool = False, **fields: str) -> Connection:
        self._check_name_free(db, fields["name"])

        connection = Connection(kind=ConnectionKind.INFLUXDB, is_default=False, **fields)
        db.add(connection)
        db.flush()

        # The first connection of a kind is its default, so launches always have one to fall back on
        if is_default or self.default(db, connection.kind) is None:
            self._make_default(db, connection)

        db.commit()
        db.refresh(connection)
        logger.info("Created connection %r (%s)", connection.name, connection.url)
        return connection

    def update(self, db: Session, connection_id: int, **fields: str | None) -> Connection | None:
        connection = self.get(db, connection_id)
        if connection is None:
            return None

        changes = {key: value for key, value in fields.items() if key in EDITABLE_FIELDS and value is not None}
        if "name" in changes:
            self._check_name_free(db, changes["name"], exclude_id=connection.id)

        for key, value in changes.items():
            setattr(connection, key, value)

        # An earlier test result says nothing about the new settings
        if changes.keys() - {"name"}:
            connection.last_tested_at = None
            connection.last_test_ok = None
            connection.last_test_detail = None

        db.commit()
        db.refresh(connection)
        return connection

    def set_default(self, db: Session, connection_id: int) -> Connection | None:
        connection = self.get(db, connection_id)
        if connection is None:
            return None

        self._make_default(db, connection)
        db.commit()
        db.refresh(connection)
        return connection

    def delete(self, db: Session, connection_id: int) -> bool:
        connection = self.get(db, connection_id)
        if connection is None:
            return False

        was_default, kind = connection.is_default, connection.kind
        db.delete(connection)
        db.flush()

        if was_default:
            successor = db.scalar(select(Connection).where(Connection.kind == kind).order_by(Connection.id))
            if successor is not None:
                successor.is_default = True

        db.commit()
        return True

    def window_for(self, db: Session, event_id: int | None) -> tuple[datetime, datetime] | None:
        if event_id is None:
            return None

        event = db.get(Event, event_id)
        if event is None:
            raise ValueError(f"No event exists with id={event_id}")
        return event_window(event.starts_at, event.ends_at)

    def check(
        self,
        *,
        url: str,
        organization: str,
        bucket: str,
        token: str,
        window: tuple[datetime, datetime] | None,
    ) -> CheckResult:
        return self._checker(url, organization, bucket, token, window)

    def check_saved(
        self,
        db: Session,
        connection: Connection,
        window: tuple[datetime, datetime] | None,
    ) -> CheckResult:
        """ Test a saved connection and record the outcome on it. """
        result = self.check(
            url=connection.url,
            organization=connection.organization,
            bucket=connection.bucket,
            token=connection.token,
            window=window,
        )

        connection.last_tested_at = utcnow()
        connection.last_test_ok = result.ok
        connection.last_test_detail = result.detail
        db.commit()
        db.refresh(connection)

        return result
