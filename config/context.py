"""
Where this process finds Postgres, InfluxDB and the Sunbeam server, read
from the environment and nothing else. The server keeps its InfluxDB
connections in the database and hands the chosen one to each worker it
launches as SUNBEAM_INFLUX_* variables; a hand-run worker sets them itself
(or passes the matching sunbeam.py flags).
"""
import dataclasses
import os
from dataclasses import dataclass

from sqlalchemy import URL, make_url

DEFAULT_DATABASE_URL = "postgresql+psycopg://telemetry:telemetry@localhost:5432/telemetry"
DEFAULT_SERVER_URL = "http://localhost:8000"
DEFAULT_WORKER_NETWORK = "sunbeam-net"
DEFAULT_WORKER_SERVER_URL = "http://server:8000"

INFLUX_ENV = {
    "database_url": "SUNBEAM_INFLUX_URL",
    "organization": "SUNBEAM_INFLUX_ORG",
    "bucket": "SUNBEAM_INFLUX_BUCKET",
    "token": "SUNBEAM_INFLUX_TOKEN",
}


def _redacted_dict(instance, secret_fields: set[str]) -> dict:
    data = dataclasses.asdict(instance)
    for field in secret_fields:
        if data.get(field) is not None:
            data[field] = "***"
    return data


@dataclass
class TelemetryDB:
    database_url: str
    bucket: str
    organization: str
    token: str

    def as_env(self) -> dict[str, str]:
        """ The SUNBEAM_INFLUX_* variables that make a worker load this connection. """
        return {env: getattr(self, field) for field, env in INFLUX_ENV.items()}


@dataclass
class SunbeamDB:
    url: str

    def build_url(self) -> URL:
        return make_url(self.url)


@dataclass
class SunbeamServer:
    url: str

    def build_url(self) -> str:
        return self.url.rstrip("/")


class Context:
    """
    Read-only view of the environment. Each property reads its variables on
    access, so tests (and sunbeam.py, which turns its flags into variables)
    only need to set the environment before using it.
    """

    @property
    def sunbeam_db(self) -> SunbeamDB:
        return SunbeamDB(os.environ.get("SUNBEAM_DATABASE_URL") or DEFAULT_DATABASE_URL)

    @property
    def telemetry_db(self) -> TelemetryDB:
        values = {field: os.environ.get(env) for field, env in INFLUX_ENV.items()}
        missing = [INFLUX_ENV[field] for field, value in values.items() if not value]
        if missing:
            raise RuntimeError(
                f"InfluxDB is not configured: set {', '.join(missing)} "
                "(or the matching sunbeam.py --influx-* flags)."
            )
        return TelemetryDB(**values)  # type: ignore[arg-type]

    @property
    def sunbeam_server(self) -> SunbeamServer:
        return SunbeamServer(os.environ.get("SUNBEAM_SERVER_URL") or DEFAULT_SERVER_URL)

    @property
    def worker_network(self) -> str | None:
        """ The Docker network the server attaches launched workers to; empty means none. """
        return os.environ.get("SUNBEAM_WORKER_NETWORK", DEFAULT_WORKER_NETWORK) or None

    @property
    def worker_server_url(self) -> str:
        """ The server's URL as seen from inside a launched worker container. """
        return os.environ.get("SUNBEAM_WORKER_SERVER_URL") or DEFAULT_WORKER_SERVER_URL

    @property
    def worker_database_url(self) -> str:
        """
        The server's own database as seen from inside a launched worker container: the same URL, unless the
        server reaches it by a name the workers can't (e.g. localhost), when SUNBEAM_WORKER_DATABASE_URL says.
        """
        return os.environ.get("SUNBEAM_WORKER_DATABASE_URL") or self.sunbeam_db.url

    def describe(self) -> dict:
        """
        A redacted, log-safe snapshot of the resolved configuration.
        Passwords and API tokens are never included.
        """
        try:
            telemetry_db: dict | str = _redacted_dict(self.telemetry_db, {"token"})
        except RuntimeError:
            telemetry_db = "not configured"

        return {
            "sunbeam_db": self.sunbeam_db.build_url().render_as_string(hide_password=True),
            "telemetry_db": telemetry_db,
            "sunbeam_server": self.sunbeam_server.build_url(),
        }
