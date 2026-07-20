import dataclasses
import pathlib
import tomllib
from dataclasses import dataclass
from enum import StrEnum
from sqlalchemy import URL

_CONTEXT_PATH = pathlib.Path(__file__).parent / "context.toml"


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
    debug: bool
    debug_time: str


@dataclass
class SunbeamDB:
    database_host_ip: str
    database_port: int
    database_name: str
    database_username: str
    database_password: str

    def build_url(self) -> URL:
        return URL.create(
            drivername="postgresql+psycopg",
            username=self.database_username,
            password=self.database_password,
            host=self.database_host_ip,
            port=self.database_port,
            database=self.database_name,
        )


@dataclass
class SunbeamServer:
    server_url: str
    server_port: int
    worker_network: str | None = None

    def build_url(self) -> str:
        return f"http://{self.server_url}:{self.server_port}"


class ServiceType(StrEnum):
    Client = "client"
    Server = "server"
    Worker = "worker"


class Context:
    _instance: "Context | None" = None

    _sunbeam_db: "SunbeamDB | None"
    _telemetry_db: "TelemetryDB | None"
    _sunbeam_server: "SunbeamServer | None"

    def __new__(cls) -> "Context":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._sunbeam_db = None
            cls._instance._telemetry_db = None
            cls._instance._sunbeam_server = None
        return cls._instance

    @classmethod
    def from_config(
        cls,
        config_dict: dict,
        service: ServiceType,
        configuration_type: str | None = None,
    ) -> "Context":
        ctx = cls()

        if configuration_type is None:
            configuration_type = config_dict["main"]["default_config"]

        service_config_dict = config_dict[service.value]

        ctx._sunbeam_db = SunbeamDB(
            **service_config_dict["sunbeamdb"][configuration_type]
        )

        ctx._telemetry_db = TelemetryDB(
            **service_config_dict["telemetrydb"][configuration_type]
        )

        ctx._sunbeam_server = SunbeamServer(
            **service_config_dict["sunbeam-server"][configuration_type]
        )

        return ctx

    @classmethod
    def load(
        cls,
        service: ServiceType,
        configuration_type: str | None = None,
    ) -> "Context":
        """
        Load context.toml and configure the singleton, unless it has already
        been configured and no explicit configuration_type override is given
        (in which case the existing configuration is returned as-is). This
        lets any module reach a ready-to-use Context on its own, without
        needing to repeat the tomllib-load boilerplate or coordinate with
        whichever code path configures Context first.
        """
        ctx = cls()

        if ctx._sunbeam_db is not None and configuration_type is None:
            return ctx

        with open(_CONTEXT_PATH, "rb") as f:
            config_dict = tomllib.load(f)

        return cls.from_config(config_dict, service, configuration_type)

    @property
    def sunbeam_db(self) -> SunbeamDB:
        if self._sunbeam_db is None:
            raise RuntimeError("Sunbeam DB has not been configured.")
        return self._sunbeam_db

    @property
    def telemetry_db(self) -> TelemetryDB:
        if self._telemetry_db is None:
            raise RuntimeError("Telemetry DB has not been configured.")
        return self._telemetry_db

    @property
    def sunbeam_server(self) -> SunbeamServer:
        if self._sunbeam_server is None:
            raise RuntimeError("Sunbeam server has not been configured.")
        return self._sunbeam_server

    def describe(self) -> dict:
        """
        A redacted, log-safe snapshot of the resolved configuration.
        Passwords and API tokens are never included.
        """
        return {
            "sunbeam_db": _redacted_dict(self.sunbeam_db, {"database_password"}),
            "telemetry_db": _redacted_dict(self.telemetry_db, {"token"}),
            "sunbeam_server": dataclasses.asdict(self.sunbeam_server),
        }
