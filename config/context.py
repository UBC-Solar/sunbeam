from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from sqlalchemy import URL

@dataclass
class TelemetryDB:
    database_url: str
    bucket: str
    organization: str
    token: str
    debug: bool
    debug_time: datetime


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
class SunbeamBroker:
    broker_url: str
    broker_port: int


class ServiceType(StrEnum):
    Client = "client"
    Broker = "broker"


class Context:
    _instance: "Context | None" = None

    def __new__(cls) -> "Context":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._sunbeam_db = None
            cls._instance._telemetry_db = None
            cls._instance._sunbeam_broker = None
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

        if service != ServiceType.Broker:
            ctx._sunbeam_broker = SunbeamBroker(
                **service_config_dict["sunbeam-broker"][configuration_type]
            )
        else:
            ctx._sunbeam_broker = None

        return ctx

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
    def sunbeam_broker(self) -> SunbeamBroker:
        if self._sunbeam_broker is None:
            raise RuntimeError("Sunbeam broker has not been configured.")
        return self._sunbeam_broker