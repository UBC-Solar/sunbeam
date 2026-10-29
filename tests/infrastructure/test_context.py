import pytest

from config.context import DEFAULT_DATABASE_URL, Context

CONTEXT_ENV = [
    "SUNBEAM_DATABASE_URL",
    "SUNBEAM_WORKER_DATABASE_URL",
    "SUNBEAM_SERVER_URL",
    "SUNBEAM_WORKER_SERVER_URL",
    "SUNBEAM_WORKER_NETWORK",
    "SUNBEAM_INFLUX_URL",
    "SUNBEAM_INFLUX_ORG",
    "SUNBEAM_INFLUX_BUCKET",
    "SUNBEAM_INFLUX_TOKEN",
]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in CONTEXT_ENV:
        monkeypatch.delenv(name, raising=False)


class TestSunbeamDB:
    def test_defaults_to_the_compose_database_on_localhost(self):
        assert Context().sunbeam_db.url == DEFAULT_DATABASE_URL
        assert Context().sunbeam_db.build_url().host == "localhost"

    def test_reads_the_environment(self, monkeypatch):
        monkeypatch.setenv("SUNBEAM_DATABASE_URL", "postgresql+psycopg://u:p@db:5432/telemetry")

        url = Context().sunbeam_db.build_url()

        assert (url.host, url.username, url.database) == ("db", "u", "telemetry")

    def test_workers_get_the_servers_database_unless_overridden(self, monkeypatch):
        monkeypatch.setenv("SUNBEAM_DATABASE_URL", "postgresql+psycopg://u:p@localhost:5432/telemetry")
        assert Context().worker_database_url == "postgresql+psycopg://u:p@localhost:5432/telemetry"

        monkeypatch.setenv("SUNBEAM_WORKER_DATABASE_URL", "postgresql+psycopg://u:p@db:5432/telemetry")
        assert Context().worker_database_url == "postgresql+psycopg://u:p@db:5432/telemetry"


class TestTelemetryDB:
    def test_reads_the_environment(self, test_context):
        telemetry_db = Context().telemetry_db

        assert (telemetry_db.database_url, telemetry_db.organization, telemetry_db.bucket, telemetry_db.token) == (
            "http://localhost:8086", "test-org", "test-bucket", "test-token",
        )

    def test_missing_variables_are_named(self, monkeypatch):
        monkeypatch.setenv("SUNBEAM_INFLUX_URL", "http://localhost:8086")
        monkeypatch.setenv("SUNBEAM_INFLUX_ORG", "org")

        with pytest.raises(RuntimeError, match="SUNBEAM_INFLUX_BUCKET, SUNBEAM_INFLUX_TOKEN"):
            Context().telemetry_db

    def test_as_env_round_trips(self, test_context, monkeypatch):
        env = Context().telemetry_db.as_env()
        for name in env:
            monkeypatch.delenv(name)
        for name, value in env.items():
            monkeypatch.setenv(name, value)

        assert Context().telemetry_db.bucket == "test-bucket"


class TestServer:
    def test_defaults(self):
        context = Context()

        assert context.sunbeam_server.build_url() == "http://localhost:8000"
        assert context.worker_server_url == "http://server:8000"
        assert context.worker_network == "sunbeam-net"

    def test_empty_worker_network_means_none(self, monkeypatch):
        monkeypatch.setenv("SUNBEAM_WORKER_NETWORK", "")

        assert Context().worker_network is None

    def test_trailing_slash_is_dropped(self, monkeypatch):
        monkeypatch.setenv("SUNBEAM_SERVER_URL", "http://server:8000/")

        assert Context().sunbeam_server.build_url() == "http://server:8000"


class TestDescribe:
    def test_redacts_secrets(self, test_context):
        described = Context().describe()

        assert described["telemetry_db"]["token"] == "***"
        assert "test:test@" not in described["sunbeam_db"]

    def test_unconfigured_influx_is_not_an_error(self):
        assert Context().describe()["telemetry_db"] == "not configured"
