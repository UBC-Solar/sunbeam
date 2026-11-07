"""
Keeps the Alembic migrations honest: upgrading a fresh database to head must
produce the models' schema (plus the hypertable), and it must walk back down.
"""
import pathlib

import pytest
from sqlalchemy import inspect, text

from alembic import command
from alembic.config import Config

pytestmark = pytest.mark.postgres

REPO_ROOT = pathlib.Path(__file__).parent.parent.parent

EXPECTED_TABLES = {
    "vehicle",
    "event",
    "signal",
    "worker_run",
    "connection",
    "sample",
}


@pytest.fixture
def alembic_config(pg_container, pg_admin_engine, monkeypatch):
    """An Alembic Config pointed at a brand-new, schema-less database."""
    db_name = "sunbeam_migrations_test"

    with pg_admin_engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {db_name} WITH (FORCE)"))
        conn.execute(text(f"CREATE DATABASE {db_name}"))

    url = pg_container.get_connection_url().rsplit("/", 1)[0] + f"/{db_name}"
    monkeypatch.setenv("SUNBEAM_DATABASE_URL", url)

    config = Config(str(REPO_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    return config, url


class TestMigrations:
    def test_upgrade_head_builds_full_schema(self, alembic_config):
        config, url = alembic_config

        command.upgrade(config, "head")

        from sqlalchemy import create_engine

        engine = create_engine(url)
        try:
            tables = set(inspect(engine).get_table_names())
            with engine.connect() as conn:
                hypertables = set(
                    conn.scalars(
                        text(
                            "SELECT hypertable_name FROM timescaledb_information.hypertables"
                        )
                    )
                )
        finally:
            engine.dispose()

        assert EXPECTED_TABLES <= tables
        assert hypertables == {"sample"}

    def test_downgrade_base_removes_schema(self, alembic_config):
        config, url = alembic_config

        command.upgrade(config, "head")
        command.downgrade(config, "base")

        from sqlalchemy import create_engine

        engine = create_engine(url)
        try:
            tables = set(inspect(engine).get_table_names())
        finally:
            engine.dispose()

        assert EXPECTED_TABLES.isdisjoint(tables)


class TestBaseline:
    def test_matches_the_models(self, alembic_config):
        """ Fails when a model changes without a migration: run `alembic revision --autogenerate`. """
        config, _ = alembic_config
        command.upgrade(config, "head")

        command.check(config)

    def test_seeds_the_default_connections(self, alembic_config):
        from sqlalchemy import create_engine

        config, url = alembic_config
        command.upgrade(config, "head")

        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                connections = conn.execute(text("SELECT name, is_default FROM connection ORDER BY id")).all()
        finally:
            engine.dispose()

        assert connections == [("UBC Influx (CAN_log)", True), ("Production (CAN_prod)", False)]


class TestServerStartupUpgrade:
    """server.migrations.upgrade_database — what on_startup now runs."""

    def test_fresh_database_gets_full_schema(self, pg_blank_engine):
        from server.migrations import upgrade_database

        upgrade_database(pg_blank_engine)

        tables = set(inspect(pg_blank_engine).get_table_names())
        assert EXPECTED_TABLES <= tables
        assert "alembic_version" in tables

    def test_blocked_migration_fails_loudly_instead_of_hanging(self, alembic_config):
        from sqlalchemy import create_engine

        from server.migrations import upgrade_database

        _, url = alembic_config

        engine = create_engine(url)
        blocker = engine.connect()
        try:
            # An open transaction the migration has to wait for: here one that's creating a table it creates too
            blocker.execute(text("CREATE TABLE vehicle (id int)"))

            with pytest.raises(RuntimeError, match="pg_blocking_pids"):
                upgrade_database(engine, lock_timeout="500ms")
        finally:
            blocker.rollback()
            blocker.close()
            engine.dispose()

    def test_startup_upgrade_preserves_application_logging(self, pg_blank_engine):
        # fileConfig(alembic.ini) with default arguments disables every
        # already-created logger, which silenced the whole server after
        # startup migrations.
        import logging

        from server.migrations import upgrade_database

        probe = logging.getLogger("sunbeam.server")
        assert not probe.disabled

        upgrade_database(pg_blank_engine)

        assert not probe.disabled

    def test_startup_upgrade_is_idempotent(self, pg_blank_engine):
        from server.migrations import upgrade_database

        upgrade_database(pg_blank_engine)
        upgrade_database(pg_blank_engine)

        tables = set(inspect(pg_blank_engine).get_table_names())
        assert EXPECTED_TABLES <= tables
