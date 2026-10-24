"""
Keeps the Alembic migration chain honest: upgrading a fresh database to head
must produce the full production schema (including hypertables), and the chain
must be able to walk back down.
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
    "raw_sample",
    "aligned_sample",
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
        assert {"raw_sample", "aligned_sample"} <= hypertables

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


class TestServerStartupUpgrade:
    """server.migrations.upgrade_database — what on_startup now runs."""

    def test_fresh_database_gets_full_schema(self, pg_blank_engine):
        from server.migrations import upgrade_database

        upgrade_database(pg_blank_engine)

        tables = set(inspect(pg_blank_engine).get_table_names())
        assert EXPECTED_TABLES <= tables
        assert "alembic_version" in tables

    def test_pre_alembic_database_is_stamped_then_migrated(self, alembic_config):
        # Fabricate a genuine legacy database: schema exactly at the baseline
        # revision, with no alembic_version bookkeeping - what a deployment
        # created by the old create_schema() looks like.
        from alembic.script import ScriptDirectory
        from sqlalchemy import create_engine

        from server.migrations import BASELINE_REVISION, upgrade_database

        config, url = alembic_config
        command.upgrade(config, BASELINE_REVISION)

        engine = create_engine(url)
        try:
            with engine.begin() as conn:
                conn.execute(text("DROP TABLE alembic_version"))
                conn.execute(
                    text(
                        "INSERT INTO vehicle (name, description) "
                        "VALUES ('Brightside', 'pre-existing')"
                    )
                )

            # Must not raise (a blind `upgrade head` would fail on the
            # existing tables), must keep existing data, and must carry the
            # database through every migration after the baseline.
            upgrade_database(engine)

            with engine.connect() as conn:
                version = conn.scalar(text("SELECT version_num FROM alembic_version"))
                vehicles = conn.scalar(text("SELECT count(*) FROM vehicle"))
                kind_column = conn.scalar(
                    text(
                        "SELECT count(*) FROM information_schema.columns "
                        "WHERE table_name='worker_run' AND column_name='kind'"
                    )
                )
        finally:
            engine.dispose()

        head = ScriptDirectory.from_config(config).get_current_head()
        assert version == head
        assert vehicles == 1
        assert kind_column == 1

    def test_blocked_migration_fails_loudly_instead_of_hanging(self, alembic_config):
        from sqlalchemy import create_engine

        from server.migrations import BASELINE_REVISION, upgrade_database

        config, url = alembic_config
        command.upgrade(config, BASELINE_REVISION)

        engine = create_engine(url)
        blocker = engine.connect()
        try:
            # An open transaction holding the table lock - the shape of an
            # 'idle in transaction' session sitting on worker_run.
            blocker.execute(text("LOCK TABLE worker_run IN ACCESS EXCLUSIVE MODE"))

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
