"""
Fixtures for tests that need a real Postgres (marked `postgres`, excluded from
the default run). A single TimescaleDB container serves the whole session;
every test gets its own freshly created database with the real production
schema (create_schema, including hypertables).
"""
import itertools

import pytest

testcontainers_postgres = pytest.importorskip("testcontainers.postgres")

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from db import create_schema
from tests.infrastructure.conftest import SeededEvent, seed_basic_event

POSTGRES_IMAGE = "timescale/timescaledb:2.14.2-pg16"

_db_counter = itertools.count()


@pytest.fixture(scope="session")
def pg_container():
    container = testcontainers_postgres.PostgresContainer(
        POSTGRES_IMAGE,
        driver="psycopg",
    )
    with container as running:
        yield running


@pytest.fixture(scope="session")
def pg_admin_engine(pg_container):
    engine = create_engine(
        pg_container.get_connection_url(),
        isolation_level="AUTOCOMMIT",
    )
    yield engine
    engine.dispose()


@pytest.fixture
def pg_blank_engine(pg_container, pg_admin_engine):
    """A brand-new database with no schema at all."""
    db_name = f"sunbeam_test_{next(_db_counter)}"

    with pg_admin_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE {db_name}"))

    base_url = pg_container.get_connection_url()
    engine = create_engine(base_url.rsplit("/", 1)[0] + f"/{db_name}")

    yield engine

    engine.dispose()
    # The container is thrown away at session end; no need to drop databases.


@pytest.fixture
def pg_engine(pg_blank_engine):
    """A brand-new database with the full production schema."""
    create_schema(pg_blank_engine)
    return pg_blank_engine


@pytest.fixture
def pg_session_factory(pg_engine):
    return sessionmaker(bind=pg_engine, autoflush=False, autocommit=False)


@pytest.fixture
def pg_seeded_event(pg_engine) -> SeededEvent:
    return seed_basic_event(pg_engine)
