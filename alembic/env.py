import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool, text

from db.sunbeamdb.models import Base

config = context.config

# Apply alembic.ini's logging config only for CLI runs. Programmatic callers
# (server startup) set configure_logger=False: fileConfig would otherwise
# disable every already-created logger and mute the application's own logs.
if config.config_file_name is not None and config.attributes.get("configure_logger", True):
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = Base.metadata


def _database_url() -> str:
    """
    Resolution order:
      1. config.attributes["sunbeam_database_url"] (programmatic callers,
         e.g. server startup passing its own engine's URL)
      2. SUNBEAM_DATABASE_URL environment variable (CI, ad-hoc targets)
      3. The broker configuration from context.toml (normal operation)
    """
    url = config.attributes.get("sunbeam_database_url")
    if url:
        return url

    url = os.environ.get("SUNBEAM_DATABASE_URL")
    if url:
        return url

    from config.context import Context, ServiceType

    return (
        Context.load(ServiceType.Broker)
        .sunbeam_db.build_url()
        .render_as_string(hide_password=False)
    )


def run_migrations_offline() -> None:
    """Emit SQL to stdout instead of executing (alembic upgrade --sql)."""
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = create_engine(_database_url(), poolclass=pool.NullPool)

    # Bound how long DDL waits for locks: a migration blocked behind another
    # session (e.g. something idle-in-transaction on the table) should fail
    # loudly, not hang startup forever. lock_timeout only limits lock WAITING,
    # so long-running migrations themselves are unaffected. Overridable via
    # config.attributes for a deliberate slow migration.
    lock_timeout = config.attributes.get("lock_timeout", "10s")

    with connectable.connect() as connection:
        connection.execute(text(f"SET lock_timeout = '{lock_timeout}'"))
        connection.commit()  # end the autobegun tx; the setting is session-scoped

        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
