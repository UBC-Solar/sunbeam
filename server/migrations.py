import logging
import pathlib

from sqlalchemy import Engine, inspect

logger = logging.getLogger("sunbeam.server")

REPO_ROOT = pathlib.Path(__file__).parent.parent

# The first migration: a frozen copy of the schema that create_schema used to
# build. Pre-Alembic databases are, by definition, at exactly this revision.
BASELINE_REVISION = "f3f709edb0e7"


def upgrade_database(engine: Engine) -> None:
    """
    Bring the database to the newest Alembic revision.

    Transition shim: a database created before Alembic (via create_schema)
    already has the baseline schema but no alembic_version table. Stamp it at
    the baseline first, then upgrade normally. Once every deployment has been
    stamped, the shim never triggers again and can eventually be deleted.
    """
    # Imported lazily: alembic ships with the broker extra, and this module
    # must stay importable in worker environments that do not install it.
    from alembic import command
    from alembic.config import Config

    config = Config(str(REPO_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    config.attributes["sunbeam_database_url"] = engine.url.render_as_string(
        hide_password=False
    )

    tables = set(inspect(engine).get_table_names())

    if "alembic_version" not in tables and "event" in tables:
        logger.info(
            "Pre-Alembic schema detected; stamping baseline revision %s.",
            BASELINE_REVISION,
        )
        command.stamp(config, BASELINE_REVISION)

    logger.info("Running database migrations (alembic upgrade head).")
    command.upgrade(config, "head")