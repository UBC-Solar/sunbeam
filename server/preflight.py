import logging

import docker
from docker.errors import DockerException
from sqlalchemy import Engine, text

logger = logging.getLogger("sunbeam.server.preflight")


def check_postgres(engine: Engine) -> None:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Preflight PostgreSQL: OK")
    except Exception as exc:
        logger.error("Preflight PostgreSQL: FAILED - %s", exc)
        raise SystemExit(1) from exc


def check_docker() -> None:
    try:
        client = docker.from_env()
        try:
            client.ping()
        finally:
            client.close()
        logger.info("Preflight Docker: OK")
    except DockerException as exc:
        logger.error("Preflight Docker: FAILED - %s", exc)
        raise SystemExit(1) from exc