import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

import requests
from sqlalchemy import Engine, text

from config.context import Context
from db.telemetrydb.connection_check import check_influx

logger = logging.getLogger("sunbeam.worker.preflight")


@dataclass
class PreflightResult:
    name: str
    ok: bool
    detail: str


def _check_sunbeam_db(engine: Engine) -> PreflightResult:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return PreflightResult("SunbeamDB", True, "connected")
    except Exception as exc:
        return PreflightResult("SunbeamDB", False, str(exc))


def _check_telemetry_db(window: tuple[datetime, datetime] | None) -> PreflightResult:
    try:
        import influxdb_client  # noqa: F401
    except ImportError:
        # Not every pipeline edition depends on the influxdb-client extra
        # (see pyproject.toml); a missing package here just means this
        # edition's image was never meant to talk to InfluxDB.
        return PreflightResult(
            "InfluxDB", True,
            "skipped: influxdb-client is not installed in this image",
        )

    try:
        telemetry_db = Context().telemetry_db
    except RuntimeError as exc:
        return PreflightResult("InfluxDB", False, str(exc))

    result = check_influx(
        telemetry_db.database_url,
        telemetry_db.organization,
        telemetry_db.bucket,
        telemetry_db.token,
        window,
    )
    detail = result.detail
    if result.ok and window is None:
        detail += " (event has not started; no data to check yet)"
    return PreflightResult("InfluxDB", result.ok, detail)


def _check_server() -> PreflightResult:
    base_url = Context().sunbeam_server.build_url()

    try:
        response = requests.get(f"{base_url}/health", timeout=3)
        response.raise_for_status()
        return PreflightResult("server", True, "reachable")
    except Exception as exc:
        return PreflightResult("server", False, str(exc))


def run_preflight_checks(
    engine: Engine,
    *,
    check_server: bool,
    window: tuple[datetime, datetime] | None = None,
) -> list[PreflightResult]:
    """
    :param window: The span of the event that should already have telemetry (see
        db.telemetrydb.connection_check.event_window); InfluxDB must have data in it
    """
    checks: list[Callable[[], PreflightResult]] = [
        lambda: _check_sunbeam_db(engine),
        lambda: _check_telemetry_db(window),
    ]

    if check_server:
        checks.append(_check_server)

    results = []
    for check in checks:
        result = check()
        if result.ok:
            logger.info("Preflight %s: OK (%s)", result.name, result.detail)
        else:
            logger.error("Preflight %s: FAILED - %s", result.name, result.detail)
        results.append(result)

    return results
