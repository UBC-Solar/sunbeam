import logging
from dataclasses import dataclass
from typing import Callable

import requests
from sqlalchemy import Engine, text

from config.context import Context

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


def _check_telemetry_db() -> PreflightResult:
    try:
        from influxdb_client import InfluxDBClient
    except ImportError:
        # Not every pipeline edition depends on the influxdb-client extra
        # (see pyproject.toml); a missing package here just means this
        # edition's image was never meant to talk to InfluxDB.
        return PreflightResult(
            "InfluxDB", True,
            "skipped: influxdb-client is not installed in this image",
        )

    telemetry_db = Context().telemetry_db

    try:
        client = InfluxDBClient(
            url=telemetry_db.database_url,
            token=telemetry_db.token,
            org=telemetry_db.organization,
            timeout=5_000,
        )
    except Exception as exc:
        return PreflightResult("InfluxDB", False, f"failed to construct client: {exc}")

    try:
        if not client.ping():
            return PreflightResult("InfluxDB", False, "server did not respond to ping")

        bucket = client.buckets_api().find_bucket_by_name(telemetry_db.bucket)
        if bucket is None:
            return PreflightResult(
                "InfluxDB", False,
                f"bucket {telemetry_db.bucket!r} not found "
                "(check the configured token, organization, and bucket name)",
            )

        return PreflightResult(
            "InfluxDB", True, f"connected, bucket {telemetry_db.bucket!r} found",
        )
    except Exception as exc:
        return PreflightResult("InfluxDB", False, str(exc))
    finally:
        client.close()


def _check_server() -> PreflightResult:
    base_url = Context().sunbeam_broker.build_url()

    try:
        response = requests.get(f"{base_url}/health", timeout=3)
        response.raise_for_status()
        return PreflightResult("server", True, "reachable")
    except Exception as exc:
        return PreflightResult("server", False, str(exc))


def run_preflight_checks(engine: Engine, *, check_server: bool) -> list[PreflightResult]:
    checks: list[Callable[[], PreflightResult]] = [lambda: _check_sunbeam_db(engine), _check_telemetry_db]

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