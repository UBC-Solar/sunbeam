import logging
import time
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta, timezone
from typing import Any

from data_tools.query import InfluxDBClient  # type: ignore

from db.telemetrydb.protocols import TimeProvider

logger = logging.getLogger("sunbeam.worker")

# How far back to look for each field's latest value
LOOKBACK = timedelta(seconds=10)

# InfluxDB rejected the request itself (bad token, unknown org or bucket): retrying won't help
FATAL_STATUSES = {400, 401, 403, 404}

# How often to repeat the warning while polls keep failing
FAILURE_LOG_INTERVAL_S = 30.0


class RealtimeIngressQuerier:
    """
    Polls the latest value of each field, with one Flux query for all of them (InfluxDBClient.query_last_values,
    which streams its results and corrects InfluxDB's timezone errors). Reuses one InfluxDBClient.
    """

    def __init__(
            self,
            bucket: str | None = None,
            organization: str | None = None,
            url: str | None = None,
            token: str | None = None,
            timeout_s: float = 1.0,
            fields: Iterable[str] | None = None,
            time_provider: TimeProvider = datetime,  # type: ignore[assignment]
            monotonic: Callable[[], float] = time.monotonic,
    ):
        self._bucket = bucket
        self._fields = tuple(fields) if fields is not None else ()
        self._monotonic = monotonic
        self._last_failure_log: float | None = None

        self._client = InfluxDBClient(
            url=url,
            influxdb_token=token,
            influxdb_org=organization,
            timeout=timeout_s,
        )

        self.now = lambda: time_provider.now(timezone.utc)

    def get_last_values(self) -> dict[str, dict[str, Any] | None]:
        """ The latest value of each field in the LOOKBACK before now; None for any without one.

        A poll that fails (timeout, connection error) is a poll with no data: the ingress stage tracks how long
        that lasts. One InfluxDB rejects outright (e.g. a bad token) raises, since it will never succeed.
        """
        stop_time = self.now()
        try:
            latest = self._client.query_last_values(self._bucket, self._fields, stop_time - LOOKBACK, stop_time)
        except Exception as exc:
            if getattr(exc, "status", None) in FATAL_STATUSES:
                raise
            self._log_failure(exc)
            return {_field: None for _field in self._fields}

        self._last_failure_log = None
        return {
            _field: None if point is None else {"time": point[0], "value": point[1]}
            for _field, point in latest.items()
        }

    def _log_failure(self, exc: Exception) -> None:
        now = self._monotonic()
        if self._last_failure_log is None or now - self._last_failure_log >= FAILURE_LOG_INTERVAL_S:
            self._last_failure_log = now
            logger.warning("InfluxDB poll for %s failed: %s", list(self._fields), exc)

    def close(self) -> None:
        self._client.close()
