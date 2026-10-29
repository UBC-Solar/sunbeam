from data_tools.query import InfluxDBClient  # type: ignore
from db.telemetrydb.protocols import TimeProvider
from datetime import datetime, timedelta, timezone
from typing import Any
from typing import Iterable

# How far back to look for each field's latest value
LOOKBACK = timedelta(seconds=10)


class RealtimeIngressQuerier:
    """
    Fast path for:
      'get the last value of these three fields before timestamp T'

    Notes:
    - Reuses one InfluxDBClient.
    - Uses one Flux query for all fields (InfluxDBClient.query_last_values), which streams its results rather
      than materializing tables or dataframes, and corrects InfluxDB's timezone errors (temporal localization).
    """

    def __init__(
            self,
            bucket: str | None = None,
            organization: str | None = None,
            url: str | None = None,
            token: str | None = None,
            timeout_s: float = 1.0,
            fields: Iterable[str] | None = None,
            time_provider: TimeProvider = datetime
    ):
        self._bucket = bucket
        self._organization = organization
        self._url = url
        self._token = token
        self._fields = tuple(fields) if fields is not None else ()

        self._client = InfluxDBClient(
            url=self._url,
            influxdb_token=self._token,
            influxdb_org=self._organization,
            timeout=timeout_s,
        )

        self.now = lambda: time_provider.now(timezone.utc)

    def _query_last_values(self, stop_time: datetime) -> dict[str, dict[str, Any] | None]:
        """ The latest value of each field in the LOOKBACK before ``stop_time``; None for any without one.

        :param datetime stop_time: Stop time for the query
        :return dict[str, dict[str, Any] | None]: List of fields alongside their values and time
        """
        try:
            latest = self._client.query_last_values(self._bucket, self._fields, stop_time - LOOKBACK, stop_time)
        except Exception:
            # A failed poll is a poll with no data; the ingress stage tracks how long that lasts
            return {_field: None for _field in self._fields}

        return {
            _field: None if point is None else {"time": point[0], "value": point[1]}
            for _field, point in latest.items()
        }

    def get_last_values(self) -> dict[str, dict[str, Any] | None]:
        """ Returns the latest values from InfluxDB

        :return dict[str, dict[str, Any] | None]: List of fields alongside their values and time
        """        
        return self._query_last_values(self.now())

    def close(self) -> None:
        self._client.close()


if __name__ == "__main__":
    import sys
    import time

    from config.context import Context

    telemetry_db = Context().telemetry_db
    reader = RealtimeIngressQuerier(
            bucket=telemetry_db.bucket,
            organization=telemetry_db.organization,
            url=telemetry_db.database_url,
            token=telemetry_db.token,
            fields=(
                "TotalPackVoltage",
                "PackCurrent",
                "VehicleVelocity",
                "AcceleratorPosition",
                "MechBrakePressed",
                "BatteryCurrent",
                "BatteryVoltage",
            ),
    )

    try:
        while True:
            loop_start = time.perf_counter()

            values = reader.get_last_values()

            total_time_ms = (time.perf_counter() - loop_start) * 1000

            # Clear screen and move cursor to top
            sys.stdout.write("\033[H\033[J")

            print("=== Live Telemetry (Last Values) ===")
            print(f"Query time: {total_time_ms:.2f} ms\n")

            for field, data in values.items():
                if data is None:
                    print(f"{field}: None")
                else:
                    print(f"{field}: {data['value']} @ {data['time']}")

            sys.stdout.flush()

            # Maintain ~0.05s loop
            elapsed = time.perf_counter() - loop_start
            time.sleep(max(0.0, 0.05 - elapsed))

    except KeyboardInterrupt:
        reader.close()
        print("\nStopped.")