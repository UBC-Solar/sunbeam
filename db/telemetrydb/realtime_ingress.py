from data_tools.localization import TemporalLocalization
from datetime import datetime, timedelta, timezone
from influxdb_client import InfluxDBClient
from typing import Any, Protocol
from typing import Iterable


class TimeProvider(Protocol):
    def now(self, tz: timezone) -> datetime: ...


class DebugTimeProvider:
    """
    Assistant class to debug real-time ingress by producing a now() function which does not return the current time
    but pretending to return the current time in the past by computing the time since the class instantiation.

    Concretely, if the class is instantiated at time ``t1``  and provided the start time ``t0``,

        DebugTimeProvider.now() => t0 + (system.now() - t1)

    where system.now() is the genuine, real time. Note that `system.now() - t1` computes the time since instantiation.

    Basically, the class pretends that it was instantiated at ``start_time`` and counts from then to give
    the "current" time.
    """
    def __init__(self, start_time: datetime):
        self._start_time = datetime.now(timezone.utc)
        self._query_start_time = start_time

    def now(self, tz: timezone) -> datetime:
        now = datetime.now(tz)
        time_elapsed = now - self._start_time
        timestamp = self._query_start_time + time_elapsed

        return timestamp


class RealtimeIngress:
    """
    Fast path for:
      'get the last value of these three fields before timestamp T'

    Notes:
    - Reuses one InfluxDBClient and one QueryApi.
    - Uses one Flux query for all fields.
    - Uses query_stream() to avoid heavier table/dataframe materialization.
    """

    def __init__(
            self,
            bucket: str = "CAN_log",
            organization: str = "8a0b66d77a331e96",
            url: str = "http://influxdb.telemetry.ubcsolar.com",
            token: str = "s4Z9_S6_O09kDzYn1KZcs7LVoCA2cVK9_ObY44vR4xMh-wYLSWBkypS0S0ZHQgBvEV2A5LgvQ1IKr8byHes2LA==",
            timeout_s: float = 1.0,
            fields: Iterable[str] = None,
            time_provider: TimeProvider = datetime
    ):
        self._bucket = bucket
        self._organization = organization
        self._url = url
        self._token = token
        self._fields = fields

        self._client = InfluxDBClient(
            url=self._url,
            token=self._token,
            org=self._organization,
            timeout=timeout_s * 1000,
            enable_gzip=False,
        )
        self._query_api = self._client.query_api()

        self.now = lambda: time_provider.now(timezone.utc)
        self._timezone_fix = TemporalLocalization.localize(self.now())

    @staticmethod
    def _flux_time(dt: datetime) -> str:
        if dt.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _get_fields_filter(self):
        field_filter = " or ".join(
            f'r["_field"] == "{_field}"' for _field in self._fields
        )
        return field_filter

    def _build_last_value_query(self, stop_time: datetime):
        stop_flux = self._flux_time(stop_time - self._timezone_fix)
        start_time = self._flux_time(stop_time - self._timezone_fix - timedelta(seconds=10))

        field_filter = self._get_fields_filter()

        return f'''
                    from(bucket: "{self._bucket}")
                      |> range(start: {start_time}, stop: {stop_flux})
                      |> filter(fn: (r) => {field_filter})
                      |> last()
                      |> keep(columns: ["_field", "_value", "_time"])
                    '''.strip()

    def _process_query(self, query: str):
        _out: dict[str, dict[str, Any] | None] = {
            _field: None for _field in self._fields
        }

        try:
            stream = self._query_api.query_stream(query=query, org=self._organization)

            if stream is None:
                return _out  # nothing returned at all

            for record in stream:
                if record is None:
                    continue

                _field = record.get_field()
                if _field in _out:
                    _out[_field] = {
                        "time": record.get_time() + self._timezone_fix,
                        "value": record.get_value(),
                    }

        except Exception:
            return _out

        return _out

    def get_last_values(self) -> dict[str, dict[str, Any] | None]:
        query = self._build_last_value_query(self.now())
        out = self._process_query(query)
        return out

    def close(self) -> None:
        self._client.close()


if __name__ == "__main__":
    from datetime import datetime, timezone
    import sys
    import time

    reader = RealtimeIngress(bucket="CAN_log",
            organization="8a0b66d77a331e96",
            url="http://influxdb.telemetry.ubcsolar.com",
            token="s4Z9_S6_O09kDzYn1KZcs7LVoCA2cVK9_ObY44vR4xMh-wYLSWBkypS0S0ZHQgBvEV2A5LgvQ1IKr8byHes2LA==",
            fields=(
                "TotalPackVoltage",
                "PackCurrent",
                "VehicleVelocity",
                "AcceleratorPosition",
                "MechBrakePressed",
                "BatteryCurrent",
                "BatteryVoltage",
            ),
            time_provider=DebugTimeProvider(
                start_time=datetime(2024, 7, 16, 14, 10, tzinfo=timezone.utc)
            )
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