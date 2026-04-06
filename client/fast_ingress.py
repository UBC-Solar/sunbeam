from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any

from influxdb_client import InfluxDBClient
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import time
import os
import json


def json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class FastLastValueReader:
    """
    Fast path for:
      'get the last value of these three fields before timestamp T'

    Notes:
    - Reuses one InfluxDBClient and one QueryApi.
    - Uses one Flux query for all fields.
    - Uses query_stream() to avoid heavier table/dataframe materialization.
    """

    def __init__(self) -> None:
        self.bucket = "CAN_log"
        self.org = "8a0b66d77a331e96"

        self.client = InfluxDBClient(
            url="http://influxdb.telemetry.ubcsolar.com",
            token="s4Z9_S6_O09kDzYn1KZcs7LVoCA2cVK9_ObY44vR4xMh-wYLSWBkypS0S0ZHQgBvEV2A5LgvQ1IKr8byHes2LA==",
            org=self.org,
            timeout=10000,
            # keep this False for tiny result sets; compression usually helps
            # larger payloads more than tiny "3 records" queries
            enable_gzip=False,
        )
        self.query_api = self.client.query_api()

        self.fields = (
            "TotalPackVoltage",
            "PackCurrent",
            "VehicleVelocity",
            "AcceleratorPosition",
            "MechBrakePressed",
            "BatteryCurrent",
            "BatteryVoltage",
        )

        self.start_time = datetime.now(timezone.utc)
        self.query_start_time = datetime(2024, 7, 16, 10, 00, tzinfo=timezone.utc)


    @staticmethod
    def _flux_time(dt: datetime) -> str:
        if dt.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _build_query(self, stop_time: datetime, lookback: str) -> str:
        stop_flux = self._flux_time(stop_time)
        start_time = self._flux_time(stop_time - timedelta(seconds=1))

        field_filter = " or ".join(
            f'r["_field"] == "{field}"' for field in self.fields
        )

        return f'''
                    from(bucket: "{self.bucket}")
                      |> range(start: {start_time}, stop: {stop_flux})
                      |> filter(fn: (r) => {field_filter})
                      |> last()
                      |> keep(columns: ["_field", "_value", "_time"])
                    '''.strip()

    def get_last_values_before(
            self,
            *,
            lookback: str = "5m",
    ) -> dict[str, dict[str, Any] | None]:

        now = datetime.now(timezone.utc)
        time_elapsed = now - self.start_time
        timestamp = self.query_start_time + time_elapsed

        query = self._build_query(timestamp, lookback)

        out: dict[str, dict[str, Any] | None] = {
            field: None for field in self.fields
        }

        try:
            stream = self.query_api.query_stream(query=query, org=self.org)

            if stream is None:
                return out  # nothing returned at all

            for record in stream:
                if record is None:
                    continue

                field = record.get_field()
                if field in out:
                    out[field] = {
                        "time": record.get_time(),
                        "value": record.get_value(),
                    }

        except Exception:
            # swallow ALL query errors (timeouts, empty responses, etc.)
            # optionally log if you want visibility:
            # print(f"Influx query failed: {e}")
            return out

        return out

    def close(self) -> None:
        self.client.close()


if __name__ == "__main__":
    from datetime import datetime, timezone
    import sys
    import time

    reader = FastLastValueReader()

    try:
        while True:
            loop_start = time.perf_counter()

            values = reader.get_last_values_before(lookback="30s")

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
            time.sleep(max(0, 0.05 - elapsed))

    except KeyboardInterrupt:
        reader.close()
        print("\nStopped.")