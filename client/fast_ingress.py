from __future__ import annotations

from datetime import datetime, timezone
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

    @staticmethod
    def _flux_time(dt: datetime) -> str:
        if dt.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _build_query(self, stop_time: datetime, lookback: str) -> str:
        stop_flux = self._flux_time(stop_time)

        field_filter = " or ".join(
            f'r["_field"] == "{field}"' for field in self.fields
        )


        # Important:
        # - One query
        # - Narrow range if possible
        # - last() on the server
        #
        # If you truly need "last value before T no matter how old",
        # use range(start: 0, stop: T) instead of -lookback.
        return f'''
from(bucket: "{self.bucket}")
  |> range(start: 0, stop: {stop_flux})
  |> filter(fn: (r) => {field_filter})
  |> last()
  |> keep(columns: ["_field", "_value", "_time"])
'''.strip()

    def get_last_values_before(
            self,
            timestamp: datetime,
            *,
            lookback: str = "5m",
    ) -> dict[str, dict[str, Any] | None]:

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

class InfluxKafkaPublisher:
    def __init__(self) -> None:
        self.lookback = os.getenv("LOOKBACK", "30s")
        self.topic = os.getenv("KAFKA_TOPIC", "telemetry.latest")
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
        self.poll_interval = float(os.getenv("POLL_INTERVAL_SECONDS", "0.05"))

        self.reader = FastLastValueReader()

        self.admin = AdminClient(
            {"bootstrap.servers": self.bootstrap_servers}
        )
        self.producer = Producer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": "influx-publisher",
                "acks": "1",
                "linger.ms": 0,
            }
        )

        self.start_time = datetime.now(timezone.utc)
        self.query_start_time = datetime(2024, 7, 16, 10, 00, tzinfo=timezone.utc)

    def wait_for_kafka(self, timeout_s: float = 60.0) -> None:
        deadline = time.time() + timeout_s
        last_err: Exception | None = None

        while time.time() < deadline:
            try:
                self.admin.list_topics(timeout=2)
                return
            except Exception as e:
                last_err = e
                time.sleep(1.0)

        raise RuntimeError(f"Kafka did not become ready: {last_err}")

    def ensure_topic(self) -> None:
        try:
            futures = self.admin.create_topics(
                [NewTopic(self.topic, num_partitions=1, replication_factor=1)]
            )
            futures[self.topic].result(timeout=10)
        except Exception as e:
            # fine if topic already exists
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                try:
                    md = self.admin.list_topics(timeout=5)
                    if self.topic in md.topics:
                        return
                except Exception:
                    pass
                raise

    def publish_once(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        time_elapsed = now - self.start_time
        query_time = self.query_start_time + time_elapsed

        t0 = time.perf_counter()
        values = self.reader.get_last_values_before(query_time, lookback=self.lookback)
        influx_query_ms = (time.perf_counter() - t0) * 1000.0

        now = datetime.now(timezone.utc)
        payload = {
            "produced_at": now,
            "influx_query_ms": influx_query_ms,
            "values": values,
        }

        self.producer.produce(
            self.topic,
            key=b"snapshot",
            value=json.dumps(payload, default=json_default).encode("utf-8"),
        )
        self.producer.poll(0)

        return payload

    def run(self) -> None:
        self.wait_for_kafka()
        self.ensure_topic()

        while True:
            loop_start = time.perf_counter()

            try:
                self.publish_once()
            except Exception as e:
                print(f"publish error: {e}", flush=True)

            elapsed = time.perf_counter() - loop_start
            time.sleep(max(0.0, self.poll_interval - elapsed))


if __name__ == "__main__":
    InfluxKafkaPublisher().run()

# if __name__ == "__main__":
#     from datetime import datetime, timezone
#     import sys
#     import time
#
#     reader = FastLastValueReader()
#
#     start_time = datetime.now(timezone.utc)
#     query_start_time = datetime(2024, 7, 16, 10, 00, tzinfo=timezone.utc)
#
#     try:
#         while True:
#             loop_start = time.perf_counter()
#
#             now = datetime.now(timezone.utc)
#             time_elapsed = now - start_time
#             query_time = query_start_time + time_elapsed
#             values = reader.get_last_values_before(query_time, lookback="30s")
#
#             total_time_ms = (time.perf_counter() - loop_start) * 1000
#
#             # Clear screen and move cursor to top
#             sys.stdout.write("\033[H\033[J")
#
#             print("=== Live Telemetry (Last Values) ===")
#             print(f"Query time: {total_time_ms:.2f} ms\n")
#
#             for field, data in values.items():
#                 if data is None:
#                     print(f"{field}: None")
#                 else:
#                     print(f"{field}: {data['value']} @ {data['time']}")
#
#             sys.stdout.flush()
#
#             # Maintain ~0.05s loop
#             elapsed = time.perf_counter() - loop_start
#             time.sleep(max(0, 0.05 - elapsed))
#
#     except KeyboardInterrupt:
#         reader.close()
#         print("\nStopped.")