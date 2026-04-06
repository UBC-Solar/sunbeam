#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


@dataclass
class Config:
    src_url: str
    src_token: str
    src_org: str
    src_bucket: str

    dst_url: str
    dst_token: str
    dst_org: str
    dst_bucket: str

    start: datetime
    stop: datetime
    batch_window: timedelta
    write_batch_size: int
    measurement_filter: str | None


def parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"Timestamp must be timezone-aware: {value}")
    return dt.astimezone(timezone.utc)


def quote_flux_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def build_flux_query(
    bucket: str,
    start: datetime,
    stop: datetime,
    measurement_filter: str | None = None,
) -> str:
    start_s = start.isoformat().replace("+00:00", "Z")
    stop_s = stop.isoformat().replace("+00:00", "Z")

    filters = []
    if measurement_filter:
        filters.append(
            f'  |> filter(fn: (r) => r["_measurement"] == {quote_flux_string(measurement_filter)})'
        )

    filters_str = "\n".join(filters)

    return f"""
from(bucket: {quote_flux_string(bucket)})
  |> range(start: {start_s}, stop: {stop_s})
{filters_str}
""".strip()


def record_to_point(record) -> Point | None:
    measurement = record.get_measurement()
    field = record.get_field()
    value = record.get_value()
    timestamp = record.get_time()

    if measurement is None or field is None or timestamp is None:
        return None

    point = Point(measurement)

    values = record.values
    for key, val in values.items():
        if key.startswith("_"):
            continue
        if key in ("result", "table"):
            continue
        if val is None:
            continue
        point.tag(key, str(val))

    if isinstance(value, bool):
        point.field(field, value)
    elif isinstance(value, int):
        point.field(field, value)
    elif isinstance(value, float):
        if math.isfinite(value):
            point.field(field, value)
        else:
            return None
    elif isinstance(value, str):
        point.field(field, value)
    else:
        point.field(field, str(value))

    point.time(timestamp, WritePrecision.NS)
    return point


def chunked(iterable: Iterable[Point], size: int) -> Iterable[list[Point]]:
    buf: list[Point] = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def migrate_window(
    src_client: InfluxDBClient,
    dst_client: InfluxDBClient,
    cfg: Config,
    window_start: datetime,
    window_stop: datetime,
) -> int:
    query_api = src_client.query_api()
    write_api = dst_client.write_api(write_options=SYNCHRONOUS)

    query = build_flux_query(
        bucket=cfg.src_bucket,
        start=window_start,
        stop=window_stop,
        measurement_filter=cfg.measurement_filter,
    )

    stream = query_api.query_stream(query=query, org=cfg.src_org)

    def points_iter() -> Iterable[Point]:
        for record in stream:
            if record is None:
                continue
            point = record_to_point(record)
            if point is not None:
                yield point

    written = 0
    for batch in chunked(points_iter(), cfg.write_batch_size):
        write_api.write(
            bucket=cfg.dst_bucket,
            org=cfg.dst_org,
            record=batch,
        )
        written += len(batch)

    return written


def main() -> int:
    # parser = argparse.ArgumentParser(
    #     description="Copy data from an InfluxDB v2 bucket to an InfluxDB v3 bucket."
    # )
    # parser.add_argument("--src-url", required=True)
    # parser.add_argument("--src-token", required=True)
    # parser.add_argument("--src-org", required=True)
    # parser.add_argument("--src-bucket", default="CAN_log")
    #
    # parser.add_argument("--dst-url", required=True)
    # parser.add_argument("--dst-token", required=True)
    # parser.add_argument("--dst-org", required=True)
    # parser.add_argument("--dst-bucket", default="CAN_log")
    #
    # parser.add_argument("--start", required=True, help="Inclusive ISO-8601 UTC start")
    # parser.add_argument("--stop", required=True, help="Exclusive ISO-8601 UTC stop")
    # parser.add_argument(
    #     "--batch-window-minutes",
    #     type=int,
    #     default=60,
    #     help="How much time to query from v2 per batch window",
    # )
    # parser.add_argument(
    #     "--write-batch-size",
    #     type=int,
    #     default=5000,
    #     help="How many points to send per write request to v3",
    # )
    # parser.add_argument(
    #     "--measurement",
    #     default=None,
    #     help="Optional _measurement filter to copy only one measurement",
    # )
    #
    # args = parser.parse_args()

    cfg = Config(
        src_url="http://influxdb.telemetry.ubcsolar.com",
        src_token="s4Z9_S6_O09kDzYn1KZcs7LVoCA2cVK9_ObY44vR4xMh-wYLSWBkypS0S0ZHQgBvEV2A5LgvQ1IKr8byHes2LA==",
        src_org="8a0b66d77a331e96",
        src_bucket="CAN_log",
        dst_url="http://localhost:8181",
        dst_token="blob",
        dst_org="idek",
        dst_bucket="test",
        start=datetime(2024, 7, 16, 10, 00, tzinfo=timezone.utc),
        stop=datetime(2024, 7, 16, 11, 00, tzinfo=timezone.utc),
        batch_window=timedelta(minutes=10),
        write_batch_size=1000,
        measurement_filter="ECU",
    )

    if cfg.stop <= cfg.start:
        raise ValueError("--stop must be after --start")

    src_client = InfluxDBClient(
        url=cfg.src_url,
        token=cfg.src_token,
        org=cfg.src_org,
        timeout=60_000,
        enable_gzip=True,
    )
    dst_client = InfluxDBClient(
        url=cfg.dst_url,
        token=cfg.dst_token,
        org=cfg.dst_org,
        timeout=60_000,
        enable_gzip=True,
    )

    total_written = 0
    t0 = time.perf_counter()

    try:
        window_start = cfg.start
        while window_start < cfg.stop:
            window_stop = min(window_start + cfg.batch_window, cfg.stop)
            print(
                f"Migrating {window_start.isoformat()} -> {window_stop.isoformat()} ...",
                file=sys.stderr,
            )

            written = migrate_window(
                src_client=src_client,
                dst_client=dst_client,
                cfg=cfg,
                window_start=window_start,
                window_stop=window_stop,
            )
            total_written += written

            print(
                f"  wrote {written} points (total {total_written})",
                file=sys.stderr,
            )

            window_start = window_stop

    finally:
        src_client.close()
        dst_client.close()

    elapsed = time.perf_counter() - t0
    print(
        f"Done. Wrote {total_written} points in {elapsed:.1f}s.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())