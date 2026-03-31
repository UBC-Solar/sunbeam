import json
import sys
import time
from collections import deque
from datetime import datetime, timezone
from statistics import quantiles
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "telemetry.latest"

def parse_time(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def redraw(payload, poll_ms, avg_poll_ms, lat_ms, avg_lat, max_lat, p95_lat):
    sys.stdout.write("\033[H\033[J")
    print("=== Kafka Latency Test ===")

    print(f"Poll time:        {poll_ms:.2f} ms")
    print(f"Avg poll (max):   {avg_poll_ms:.2f} ms\n")

    if payload is None:
        print("No message yet.")
        sys.stdout.flush()
        return

    print(f"Instant latency:  {lat_ms:.2f} ms")
    print(f"Avg latency:      {avg_lat:.2f} ms")
    print(f"Max latency:      {max_lat:.2f} ms")
    print(f"P95 latency:      {p95_lat:.2f} ms\n")

    values = payload.get("values", {})
    print(f"produced_at: {payload.get('produced_at')}")
    print(f"influx_query_ms: {payload.get('influx_query_ms'):.2f}")
    print()

    for field in ("TotalPackVoltage", "PackCurrent", "VehicleVelocity"):
        item = values.get(field)
        if item is None:
            print(f"{field}: None")
        else:
            print(f"{field}: {item.get('value')} @ {item.get('time')}")

    sys.stdout.flush()


consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "manual-latest-reader",
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
})

consumer.subscribe([TOPIC])

latest_payload = None
poll_times_ms = deque(maxlen=50)
latencies_ms = deque(maxlen=200)  # slightly larger window

try:
    while True:
        t0 = time.perf_counter()
        msg = consumer.poll(0.001)
        poll_ms = (time.perf_counter() - t0) * 1000.0

        poll_times_ms.append(poll_ms)
        avg_poll_ms = max(poll_times_ms)  # your "worst-case" metric

        if msg is not None and not msg.error():
            latest_payload = json.loads(msg.value().decode("utf-8"))

            # 🔥 compute latency
            try:
                produced_at = parse_time(latest_payload["produced_at"])
                now = datetime.now(timezone.utc)
                latency_ms = (now - produced_at).total_seconds() * 1000.0

                latencies_ms.append(latency_ms)

            except Exception:
                latency_ms = None
        else:
            latency_ms = None

        # compute stats safely
        if latencies_ms:
            avg_lat = sum(latencies_ms) / len(latencies_ms)
            max_lat = max(latencies_ms)

            if len(latencies_ms) >= 5:
                p95_lat = quantiles(latencies_ms, n=20)[-1]  # 95th percentile
            else:
                p95_lat = avg_lat
        else:
            avg_lat = max_lat = p95_lat = 0.0

        redraw(
            latest_payload,
            poll_ms,
            avg_poll_ms,
            latency_ms if latency_ms is not None else 0.0,
            avg_lat,
            max_lat,
            p95_lat,
        )

except KeyboardInterrupt:
    pass
finally:
    consumer.close()