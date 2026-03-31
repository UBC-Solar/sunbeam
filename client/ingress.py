from data_tools.query import InfluxDBClient, FluxStatement, FluxQuery
from datetime import datetime, timezone


class Subscriber:
    def __init__(self, start_time: datetime):
        assert start_time.tzinfo is not None
        self.start_time = start_time
        self._actual_start_time = None
        self.running = False
        self._last_time = None
        self.results = [None, None, None]
        self.timings = [None, None, None]

        self.client = InfluxDBClient()

    def begin(self):
        self.running = True
        self._actual_start_time = datetime.now(tz=timezone.utc)
        self._last_time = self.start_time

    def stop(self):
        self.running = False

    def query(self):
        if not self.running:
            raise RuntimeError("Subscriber not running!")

        fields = ["TotalPackVoltage", "PackCurrent", "VehicleVelocity"]

        time_elapsed = datetime.now(tz=timezone.utc) - self._actual_start_time
        current_time = self.start_time + time_elapsed
        last_time = self._last_time
        total_start = time.perf_counter()

        for i, field in enumerate(fields):
            try:
                t0 = time.perf_counter()
                results[i] = self.client.query_time_series(last_time, current_time, field)[0]
                t1 = time.perf_counter()

                self.results[i] = results[i]
                self.timings[i] = t1 - t0
            except ValueError as e:
                continue

        self._last_time = current_time
        total_time = time.perf_counter() - total_start

        return results, total_time


if __name__ == '__main__':
    import time
    import sys
    from datetime import datetime, timezone

    results = [None, None, None]

    # assume Subscriber is already defined
    sub = Subscriber(start_time=datetime(2024, 7, 16, 18, 00, tzinfo=timezone.utc))
    sub.begin()

    try:
        while True:
            start = time.time()

            _, total_time = sub.query()
            results = sub.results
            timings = sub.timings

            # Move cursor to top-left and clear screen
            sys.stdout.write("\033[H\033[J")

            print("=== Live Telemetry ===")
            print(f"Time: {sub._last_time}\n")

            fields = ["TotalPackVoltage", "PackCurrent", "VehicleVelocity"]

            for field, result in zip(fields, results):
                print(f"{field}: {result}")

            print("=== Query Timing ===")
            for field, t in zip(fields, timings):
                if t is not None:
                    print(f"{field}: {t * 1000:.2f} ms")

            print(f"\nTotal query time: {total_time * 1000:.2f} ms")

            sys.stdout.flush()

            # Maintain ~0.05s interval
            elapsed = time.time() - start
            time.sleep(max(0, 0.05 - elapsed))

    except KeyboardInterrupt:
        sub.stop()
        print("\nStopped.")

