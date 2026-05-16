from __future__ import annotations

import datetime as dt
import heapq
import itertools
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol


class RunnablePipeline(Protocol):
    frequency: float  # Hz

    def run(self, state: Any, timestamp: dt.datetime) -> Iterable[Any]:
        ...


@dataclass(order=True)
class ScheduledRun:
    next_run_ns: int
    tie_breaker: int
    pipeline: RunnablePipeline = field(compare=False)
    period_ns: int = field(compare=False)


class Scheduler:
    def __init__(self, pipelines: Iterable[RunnablePipeline]):
        self._heap: list[ScheduledRun] = []
        self._counter = itertools.count()

        now_mono_ns = time.monotonic_ns()
        now_wall = dt.datetime.now(dt.UTC)

        # Start at the next whole UTC second, i.e. nice 000 ms timestamp.
        self._start_wall_time = (now_wall + dt.timedelta(seconds=1)).replace(
            microsecond=0
        )

        delay_s = (self._start_wall_time - now_wall).total_seconds()
        self._start_monotonic_ns = now_mono_ns + int(delay_s * 1_000_000_000)

        for pipeline in pipelines:
            if pipeline.frequency <= 0:
                raise ValueError(
                    f"Pipeline frequency must be positive: {pipeline.frequency}"
                )

            period_ns = round(1_000_000_000 / pipeline.frequency)

            heapq.heappush(
                self._heap,
                ScheduledRun(
                    next_run_ns=self._start_monotonic_ns,
                    tie_breaker=next(self._counter),
                    pipeline=pipeline,
                    period_ns=period_ns,
                ),
            )

    def _timestamp_from_monotonic_ns(self, scheduled_ns: int) -> dt.datetime:
        offset_ns = scheduled_ns - self._start_monotonic_ns
        return self._start_wall_time + dt.timedelta(microseconds=offset_ns / 1000)

    def run_forever(
        self,
        state: Any,
        on_output: Callable[[RunnablePipeline, Any, dt.datetime], None] | None = None,
        *,
        stop_on_error: bool = True,
    ) -> None:
        while True:
            scheduled = heapq.heappop(self._heap)

            sleep_ns = scheduled.next_run_ns - time.monotonic_ns()
            if sleep_ns > 0:
                time.sleep(sleep_ns / 1_000_000_000)

            # This is the intended/logical pipeline time.
            timestamp = self._timestamp_from_monotonic_ns(scheduled.next_run_ns)

            try:
                for frame in scheduled.pipeline.run(state, timestamp):
                    if on_output is not None:
                        on_output(scheduled.pipeline, frame, timestamp)

            except Exception:
                if stop_on_error:
                    raise

            # Advance by one logical period.
            scheduled.next_run_ns += scheduled.period_ns

            # If execution fell behind, skip missed logical ticks.
            now_ns = time.monotonic_ns()
            while scheduled.next_run_ns <= now_ns:
                scheduled.next_run_ns += scheduled.period_ns

                raise RuntimeError("Missed pipeline run!")

            heapq.heappush(self._heap, scheduled)