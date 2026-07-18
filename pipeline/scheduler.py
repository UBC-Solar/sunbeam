from pipeline.protocols import SchedulerObserver, ScheduledRun, RunnablePipeline
from collections.abc import Callable, Iterable
from typing import Any
import datetime as dt
import itertools
import heapq
import time


class Scheduler:
    def __init__(self, pipelines: Iterable[RunnablePipeline], observer: SchedulerObserver | None = None,):
        self._heap: list[ScheduledRun] = []
        self._counter = itertools.count()
        self._observer = observer

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
        on_tick: Callable[[], None] | None = None,
        *,
        stop_on_error: bool = True,
        should_stop: Callable[[], bool] | None = None,
    ) -> None:
        while not (should_stop and should_stop()):
            scheduled = heapq.heappop(self._heap)

            now_ns = time.monotonic_ns()
            sleep_ns = scheduled.next_run_ns - now_ns

            if sleep_ns > 0:
                time.sleep(sleep_ns / 1_000_000_000)
                if self._observer is not None:
                    self._observer.on_idle(sleep_ns)
                late_ns = 0
            else:
                late_ns = -sleep_ns

            timestamp = self._timestamp_from_monotonic_ns(scheduled.next_run_ns)
            pipeline_name = scheduled.pipeline.name

            if self._observer is not None:
                self._observer.on_pipeline_start(pipeline_name, late_ns)

            run_start_ns = time.monotonic_ns()

            try:
                for frame in scheduled.pipeline.run(state, timestamp):
                    if on_output is not None:
                        write_start_ns = time.monotonic_ns()
                        on_output(scheduled.pipeline, frame, timestamp)
                        write_elapsed_ns = time.monotonic_ns() - write_start_ns

                        if self._observer is not None:
                            self._observer.on_writer_done(write_elapsed_ns)

            except Exception:
                if stop_on_error:
                    raise

            run_elapsed_ns = time.monotonic_ns() - run_start_ns

            if self._observer is not None:
                self._observer.on_pipeline_done(pipeline_name, run_elapsed_ns)

            scheduled.next_run_ns += scheduled.period_ns
            heapq.heappush(self._heap, scheduled)

            if on_tick is not None:
                on_tick()
