import datetime as dt
import heapq
import itertools
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from typing import Any

from pipeline.protocols import RunnablePipeline, ScheduledRun, SchedulerObserver


class Scheduler(ABC):
    def __init__(
            self,
            pipelines: Iterable[RunnablePipeline],
            observer: SchedulerObserver | None = None,
            *,
            now_wall: dt.datetime | None = None,
            end_wall: dt.datetime | None = None,
            monotonic_ns: Callable[[], int] = time.monotonic_ns,
        ):
        self._heap: list[ScheduledRun] = []
        self._counter = itertools.count()
        self._observer = observer
        self._end_wall = end_wall
        self._monotonic_ns = monotonic_ns

        now_mono_ns = self._monotonic_ns()
        if now_wall is None: # Reassigns now_wall if no start time is provided
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

    def _next_timestamp(self) -> dt.datetime:
        return self._timestamp_from_monotonic_ns(self._heap[0].next_run_ns)

    @abstractmethod
    def _wait_until(self, next_run_ns: int) -> int:
        """ Wait (or not) until a run is due.

        :param int next_run_ns: Monotonic time at which the next run is scheduled
        :return int: How late, in ns, the run is starting
        """
        ...

    def run_once(
        self,
        state: Any,
        on_output: Callable[[RunnablePipeline, Any, dt.datetime], None] | None = None,
        on_tick: Callable[[dt.datetime], None] | None = None,
        *,
        stop_on_error: bool = True,
    ) -> None:
        """ Runs the next scheduled pipeline once.

        :param Any state: State object which holds the values for processing
        :param on_output: Called with every frame the pipeline yields, defaults to None
        :param on_tick: Called after every run with the timestamp just processed, defaults to None
        :param bool stop_on_error: Bool to stop Sunbeam if an error is encountered, defaults to True
        """
        scheduled = heapq.heappop(self._heap)

        late_ns = self._wait_until(scheduled.next_run_ns)

        timestamp = self._timestamp_from_monotonic_ns(scheduled.next_run_ns)
        pipeline_name = scheduled.pipeline.name

        if self._observer is not None:
            self._observer.on_pipeline_start(pipeline_name, late_ns)

        run_start_ns = self._monotonic_ns()

        try:
            for frame in scheduled.pipeline.run(state, timestamp):
                if on_output is not None:
                    write_start_ns = self._monotonic_ns()
                    on_output(scheduled.pipeline, frame, timestamp)
                    write_elapsed_ns = self._monotonic_ns() - write_start_ns

                    if self._observer is not None:
                        self._observer.on_writer_done(write_elapsed_ns)

        except Exception:
            if stop_on_error:
                raise

        run_elapsed_ns = self._monotonic_ns() - run_start_ns

        if self._observer is not None:
            self._observer.on_pipeline_done(pipeline_name, run_elapsed_ns)

        scheduled.next_run_ns += scheduled.period_ns
        heapq.heappush(self._heap, scheduled)

        if on_tick is not None:
            on_tick(timestamp)

    def run(
            self,
            state: Any,
            on_output: Callable[[RunnablePipeline, Any, dt.datetime], None] | None = None,
            on_tick: Callable[[dt.datetime], None] | None = None,
            *,
            stop_on_error: bool = True,
            should_stop: Callable[[], bool] | None = None,
        ) -> None:
        """ Run pipelines until ``should_stop`` returns True, or the next scheduled run falls after ``end_wall``.

        Without either, runs forever.
        """
        while (
            self._heap
            and not (should_stop and should_stop())
            and (self._end_wall is None or self._next_timestamp() <= self._end_wall)
        ):
            self.run_once(state=state, on_output=on_output, on_tick=on_tick, stop_on_error=stop_on_error)


class RealtimeScheduler(Scheduler):
    """ Runs pipelines in step with the wall clock, sleeping between runs. """
    def __init__(
            self,
            pipelines: Iterable[RunnablePipeline],
            observer: SchedulerObserver | None = None,
            *,
            sleep: Callable[[float], None] = time.sleep,
            **kwargs,
        ):
        super().__init__(pipelines, observer, **kwargs)
        self._sleep = sleep

    def _wait_until(self, next_run_ns: int) -> int:
        sleep_ns = next_run_ns - self._monotonic_ns()

        if sleep_ns > 0:
            self._sleep(sleep_ns / 1_000_000_000)
            if self._observer is not None:
                self._observer.on_idle(sleep_ns)
            return 0

        return -sleep_ns


class ReplayScheduler(Scheduler):
    """ Runs pipelines over simulated time as fast as possible, never sleeping. """
    def _wait_until(self, next_run_ns: int) -> int:
        return 0
