from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from collections import defaultdict
from typing import Any, Protocol
import datetime as dt
import itertools
import heapq
import time
from rich.live import Live
from rich.console import Console


console = Console()


class RunnablePipeline(Protocol):
    frequency: float  # Hz
    name: str

    def run(self, state: Any, timestamp: dt.datetime) -> Iterable[Any]: ...


@dataclass(order=True)
class ScheduledRun:
    next_run_ns: int
    tie_breaker: int
    pipeline: RunnablePipeline = field(compare=False)
    period_ns: int = field(compare=False)


@dataclass
class TimingStats:
    pipelines_by_name: dict[str, RunnablePipeline]

    idle_ns: int = 0
    pipeline_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    writer_ns: int = 0
    ticks: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    late_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    current_late_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    max_late_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    last_print_ns: int = field(default_factory=time.monotonic_ns)

    def should_print(self, interval_s: float = 1.0) -> bool:
        now = time.monotonic_ns()
        return now - self.last_print_ns >= int(interval_s * 1_000_000_000)

    def reset(self) -> None:
        self.idle_ns = 0
        self.pipeline_ns.clear()
        self.writer_ns = 0
        self.ticks.clear()
        self.late_ns.clear()
        self.current_late_ns.clear()
        self.max_late_ns.clear()
        self.last_print_ns = time.monotonic_ns()

        for pipeline in self.pipelines_by_name.values():
            timing = getattr(pipeline, "timing", None)
            if timing is not None:
                timing.reset()

    def maybe_print(self, interval_s: float = 1.0):
        now = time.monotonic_ns()
        if now - self.last_print_ns < interval_s * 1_000_000_000:
            return

        total_busy = sum(self.pipeline_ns.values()) + self.writer_ns
        total = total_busy + self.idle_ns

        if total == 0:
            return

        print("\n--- Scheduler timing ---", flush=True)
        print(f"idle:   {100 * self.idle_ns / total:6.2f}%", flush=True)
        print(f"busy:   {100 * total_busy / total:6.2f}%", flush=True)
        print(f"writer: {self.writer_ns / 1e6:8.2f} ms", flush=True)

        print("pipelines:", flush=True)

        for name, ns in sorted(self.pipeline_ns.items(), key=lambda x: x[1], reverse=True):
            ticks = self.ticks[name]
            avg_ms = ns / max(ticks, 1) / 1e6
            late_total_ms = self.late_ns[name] / 1e6
            late_now_ms = self.current_late_ns[name] / 1e6
            late_max_ms = self.max_late_ns[name] / 1e6

            print(
                f"  {name:40s} total={ns/1e6:9.2f} ms "
                f"avg={avg_ms:7.3f} ms/tick "
                f"late_now={late_now_ms:7.3f} ms "
                f"late_max={late_max_ms:7.3f} ms "
                f"late_total={late_total_ms:7.3f} ms",
                flush=True,
            )

            pipeline = self.pipelines_by_name.get(name)
            timing = getattr(pipeline, "timing", None)

            if timing is not None and timing.total_ns:
                for stage_name, stage_ns in sorted(
                    timing.total_ns.items(),
                    key=lambda x: x[1],
                    reverse=True,
                ):
                    calls = timing.calls[stage_name]
                    avg_stage_ms = stage_ns / max(calls, 1) / 1e6
                    max_stage_ms = timing.max_ns[stage_name] / 1e6

                    print(
                        f"    {stage_name:36s} total={stage_ns/1e6:9.2f} ms "
                        f"avg={avg_stage_ms:7.3f} ms/call "
                        f"max={max_stage_ms:7.3f} ms",
                        flush=True,
                    )

                timing.reset()

        self.idle_ns = 0
        self.pipeline_ns.clear()
        self.writer_ns = 0
        self.ticks.clear()
        self.late_ns.clear()
        self.current_late_ns.clear()
        self.max_late_ns.clear()
        self.last_print_ns = now


from rich.table import Table


def make_table(self) -> Table:
    table = Table(title="Scheduler timing")

    table.add_column("Type")
    table.add_column("Name")
    table.add_column("Total", justify="right")
    table.add_column("Avg", justify="right")
    table.add_column("Max", justify="right")
    table.add_column("Late Now", justify="right")
    table.add_column("Late Max", justify="right")

    for name, ns in sorted(self.pipeline_ns.items(), key=lambda x: x[1], reverse=True):
        ticks = self.ticks[name]
        avg_ms = ns / max(ticks, 1) / 1e6
        late_now_ms = self.current_late_ns[name] / 1e6
        late_max_ms = self.max_late_ns[name] / 1e6

        table.add_row(
            "[bold cyan]pipeline[/bold cyan]",
            f"[bold]{name}[/bold]",
            f"{ns / 1e6:.2f} ms",
            f"{avg_ms:.3f} ms/tick",
            "",
            f"{late_now_ms:.3f} ms",
            f"{late_max_ms:.3f} ms",
        )

        pipeline = self.pipelines_by_name.get(name)
        timing = getattr(pipeline, "timing", None)

        if timing is None:
            continue

        for stage_name, stage_ns in sorted(
            timing.total_ns.items(),
            key=lambda x: x[1],
            reverse=True,
        ):
            calls = timing.calls[stage_name]
            avg_stage_ms = stage_ns / max(calls, 1) / 1e6
            max_stage_ms = timing.max_ns[stage_name] / 1e6

            table.add_row(
                "[dim]stage[/dim]",
                f"[dim]  ↳ {stage_name}[/dim]",
                f"[dim]{stage_ns / 1e6:.2f} ms[/dim]",
                f"[dim]{avg_stage_ms:.3f} ms/call[/dim]",
                f"[dim]{max_stage_ms:.3f} ms[/dim]",
                "",
                "",
            )

    return table


class Scheduler:
    def __init__(self, pipelines: Iterable[RunnablePipeline]):
        self._heap: list[ScheduledRun] = []
        self._counter = itertools.count()
        self._pipelines_by_name: dict[str, RunnablePipeline] = {}

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

            if pipeline.name in self._pipelines_by_name:
                raise ValueError(f"Duplicate pipeline name: {pipeline.name}")

            self._pipelines_by_name[pipeline.name] = pipeline
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

        self._timing = TimingStats(self._pipelines_by_name)

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
        with Live(console=console, refresh_per_second=4) as live:
            while True:
                scheduled = heapq.heappop(self._heap)

                now_ns = time.monotonic_ns()
                sleep_ns = scheduled.next_run_ns - now_ns

                if sleep_ns > 0:
                    time.sleep(sleep_ns / 1_000_000_000)
                    self._timing.idle_ns += sleep_ns
                    late_ns = 0

                else:
                    late_ns = -sleep_ns

                timestamp = self._timestamp_from_monotonic_ns(scheduled.next_run_ns)
                pipeline_name = scheduled.pipeline.name
                run_start_ns = time.monotonic_ns()

                try:
                    for frame in scheduled.pipeline.run(state, timestamp):
                        if on_output is not None:
                            write_start_ns = time.monotonic_ns()
                            on_output(scheduled.pipeline, frame, timestamp)
                            self._timing.writer_ns += time.monotonic_ns() - write_start_ns

                except Exception:
                    pass
                    if stop_on_error:
                        raise

                run_elapsed_ns = time.monotonic_ns() - run_start_ns

                self._timing.pipeline_ns[pipeline_name] += run_elapsed_ns
                self._timing.ticks[pipeline_name] += 1
                self._timing.late_ns[pipeline_name] += late_ns
                self._timing.current_late_ns[pipeline_name] = late_ns
                self._timing.max_late_ns[pipeline_name] = max(
                    self._timing.max_late_ns[pipeline_name],
                    late_ns,
                )

                scheduled.next_run_ns += scheduled.period_ns
                heapq.heappush(self._heap, scheduled)

                if self._timing.should_print(interval_s=0.5):
                    live.update(make_table(self._timing))
                    self._timing.reset()
