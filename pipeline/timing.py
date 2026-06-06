from pipeline.protocols import RunnablePipeline
from dataclasses import dataclass, field
import threading

from collections import defaultdict
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
import time


@dataclass
class StageTimingStats:
    total_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    calls: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    max_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    lock: threading.RLock = field(default_factory=threading.RLock)

    def record(self, stage_name: str, elapsed_ns: int) -> None:
        with self.lock:
            self.total_ns[stage_name] += elapsed_ns
            self.calls[stage_name] += 1
            self.max_ns[stage_name] = max(self.max_ns[stage_name], elapsed_ns)

    def reset(self) -> None:
        with self.lock:
            self.total_ns.clear()
            self.calls.clear()
            self.max_ns.clear()

    def snapshot(self):
        with self.lock:
            return {
                "total_ns": dict(self.total_ns),
                "calls": dict(self.calls),
                "max_ns": dict(self.max_ns),
            }


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
    lock: threading.RLock = field(default_factory=threading.RLock)

    def on_idle(self, ns: int) -> None:
        with self.lock:
            self.idle_ns += ns

    def on_pipeline_start(self, pipeline_name: str, late_ns: int) -> None:
        with self.lock:
            self.late_ns[pipeline_name] += late_ns
            self.current_late_ns[pipeline_name] = late_ns
            self.max_late_ns[pipeline_name] = max(
                self.max_late_ns[pipeline_name],
                late_ns,
            )

    def on_pipeline_done(self, pipeline_name: str, elapsed_ns: int) -> None:
        with self.lock:
            self.pipeline_ns[pipeline_name] += elapsed_ns
            self.ticks[pipeline_name] += 1

    def on_writer_done(self, elapsed_ns: int) -> None:
        with self.lock:
            self.writer_ns += elapsed_ns

    def should_print(self, interval_s: float = 1.0) -> bool:
        with self.lock:
            now = time.monotonic_ns()
            return now - self.last_print_ns >= int(interval_s * 1_000_000_000)

    def snapshot_and_reset(self):
        with self.lock:
            renderable = self._make_table_unlocked()
            self._reset_unlocked()
            return renderable
    def _reset_unlocked(self):
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

    def reset(self) -> None:
        with self.lock:
            self._reset_unlocked()

    def _make_table_unlocked(self):
        total_busy = sum(self.pipeline_ns.values()) + self.writer_ns
        total = total_busy + self.idle_ns

        idle_pct = 100 * self.idle_ns / total if total else 0
        busy_pct = 100 * total_busy / total if total else 0

        summary = Panel(
            f"[green]Idle[/green]: {idle_pct:6.2f}%    "
            f"[yellow]Busy[/yellow]: {busy_pct:6.2f}%    "
            f"[cyan]Writer[/cyan]: {self.writer_ns / 1e6:.2f} ms",
            title="Scheduler usage",
        )

        table = Table(title="Pipeline / Stage timing")

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

            snapshot = timing.snapshot()

            for stage_name, stage_ns in sorted(
                    snapshot["total_ns"].items(),
                    key=lambda x: x[1],
                    reverse=True,
            ):
                calls = snapshot["calls"][stage_name]
                avg_stage_ms = stage_ns / max(calls, 1) / 1e6
                max_stage_ms = snapshot["max_ns"][stage_name] / 1e6

                table.add_row(
                    "[dim]stage[/dim]",
                    f"[dim]  ↳ {stage_name}[/dim]",
                    f"[dim]{stage_ns / 1e6:.2f} ms[/dim]",
                    f"[dim]{avg_stage_ms:.3f} ms/call[/dim]",
                    f"[dim]{max_stage_ms:.3f} ms[/dim]",
                    "",
                    "",
                )

        return Group(summary, table)

    def make_table(self):
        with self.lock:
            return self._make_table_unlocked()
