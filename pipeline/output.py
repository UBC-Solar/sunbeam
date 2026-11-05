import logging
import math
import time
from collections.abc import Callable
from datetime import datetime
from typing import Protocol, Self

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from pipeline.replay_progress import ReplayProgress, format_duration, format_speed
from pipeline.timing import TimingStats

logger = logging.getLogger("sunbeam.worker")


class OutputManager(Protocol):
    """ Displays scheduler progress. Used as a context manager, with ``on_tick`` called after every scheduled run. """
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type, exc, tb) -> bool | None: ...
    def on_tick(self, timestamp: datetime) -> None: ...


class MetricsReporter:
    """
    Every ``report_interval_s``, takes one metrics snapshot (timing, writer queue, and replay progress if any) and
    hands it to each sink:

    - the server, via ``control.report_metrics`` (a no-op for a serverless worker)
    - a log line every ``log_interval_s`` (None for no logging), which Docker captures regardless of TTY
    - with ``table=True``, a live timing table, which needs a real terminal to redraw in place

    It is the only thing that reads (and so resets) the TimingStats.
    """

    def __init__(
            self,
            timing: TimingStats,
            control,
            *,
            report_interval_s: float = 5.0,
            log_interval_s: float | None = 5.0,
            table: bool = False,
            writer_stats: Callable[[], dict] | None = None,
            replay: ReplayProgress | None = None,
            monotonic: Callable[[], float] = time.monotonic,
    ):
        self._timing = timing
        self._control = control
        self._report_interval_s = report_interval_s
        self._log_interval_s = log_interval_s
        self._writer_stats = writer_stats
        self._replay = replay
        self._monotonic = monotonic
        self._last_report = monotonic()
        self._last_log = -math.inf
        self._live = Live(console=Console(), refresh_per_second=4) if table else None

    def __enter__(self):
        if self._live is not None:
            self._live.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._live is not None:
            if exc_type is None:
                # Show whatever accumulated since the last report, so the final table isn't lost
                self._live.update(render_metrics_table(self._snapshot()))
            self._live.__exit__(exc_type, exc, tb)
        return False

    def on_tick(self, timestamp: datetime):
        if self._replay is not None:
            self._replay.update(timestamp)

        now = self._monotonic()
        if now - self._last_report < self._report_interval_s:
            return
        self._last_report = now

        payload = self._snapshot()
        self._control.report_metrics(payload)

        if self._live is not None:
            self._live.update(render_metrics_table(payload))

        if self._log_interval_s is not None and now - self._last_log >= self._log_interval_s:
            self._last_log = now
            self._log(payload)

    def _snapshot(self) -> dict:
        payload = self._timing.metrics_payload_and_reset()
        if self._writer_stats is not None:
            payload["writer_queue"] = self._writer_stats()
        if self._replay is not None:
            payload["replay"] = self._replay.snapshot()
        return payload

    def _log(self, payload: dict) -> None:
        if self._replay is not None:
            logger.info(
                "replay %.1f%% %s / %s UTC %s realtime, eta %s",
                100 * self._replay.fraction,
                f"{self._replay.current:%H:%M:%S}",
                f"{self._replay.end:%H:%M:%S}",
                format_speed(self._replay.speed),
                format_duration(self._replay.eta_s),
            )

        pipeline_summary = ", ".join(f"{p['name']}={p['avg_ms']:.2f}ms/tick" for p in payload["pipelines"])

        queue_summary = ""
        if "writer_queue" in payload:
            wq = payload["writer_queue"]
            queue_summary = (
                f" queue={wq['queue_depth']}/{wq['queue_capacity']}"
                f" (hwm {wq['queue_high_water']})"
                f" flush_avg={wq['avg_flush_ms']:.2f}ms"
            )

        logger.info(
            "timing idle=%.1f%% busy=%.1f%% writer=%.2fms%s %s",
            payload["idle_pct"],
            payload["busy_pct"],
            payload["writer_ms"],
            queue_summary,
            pipeline_summary,
        )


def render_metrics_table(payload: dict) -> Group:
    """ A metrics payload (see TimingStats.metrics_payload_and_reset) as a Rich table. """
    summary = Panel(
        f"[green]Idle[/green]: {payload['idle_pct']:6.2f}%    "
        f"[yellow]Busy[/yellow]: {payload['busy_pct']:6.2f}%    "
        f"[cyan]Writer[/cyan]: {payload['writer_ms']:.2f} ms",
        title="Scheduler usage",
    )

    table = Table(title="Pipeline / Stage timing")
    for column in ("Type", "Name"):
        table.add_column(column)
    for column in ("Total", "Avg", "Max", "Late Now", "Late Max"):
        table.add_column(column, justify="right")

    for pipeline in payload["pipelines"]:
        table.add_row(
            "[bold cyan]pipeline[/bold cyan]",
            f"[bold]{pipeline['name']}[/bold]",
            f"{pipeline['total_ms']:.2f} ms",
            f"{pipeline['avg_ms']:.3f} ms/tick",
            "",
            f"{pipeline['late_now_ms']:.3f} ms",
            f"{pipeline['late_max_ms']:.3f} ms",
        )
        for stage in pipeline["stages"]:
            table.add_row(
                "[dim]stage[/dim]",
                f"[dim]  ↳ {stage['name']}[/dim]",
                f"[dim]{stage['total_ms']:.2f} ms[/dim]",
                f"[dim]{stage['avg_ms']:.3f} ms/call[/dim]",
                f"[dim]{stage['max_ms']:.3f} ms[/dim]",
                "",
                "",
            )

    return Group(summary, table)


class RichReplayOutputManager:
    """
    Interactive display of a replay: a live progress bar through the replay
    window, with the simulated clock, speed relative to realtime, ETA and the
    writer queue. Needs a real TTY to redraw.

    The timing table matters for realtime runs, which must keep up with
    incoming data; a replay only needs to finish, so its progress is shown
    instead.
    """

    def __init__(
            self,
            replay: ReplayProgress,
            *,
            writer_stats: Callable[[], dict] | None = None,
            refresh_interval_s: float = 0.1,
            console: Console | None = None,
            monotonic: Callable[[], float] = time.monotonic,
    ):
        self._replay = replay
        self._writer_stats = writer_stats
        self._refresh_interval_s = refresh_interval_s
        self._monotonic = monotonic
        self._last_refresh = -math.inf

        self._progress = Progress(
            SpinnerColumn(finished_text="[green]✓[/green]"),
            TextColumn("[bold]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("{task.fields[clock]}"),
            TextColumn("[cyan]{task.fields[speed]}"),
            TimeElapsedColumn(),
            TextColumn("eta {task.fields[eta]}"),
            TextColumn("[dim]{task.fields[queue]}"),
            console=console or Console(),
        )
        self._task = self._progress.add_task(
            f"Replaying {replay.start:%Y-%m-%d}",
            total=replay.total_s,
            clock="", speed="", eta="", queue="",
        )

    def __enter__(self):
        self._refresh()
        self._progress.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._refresh()
        self._progress.stop()

    def on_tick(self, timestamp: datetime):
        self._replay.update(timestamp)

        # Ticks come far faster than anyone can read, so only redraw the bar a few times a second
        now = self._monotonic()
        if now - self._last_refresh >= self._refresh_interval_s:
            self._last_refresh = now
            self._refresh()

    def _refresh(self) -> None:
        replay = self._replay
        queue = ""
        if self._writer_stats is not None:
            stats = self._writer_stats()
            queue = f"queue {stats['queue_depth']:,}/{stats['queue_capacity']:,}"

        self._progress.update(
            self._task,
            completed=replay.processed_s,
            clock=f"{replay.current:%H:%M:%S} / {replay.end:%H:%M:%S} UTC",
            speed=format_speed(replay.speed),
            eta=format_duration(replay.eta_s),
            queue=queue,
        )


class IngressProgressReporter:
    """ Renders ingress query progress (an ``IngressObserver``) as one live line per queried field. """
    def __init__(self):
        self._progress = Progress(
            SpinnerColumn(finished_text=" "),
            TextColumn("{task.description}"),
            TimeElapsedColumn(),
            console=Console(),
        )
        self._tasks: dict[str, TaskID] = {}

    def __enter__(self):
        self._progress.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._progress.stop()

    def on_query_start(self, field: str) -> None:
        self._tasks[field] = self._progress.add_task(f"Querying {field} from InfluxDB...", total=1)

    def on_query_done(self, field: str, points: int, elapsed_s: float) -> None:
        self._progress.update(
            self._tasks[field],
            description=f"[green]✓[/green] {field}: {points:,} points",
            completed=1,
        )

    def on_query_failed(self, field: str, error: BaseException) -> None:
        self._progress.update(self._tasks[field], description=f"[red]✗ {field}: {type(error).__name__}[/red]")
        self._progress.stop_task(self._tasks[field])
