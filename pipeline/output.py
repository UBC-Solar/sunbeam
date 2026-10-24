import logging
from datetime import datetime
from typing import Protocol, Self

from rich.console import Console
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, TaskID, TextColumn, TimeElapsedColumn

logger = logging.getLogger("sunbeam.worker")


class OutputManager(Protocol):
    """ Displays scheduler progress. Used as a context manager, with ``on_tick`` called after every scheduled run. """
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type, exc, tb) -> bool | None: ...
    def on_tick(self, timestamp: datetime) -> None: ...


class RichOutputManager:
    """
    Interactive display for a human watching a terminal. Relies on rich.Live,
    which needs a real TTY to redraw in place - do not use this for a
    headless/orchestrated worker, since Live silently produces little to no
    output when it isn't attached to a terminal.
    """

    def __init__(self, timing, *, interval_s: float = 1.0):
        self._timing = timing
        self._interval_s = interval_s
        self._console = Console()
        self._live: Live | None = None

    def __enter__(self):
        self._live = Live(console=self._console, refresh_per_second=4)
        self._live.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._live is not None:
            if exc_type is None:
                # Show whatever accumulated since the last refresh so the final table isn't lost
                self._live.update(self._timing.snapshot_and_reset())
            self._live.__exit__(exc_type, exc, tb)

    def on_tick(self, timestamp: datetime):
        if self._live is None:
            return

        if self._timing.should_print(interval_s=self._interval_s):
            self._live.update(self._timing.snapshot_and_reset())


class LoggingOutputManager:
    """
    Headless equivalent of RichOutputManager: instead of redrawing a table in
    place, periodically emits a plain log line (which Docker captures
    correctly regardless of TTY) and pushes the same snapshot to the
    server via the worker's control channel.
    """

    def __init__(self, timing, control, *, interval_s: float = 5.0, writer_stats=None):
        self._timing = timing
        self._control = control
        self._interval_s = interval_s
        # Optional zero-arg callable returning the frame writer's queue/flush
        # statistics (see QueuedEventWriter.stats).
        self._writer_stats = writer_stats

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def on_tick(self, timestamp: datetime):
        if not self._timing.should_print(interval_s=self._interval_s):
            return

        payload = self._timing.metrics_payload_and_reset()

        if self._writer_stats is not None:
            payload["writer_queue"] = self._writer_stats()

        pipeline_summary = ", ".join(
            f"{p['name']}={p['avg_ms']:.2f}ms/tick" for p in payload["pipelines"]
        )

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

        self._control.report_metrics(payload)


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
