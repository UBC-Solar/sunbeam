import logging
import math
import time
from collections.abc import Callable
from contextlib import ExitStack
from datetime import datetime
from typing import Protocol, Self

from rich.console import Console
from rich.live import Live
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from pipeline.replay_progress import ReplayProgress, format_duration, format_speed

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


class RichReplayOutputManager:
    """
    Interactive display of a replay: a live progress bar through the replay
    window, with the simulated clock, speed relative to realtime, ETA and the
    writer queue. Like RichOutputManager, it needs a real TTY to redraw.

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


class LoggingOutputManager:
    """
    Headless equivalent of RichOutputManager: pushes a metrics snapshot to the
    server via the worker's control channel every ``report_interval_s``, and
    emits it as a plain log line (which Docker captures correctly regardless
    of TTY) every ``log_interval_s``. During a replay, the snapshot also
    carries the replay's progress.

    With ``log_interval_s=None`` it only reports, for when something else
    (like RichReplayOutputManager) is already drawing in the terminal.
    """

    def __init__(
            self,
            timing,
            control,
            *,
            report_interval_s: float = 5.0,
            log_interval_s: float | None = 5.0,
            writer_stats=None,
            replay: ReplayProgress | None = None,
            monotonic: Callable[[], float] = time.monotonic,
    ):
        self._timing = timing
        self._control = control
        self._report_interval_s = report_interval_s
        self._log_interval_s = log_interval_s
        # Optional zero-arg callable returning the frame writer's queue/flush
        # statistics (see QueuedEventWriter.stats).
        self._writer_stats = writer_stats
        self._replay = replay
        self._monotonic = monotonic
        self._last_log = -math.inf

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def on_tick(self, timestamp: datetime):
        if self._replay is not None:
            self._replay.update(timestamp)

        if not self._timing.should_print(interval_s=self._report_interval_s):
            return

        payload = self._timing.metrics_payload_and_reset()

        if self._writer_stats is not None:
            payload["writer_queue"] = self._writer_stats()

        if self._replay is not None:
            payload["replay"] = self._replay.snapshot()

        self._control.report_metrics(payload)

        now = self._monotonic()
        if self._log_interval_s is not None and now - self._last_log >= self._log_interval_s:
            self._last_log = now
            self._log(payload)

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


class CombinedOutputManager:
    """ Runs several output managers side by side, e.g. a terminal display alongside reporting to the server. """

    def __init__(self, *managers: OutputManager):
        self._managers = managers
        self._stack = ExitStack()

    def __enter__(self):
        for manager in self._managers:
            self._stack.enter_context(manager)
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._stack.__exit__(exc_type, exc, tb)

    def on_tick(self, timestamp: datetime):
        for manager in self._managers:
            manager.on_tick(timestamp)


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
