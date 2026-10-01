from datetime import datetime
from typing import Protocol, Self

from rich.console import Console
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, TaskID, TextColumn, TimeElapsedColumn


class ProgressReporter(Protocol):
    """ Displays scheduler progress. Used as a context manager, with ``on_tick`` called after every scheduled run. """
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type, exc, tb) -> None: ...
    def on_tick(self, timestamp: datetime) -> None: ...


class TimingTableReporter:
    def __init__(self, timing, *, interval_s: float = 1.0):
        self._timing = timing
        self._interval_s = interval_s
        self._console = Console()
        self._live = None

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
