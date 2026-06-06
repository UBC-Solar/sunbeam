from rich.console import Console
from rich.live import Live


class OutputManager:
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
            self._live.__exit__(exc_type, exc, tb)

    def on_tick(self):
        if self._live is None:
            return

        if self._timing.should_print(interval_s=self._interval_s):
            self._live.update(self._timing.snapshot_and_reset())