import logging

from rich.console import Console
from rich.live import Live

logger = logging.getLogger("sunbeam.worker")


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


class LoggingOutputManager:
    """
    Headless equivalent of RichOutputManager: instead of redrawing a table in
    place, periodically emits a plain log line (which Docker captures
    correctly regardless of TTY) and pushes the same snapshot to the
    orchestrator via the worker's control channel.
    """

    def __init__(self, timing, control, *, interval_s: float = 5.0):
        self._timing = timing
        self._control = control
        self._interval_s = interval_s

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def on_tick(self):
        if not self._timing.should_print(interval_s=self._interval_s):
            return

        payload = self._timing.metrics_payload_and_reset()

        pipeline_summary = ", ".join(
            f"{p['name']}={p['avg_ms']:.2f}ms/tick" for p in payload["pipelines"]
        )
        logger.info(
            "timing idle=%.1f%% busy=%.1f%% writer=%.2fms %s",
            payload["idle_pct"],
            payload["busy_pct"],
            payload["writer_ms"],
            pipeline_summary,
        )

        self._control.report_metrics(payload)