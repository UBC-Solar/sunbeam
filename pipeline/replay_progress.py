import time
from collections.abc import Callable
from datetime import datetime


class ReplayProgress:
    """
    How far a replay has got through its window of simulated time, and how
    fast it is going. Fed the timestamp of every scheduler tick; cheap enough
    to update that often, with the derived figures only computed on read.
    """

    def __init__(self, start: datetime, end: datetime, monotonic: Callable[[], float] = time.monotonic):
        self.start = start
        self.end = end
        self.current = start
        self._monotonic = monotonic
        self._wall_start = monotonic()

    def update(self, timestamp: datetime) -> None:
        self.current = min(max(timestamp, self.start), self.end)

    @property
    def total_s(self) -> float:
        return (self.end - self.start).total_seconds()

    @property
    def processed_s(self) -> float:
        return (self.current - self.start).total_seconds()

    @property
    def fraction(self) -> float:
        return self.processed_s / self.total_s if self.total_s > 0 else 1.0

    @property
    def elapsed_s(self) -> float:
        return self._monotonic() - self._wall_start

    @property
    def speed(self) -> float | None:
        """ Simulated seconds processed per wall-clock second, i.e. how many times faster than realtime. """
        elapsed_s = self.elapsed_s
        return self.processed_s / elapsed_s if elapsed_s > 0 and self.processed_s > 0 else None

    @property
    def eta_s(self) -> float | None:
        """ Wall-clock seconds left at the average speed so far. """
        speed = self.speed
        return (self.total_s - self.processed_s) / speed if speed else None

    def snapshot(self) -> dict:
        """ The progress as a JSON-ready dict, the ``replay`` field of a worker's metrics report. """
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "current": self.current.isoformat(),
            "fraction": self.fraction,
            "processed_s": self.processed_s,
            "total_s": self.total_s,
            "elapsed_s": self.elapsed_s,
            "speed": self.speed,
            "eta_s": self.eta_s,
        }


def format_duration(seconds: float | None) -> str:
    """ ``1h 02m``, ``3m 12s`` or ``45s``; ``--`` while unknown. """
    if seconds is None:
        return "--"

    seconds = int(round(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {seconds:02d}s"
    return f"{seconds}s"


def format_speed(speed: float | None) -> str:
    """ ``×830`` realtime; ``×--`` while unknown. """
    return f"×{speed:,.0f}" if speed is not None else "×--"
