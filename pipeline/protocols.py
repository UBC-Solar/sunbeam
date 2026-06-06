from dataclasses import dataclass, field
from collections.abc import Iterable
from typing import Any, Protocol
import datetime as dt


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


class SchedulerObserver(Protocol):
    def on_idle(self, ns: int) -> None: ...
    def on_pipeline_start(self, pipeline_name: str, late_ns: int) -> None: ...
    def on_pipeline_done(self, pipeline_name: str, elapsed_ns: int) -> None: ...
    def on_writer_done(self, elapsed_ns: int) -> None: ...
