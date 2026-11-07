import threading
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field

from pipeline.protocols import RunnablePipeline


@dataclass
class StageTimingStats:
    """ Per-stage run times within one pipeline. """
    total_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    calls: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    max_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    lock: threading.RLock = field(default_factory=threading.RLock)

    def record(self, stage_name: str, elapsed_ns: int) -> None:
        with self.lock:
            self.total_ns[stage_name] += elapsed_ns
            self.calls[stage_name] += 1
            self.max_ns[stage_name] = max(self.max_ns[stage_name], elapsed_ns)

    def metrics_and_reset(self, order: list[str] | None = None) -> list[dict]:
        """ :param order: The stages in the order to list them (the order they run); otherwise as first recorded """
        with self.lock:
            names = [name for name in (order or []) if name in self.total_ns] or list(self.total_ns)
            stages = [
                {
                    "name": name,
                    "total_ms": self.total_ns[name] / 1e6,
                    "avg_ms": self.total_ns[name] / max(self.calls[name], 1) / 1e6,
                    "max_ms": self.max_ns[name] / 1e6,
                    "calls": self.calls[name],
                }
                for name in names
            ]
            self.total_ns.clear()
            self.calls.clear()
            self.max_ns.clear()
            return stages


@dataclass
class TimingStats:
    """
    Scheduler timing (a SchedulerObserver), shared by every scheduler thread. One consumer turns it into a metrics
    payload at a time (see pipeline.output.MetricsReporter), since doing so resets it.

    Pipelines are reported in the order of pipelines_by_name and stages in the order they run, never by how long they
    took, so a display of the reports holds still. A pipeline is listed from its first run on, even through a report
    in which it didn't run.
    """
    # Mapping (not dict) so callers can pass dicts of concrete Pipeline subtypes without variance errors.
    pipelines_by_name: Mapping[str, RunnablePipeline]

    idle_ns: int = 0
    writer_ns: int = 0
    pipeline_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    pipeline_max_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    ticks: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    # Every pipeline that has run at all
    seen: set[str] = field(default_factory=set)
    current_late_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    max_late_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    lock: threading.RLock = field(default_factory=threading.RLock)

    def on_idle(self, ns: int) -> None:
        with self.lock:
            self.idle_ns += ns

    def on_pipeline_start(self, pipeline_name: str, late_ns: int) -> None:
        with self.lock:
            self.current_late_ns[pipeline_name] = late_ns
            self.max_late_ns[pipeline_name] = max(self.max_late_ns[pipeline_name], late_ns)

    def on_pipeline_done(self, pipeline_name: str, elapsed_ns: int) -> None:
        with self.lock:
            self.pipeline_ns[pipeline_name] += elapsed_ns
            self.pipeline_max_ns[pipeline_name] = max(self.pipeline_max_ns[pipeline_name], elapsed_ns)
            self.ticks[pipeline_name] += 1
            self.seen.add(pipeline_name)

    def on_writer_done(self, elapsed_ns: int) -> None:
        with self.lock:
            self.writer_ns += elapsed_ns

    def metrics_payload_and_reset(self) -> dict:
        """ Everything since the last call, as the JSON-ready payload a worker reports to the server. """
        with self.lock:
            busy_ns = sum(self.pipeline_ns.values()) + self.writer_ns
            total_ns = busy_ns + self.idle_ns

            pipelines = []
            for name in [*self.pipelines_by_name, *sorted(self.seen - set(self.pipelines_by_name))]:
                if name not in self.seen:
                    continue
                pipeline = self.pipelines_by_name.get(name)
                timing = getattr(pipeline, "timing", None)
                ns = self.pipeline_ns[name]
                pipelines.append({
                    "name": name,
                    "frequency": getattr(pipeline, "frequency", None),
                    "total_ms": ns / 1e6,
                    "avg_ms": ns / max(self.ticks[name], 1) / 1e6,
                    "max_ms": self.pipeline_max_ns[name] / 1e6,
                    "ticks": self.ticks[name],
                    "late_now_ms": self.current_late_ns[name] / 1e6,
                    "late_max_ms": self.max_late_ns[name] / 1e6,
                    "stages": timing.metrics_and_reset(getattr(pipeline, "stage_names", None)) if timing else [],
                })

            payload = {
                "idle_pct": 100 * self.idle_ns / total_ns if total_ns else 0.0,
                "busy_pct": 100 * busy_ns / total_ns if total_ns else 0.0,
                "writer_ms": self.writer_ns / 1e6,
                "pipelines": pipelines,
            }

            self.idle_ns = 0
            self.writer_ns = 0
            self.pipeline_ns.clear()
            self.pipeline_max_ns.clear()
            self.ticks.clear()
            self.current_late_ns.clear()
            self.max_late_ns.clear()

            return payload
