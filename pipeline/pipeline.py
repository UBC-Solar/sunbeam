from pipeline.timing import StageTimingStats
from state.frame import FrameView
from state.state import State
from stage.stage import Stage
from typing import Iterator, Optional
import networkx as nx
import datetime
import logging
import time

logger = logging.getLogger("sunbeam.worker")


class Pipeline:
    def __init__(self, node_graph: nx.DiGraph, frequency: float, not_ready_grace_s: float = 1.0):
        self.graph = node_graph
        self.frequency = frequency
        self.timing = StageTimingStats()

        # The graph is immutable after construction, so the topological order
        # is computed once here instead of on every run.
        self._stages: list[Stage] = [
            self.graph.nodes[node_id]["node"]
            for node_id in nx.topological_sort(self.graph)
        ]

        self.name = " -> ".join(stage.stage_name for stage in self._stages)

        # A stage's inputs may legitimately be missing right after startup
        # (e.g. a 10 Hz consumer ticking before its 2 Hz producer has run
        # once). Tolerate that for a bounded grace period; if the pipeline
        # still cannot run after it, something is actually wrong - raise
        # instead of silently idling forever.
        self._not_ready_grace_s = not_ready_grace_s
        self._not_ready_since: Optional[float] = None

    def run(self, state: State, timestamp: datetime.datetime) -> Iterator[FrameView]:
        for stage in self._stages:
            try:
                input_frame = state.as_frame(stage.inputs, timestamp)
            except KeyError as exc:
                self._handle_not_ready(stage, exc)
                return

            start_ns = time.monotonic_ns()
            output_frame = stage.run(input_frame)
            elapsed_ns = time.monotonic_ns() - start_ns
            self.timing.record(stage.stage_name, elapsed_ns)
            state = state.from_frame(output_frame, stage.outputs)

            yield output_frame

        self._not_ready_since = None

    def _handle_not_ready(self, stage: Stage, exc: KeyError) -> None:
        now = time.monotonic()

        if self._not_ready_since is None:
            self._not_ready_since = now
            logger.debug("%s not ready to run yet. Yielding...", stage.stage_name)
            return

        waited_s = now - self._not_ready_since
        if waited_s > self._not_ready_grace_s:
            missing_signal = exc.args[0] if exc.args else "<unknown>"
            raise RuntimeError(
                f"Pipeline {self.name!r}: stage {stage.stage_name!r} is still "
                f"missing input {missing_signal!r} after {waited_s:.2f}s "
                f"(grace is {self._not_ready_grace_s:.2f}s). A producer "
                f"upstream is not delivering."
            ) from exc

        logger.debug("%s not ready to run yet. Yielding...", stage.stage_name)

    def __repr__(self):
        return f"Pipeline({self.graph}) running at {self.frequency:.1f} Hz"
