import datetime
import logging
import time
from collections.abc import Iterator

import networkx as nx

from pipeline.timing import StageTimingStats
from stage.stage import Stage
from state.frame import FrameView
from state.state import State

logger = logging.getLogger("sunbeam.worker")


class Pipeline:
    def __init__(self, node_graph: nx.DiGraph, frequency: float, name: str | None = None):
        """ :param name: How it's shown in logs and metrics; defaults to its stages, in order """
        self.graph = node_graph
        self.frequency = frequency
        self.timing = StageTimingStats()

        # The graph is immutable after construction, so the topological order is computed once here
        self._stages: list[Stage] = [
            self.graph.nodes[node_id]["node"]
            for node_id in nx.topological_sort(self.graph)
        ]

        self.name = name or " -> ".join(self.stage_names)

    @property
    def stage_names(self) -> list[str]:
        """ In the order they run. """
        return [stage.stage_name for stage in self._stages]

    def run(self, state: State, timestamp: datetime.datetime) -> Iterator[FrameView]:
        for stage in self._stages:
            try:
                input_frame = state.as_frame(stage.inputs, timestamp)
            except KeyError:
                # An input that hasn't been produced yet, e.g. a 10 Hz consumer ticking before its 2 Hz producer has
                # run once. Skip this tick. Telemetry that stops arriving is caught where it goes missing, by the
                # ingress stage's MissingDataTracker, rather than here.
                logger.debug("%s not ready to run yet. Yielding...", stage.stage_name)
                return

            start_ns = time.monotonic_ns()
            output_frame = stage.run(input_frame)
            self.timing.record(stage.stage_name, time.monotonic_ns() - start_ns)
            state.update(output_frame, stage)

            yield output_frame

    def __repr__(self):
        return f"Pipeline({self.graph}) running at {self.frequency:.1f} Hz"
