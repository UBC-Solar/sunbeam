from dataclasses import dataclass, field
from collections import defaultdict
from state.frame import FrameView
from state.state import State
from stage.stage import Stage
from typing import Iterator
import networkx as nx
import datetime
import time

@dataclass
class StageTimingStats:
    total_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    calls: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    max_ns: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def record(self, stage_name: str, elapsed_ns: int) -> None:
        self.total_ns[stage_name] += elapsed_ns
        self.calls[stage_name] += 1
        self.max_ns[stage_name] = max(self.max_ns[stage_name], elapsed_ns)

    def reset(self) -> None:
        self.total_ns.clear()
        self.calls.clear()
        self.max_ns.clear()


class Pipeline:
    def __init__(self, node_graph: nx.DiGraph, frequency: float):
        self.graph = node_graph
        self.frequency = frequency
        self.timing = StageTimingStats()

        self.name = " -> ".join(
            self.graph.nodes[node_id]["node"].stage_name
            for node_id in nx.topological_sort(self.graph)
        )

    def run(self, state: State, timestamp: datetime.datetime) -> Iterator[FrameView]:
        for node_id in nx.topological_sort(self.graph):
            stage: Stage = self.graph.nodes[node_id]["node"]

            try:
                input_frame = state.as_frame(stage.inputs, timestamp)
            except KeyError:
                print(f"{stage.stage_name} not ready to run yet. Yielding...")
                return

            start_ns = time.monotonic_ns()
            output_frame = stage.run(input_frame)
            elapsed_ns = time.monotonic_ns() - start_ns
            self.timing.record(stage.stage_name, elapsed_ns)
            state = state.from_frame(output_frame, stage.outputs)

            yield output_frame

    def __repr__(self):
        return f"Pipeline({self.graph}) running at {self.frequency:.1f} Hz"
