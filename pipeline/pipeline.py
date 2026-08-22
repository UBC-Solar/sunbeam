from pipeline.timing import StageTimingStats
from state.frame import FrameView
from state.state import State
from stage.stage import Stage
from typing import Iterator
import networkx as nx
import datetime
import time
import traceback

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
                # Temporarilly commented out
                # print(f"{stage.stage_name} not ready to run yet. Yielding...")
                # print(traceback.print_exc())
                return

            start_ns = time.monotonic_ns()
            output_frame = stage.run(input_frame)
            elapsed_ns = time.monotonic_ns() - start_ns
            self.timing.record(stage.stage_name, elapsed_ns)
            state = state.from_frame(output_frame, stage.outputs)

            yield output_frame

    def __repr__(self):
        return f"Pipeline({self.graph}) running at {self.frequency:.1f} Hz"
