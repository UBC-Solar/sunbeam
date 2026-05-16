from state.frame import FrameView
from typing import Iterator
from state.state import State
from stage.node import Node
import networkx as nx
import datetime


class Pipeline:
    def __init__(self, node_graph: nx.DiGraph, frequency: float):
        self.graph = node_graph
        self.frequency = frequency

    def run(self, state: State, timestamp: datetime.datetime) -> Iterator[FrameView]:
        for node_id in nx.topological_sort(self.graph):
            node: Node = self.graph.nodes[node_id]["node"]

            try:
                input_frame = state.as_frame(node.inputs, timestamp)
            except KeyError:
                print(f"{node.node_name} not ready to run yet. Yielding...")
                return

            output_frame = node.run(input_frame)
            state = state.from_frame(output_frame, node.outputs)

            yield output_frame

    def __repr__(self):
        return f"Pipeline({self.graph}) running at {self.frequency:.1f} Hz"
