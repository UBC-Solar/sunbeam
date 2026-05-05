import networkx as nx


class Node:
    def __init__(self, inputs: list[str], outputs: list[str], node_name: str, frequency: float):
        self.inputs = inputs
        self.outputs = outputs
        self.node_name = node_name
        self.frequency = frequency

    def __repr__(self) -> str:
        return f"Node({self.node_name}, {self.frequency} Hz)"


def build_node_graph(nodes: list[Node]) -> nx.DiGraph:
    producer_of: dict[str, str] = {}
    for node in nodes:
        for out in node.outputs:
            if out in producer_of:
                raise ValueError(f"Signal {out!r} is produced by multiple nodes.")
            producer_of[out] = node.node_name

    g = nx.DiGraph()

    for node in nodes:
        g.add_node(node.node_name, spec=node)

    for node in nodes:
        for inp in node.inputs:
            src = producer_of.get(inp)
            if src is not None:
                g.add_edge(src, node.node_name, signal=inp)

    if not nx.is_directed_acyclic_graph(g):
        raise ValueError("Node graph is not a DAG")

    return g


def cross_rate_edges(g: nx.DiGraph) -> list[tuple[str, str, str, float, float]]:
    out = []
    for u, v, attrs in g.edges(data=True):
        ru = g.nodes[u]["spec"].frequency
        rv = g.nodes[v]["spec"].frequency
        if ru != rv:
            out.append((u, v, attrs["signal"], ru, rv))
    return out


def remove_cross_rate_edges(g: nx.DiGraph) -> nx.DiGraph:
    """
    Return a copy of g with all cross-rate edges removed.
    """
    h = g.copy()
    for u, v, _, ru, rv in cross_rate_edges(g):
        h.remove_edge(u, v)
    return h


def same_rate_components(g: nx.DiGraph) -> list[nx.DiGraph]:
    """
    Partition the graph into same-rate subgraphs with no cross-rate edges.

    Steps:
      1. Remove all cross-rate edges.
      2. Find weakly connected components in the remaining graph.
      3. Return each component as its own DiGraph.
    """
    h = remove_cross_rate_edges(g)

    components: list[nx.DiGraph] = []
    for component_nodes in nx.weakly_connected_components(h):
        sg = h.subgraph(component_nodes).copy()

        # Sanity check: every node in this component should have the same rate
        rates = {sg.nodes[n]["spec"].frequency for n in sg.nodes}
        if len(rates) != 1:
            raise RuntimeError(f"Expected one rate per component, got {rates}")

        components.append(sg)

    return components


def topo_order_for_subgraph(sg: nx.DiGraph) -> list[str]:
    return list(nx.topological_sort(sg))


def describe_subgraphs(g: nx.DiGraph) -> list[dict]:
    """
    Return a structured description of each same-rate subgraph.
    """
    result = []

    for i, sg in enumerate(same_rate_components(g)):
        rates = {sg.nodes[n]["spec"].frequency for n in sg.nodes}
        rate = next(iter(rates))

        result.append(
            {
                "subgraph_index": i,
                "rate_hz": rate,
                "nodes": list(sg.nodes),
                "topo_order": topo_order_for_subgraph(sg),
                "edges": list(sg.edges(data=True)),
            }
        )

    return result


class Pipeline:
    def __init__(self, node_graph: nx.DiGraph):
        self.graph = node_graph

    def run(self):
        pass


class PipelineGenerator:
    @staticmethod
    def generate_pipeline(nodes: list[Node]) -> list[Pipeline]:
        graph = build_node_graph(nodes)
        subgraphs = same_rate_components(graph)

        return [Pipeline(subgraph) for subgraph in subgraphs]


if __name__ == "__main__":
    ingress = Node([], ["MotorVoltage", "MotorCurrent", "VehicleSpeed"], "Ingress", 10)
    power = Node(["MotorVoltage", "MotorCurrent"], ["MotorPower"], "Power", 10)
    position = Node(["VehicleSpeed"], ["Position"], "Position", 1)
    efficiency = Node(["MotorPower", "VehicleSpeed"], ["MotorEfficiency"], "Efficiency", 5)
    lap_efficiency = Node(["Position"], ["LapEfficiency"], "LapEfficiency", 10)
    something = Node(["LapEfficiency", "Position"], ["Something"], "Something", 10)

    nodes = [ingress, power, efficiency, position, lap_efficiency, something]

    pipelines = PipelineGenerator.generate_pipeline(nodes)

    print(pipelines)
    # G = build_node_graph(nodes)
    #
    # print("All edges:")
    # for u, v, attrs in G.edges(data=True):
    #     print(f"  {u} -> {v} via {attrs['signal']}")
    #
    # print("\nCross-rate edges:")
    # for u, v, signal, ru, rv in cross_rate_edges(G):
    #     print(f"  {u} ({ru} Hz) -> {v} ({rv} Hz) via {signal}")
    #
    # print("\nSame-rate subgraphs:")
    # for info in describe_subgraphs(G):
    #     print(
    #         f"  subgraph {info['subgraph_index']}: "
    #         f"{info['rate_hz']} Hz, "
    #         f"nodes={info['nodes']}, "
    #         f"topo={info['topo_order']}"
    #     )