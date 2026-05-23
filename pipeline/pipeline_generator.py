from db.telemetrydb.realtime_ingress import DebugTimeProvider
from stage.stage import Stage
import networkx as nx
from pipeline.pipeline import Pipeline
from data_tools.localization import InfluxDBLanguageLocalization, CanonicalName
from datetime import date, datetime


def build_node_graph(nodes: list[Stage]) -> nx.DiGraph:
    producer_of: dict[str, str] = {}
    for node in nodes:
        for out in node.outputs:
            if out in producer_of:
                raise ValueError(f"Signal {out!r} is produced by multiple nodes.")
            producer_of[out] = node.stage_name

    g = nx.DiGraph()

    for node in nodes:
        g.add_node(node.stage_name, node=node)

    for node in nodes:
        for inp in node.inputs:
            src = producer_of.get(inp)
            if src is not None:
                g.add_edge(src, node.stage_name, signal=inp)

    if not nx.is_directed_acyclic_graph(g):
        raise ValueError("Node graph is not a DAG")

    return g


def cross_rate_edges(g: nx.DiGraph) -> list[tuple[str, str, str, float, float]]:
    out = []
    for u, v, attrs in g.edges(data=True):
        ru = g.nodes[u]["node"].frequency
        rv = g.nodes[v]["node"].frequency
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


def same_rate_components(g: nx.DiGraph) -> list[tuple[nx.DiGraph, float]]:
    """
    Partition the graph into same-rate subgraphs with no cross-rate edges.

    Steps:
      1. Remove all cross-rate edges.
      2. Find weakly connected components in the remaining graph.
      3. Return each component as its own DiGraph.
    """
    h = remove_cross_rate_edges(g)

    components: list[tuple[nx.DiGraph, float]] = []
    for component_nodes in nx.weakly_connected_components(h):
        sg = h.subgraph(component_nodes).copy()

        # Sanity check: every node in this component should have the same rate
        rates = {sg.nodes[n]["node"].frequency for n in sg.nodes}
        if len(rates) != 1:
            raise RuntimeError(f"Expected one rate per component, got {rates}")
        else:
            rate = next(iter(rates))

        components.append((sg, rate))

    return components


def topo_order_for_subgraph(sg: nx.DiGraph) -> list[str]:
    return list(nx.topological_sort(sg))


def describe_subgraphs(g: nx.DiGraph) -> list[dict]:
    """
    Return a structured description of each same-rate subgraph.
    """
    result = []

    for i, (sg, _) in enumerate(same_rate_components(g)):
        rates = {sg.nodes[n]["node"].frequency for n in sg.nodes}
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


class PipelineGenerator:
    @staticmethod
    def collect_signals_for_ingress(nodes: list[Stage]) -> list[CanonicalName]:
        unprovided_inputs: set[CanonicalName] = set()

        # Get every node input
        for node in nodes:
            for input_signal in node.inputs:
                if input_signal not in unprovided_inputs:
                    unprovided_inputs.add(input_signal)

        # Get every node output
        for node in nodes:
            for output_signal in node.outputs:
                if output_signal in unprovided_inputs:
                    unprovided_inputs.remove(output_signal)

        # Anything left must not be produced by any node: we assume it must be
        # required to be found by the ingress stage.
        return list(unprovided_inputs)

    @staticmethod
    def bin_signals_by_frequency(signals: list[CanonicalName], event_date: date) -> dict[float, list[CanonicalName]]:
        signal_bins: dict[float, list[CanonicalName]] = {}
        for signal in signals:
            _, _, _, frequency = InfluxDBLanguageLocalization.localize(signal, event_date)

            if frequency not in signal_bins:
                signal_bins[frequency] = []

            signal_bins[frequency].append(signal)

        return signal_bins

    @staticmethod
    def generate_ingress_for_nodes(signal_bins: dict[float, list[CanonicalName]], stage_library, debug: bool = False, debug_time: datetime = None) -> list[Stage]:
        ingress_node = stage_library.get_stage_by_name("Ingress")

        ingress_nodes: list[Stage] = []
        for frequency, signals in signal_bins.items():
            if debug:
                time_provider = DebugTimeProvider(start_time=debug_time)
            else:
                time_provider = datetime

            ingress_nodes.append(
                ingress_node(
                    frequency=frequency,
                    output_signals=signals,
                    time_provider=time_provider
                )
            )

        return ingress_nodes

    @staticmethod
    def generate_pipeline_from_nodes(nodes: list[Stage], event_date: date, stage_library, debug: bool = False, debug_time: datetime = None) -> list[Pipeline]:
        ingress_signals = PipelineGenerator.collect_signals_for_ingress(nodes)
        signal_bins = PipelineGenerator.bin_signals_by_frequency(ingress_signals, event_date)
        ingress_nodes = PipelineGenerator.generate_ingress_for_nodes(signal_bins, debug=debug, debug_time=debug_time, stage_library=stage_library)

        all_nodes = [*ingress_nodes, *nodes]

        graph = build_node_graph(all_nodes)
        subgraphs = same_rate_components(graph)

        pipelines = [Pipeline(subgraph, frequency) for (subgraph, frequency) in subgraphs]

        return pipelines

    def from_event(self):
        pass
