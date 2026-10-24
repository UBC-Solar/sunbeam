from datetime import date, datetime
from typing import Protocol

import networkx as nx  # type: ignore
from data_tools.localization import (  # type: ignore
    CanonicalName,
    InfluxDBLanguageLocalization,
)

from config.context import Context
from db.telemetrydb.protocols import IngressObserver, TimeProvider
from pipeline.pipeline import Pipeline
from stage.stage import Stage
from stage.stage_library import StageLibrary


def build_node_graph(nodes: list[Stage]) -> nx.DiGraph:
    producer_of: dict[str, str] = {}
    for node in nodes:
        for out in node.outputs:
            if out in producer_of:
                raise ValueError(f"Signal {out!r} is produced by multiple nodes.")
            producer_of[out] = node.stage_name

    g = nx.DiGraph()

    for node in nodes:
        # Stage names are the graph's node keys, so a duplicate would silently replace the earlier stage
        if node.stage_name in g:
            raise ValueError(f"Stage name {node.stage_name!r} is used by multiple nodes.")
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


def collect_signals_for_ingress(nodes: list[Stage]) -> list[CanonicalName]:
    unprovided_inputs: set[CanonicalName] = set()

    # Get every node input
    for node in nodes:
        unprovided_inputs.update(node.inputs)

    # Get every node output
    for node in nodes:
        for output_signal in node.outputs:
            unprovided_inputs.discard(output_signal)

    # Anything left must not be produced by any node: we assume it must be
    # required to be found by the ingress stage.
    return list(unprovided_inputs)


def bin_signals_by_frequency(signals: list[CanonicalName], event_date: date) -> dict[float, list[CanonicalName]]:
    signal_bins: dict[float, list[CanonicalName]] = {}
    for signal in signals:
        _, _, _, frequency = InfluxDBLanguageLocalization.localize(signal, event_date)
        signal_bins.setdefault(frequency, []).append(signal)

    return signal_bins


def build_pipeline_from_nodes(nodes: list[Stage]) -> list[Pipeline]:
    graph = build_node_graph(nodes)
    subgraphs = same_rate_components(graph)

    return [Pipeline(subgraph, frequency) for (subgraph, frequency) in subgraphs]


def _telemetry_db_kwargs() -> dict[str, str]:
    telemetry_db = Context().telemetry_db
    return {
        "bucket": telemetry_db.bucket,
        "organization": telemetry_db.organization,
        "url": telemetry_db.database_url,
        "token": telemetry_db.token,
    }


class IngressFactory(Protocol):
    """ Builds the ingress stages that provide the signals no other stage produces. """
    def make_ingress(self, signals: list[CanonicalName]) -> list[Stage]: ...


class RealtimeIngressFactory:
    """ One polling ``Ingress`` stage per signal frequency, each reading the latest values as of ``time_provider``. """
    def __init__(self, stage_library: StageLibrary, event_date: date, time_provider: TimeProvider):
        self._ingress_stage = stage_library.get_stage_by_name("Ingress")
        self._event_date = event_date
        self._time_provider = time_provider

    def make_ingress(self, signals: list[CanonicalName]) -> list[Stage]:
        return [
            self._ingress_stage(
                frequency=frequency,
                output_signals=binned_signals,
                time_provider=self._time_provider,
                **_telemetry_db_kwargs(),
            )
            for frequency, binned_signals in bin_signals_by_frequency(signals, self._event_date).items()
        ]


class OfflineIngressFactory:
    """ A single ``Offline_Ingress`` stage fetching every signal over the whole event window. """
    def __init__(self, stage_library: StageLibrary, start: datetime, end: datetime, observer: IngressObserver | None = None):
        self._ingress_stage = stage_library.get_stage_by_name("Offline_Ingress")
        self._start = start
        self._end = end
        self._observer = observer

    def make_ingress(self, signals: list[CanonicalName]) -> list[Stage]:
        if not signals:
            return []

        return [
            self._ingress_stage(
                output_signals=signals,
                event_start_date=self._start,
                event_end_date=self._end,
                observer=self._observer,
                **_telemetry_db_kwargs(),
            )
        ]


def generate_pipelines(nodes: list[Stage], ingress_factory: IngressFactory) -> tuple[list[Pipeline], list[Pipeline]]:
    """ Builds the compute pipelines for ``nodes``, and the ingress pipelines that feed them.

    :return tuple[list[Pipeline], list[Pipeline]]: (compute pipelines, ingress pipelines)
    """
    ingress_nodes = ingress_factory.make_ingress(collect_signals_for_ingress(nodes))

    return build_pipeline_from_nodes(nodes), build_pipeline_from_nodes(ingress_nodes)
