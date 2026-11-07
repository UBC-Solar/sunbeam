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


def same_rate_components(g: nx.DiGraph) -> list[tuple[nx.DiGraph, float]]:
    """ Partition the graph into same-rate subgraphs: the weakly connected components once every edge between
    stages of different rates is dropped. """
    same_rate = nx.DiGraph()
    same_rate.add_nodes_from(g.nodes(data=True))
    same_rate.add_edges_from(
        (u, v, attrs) for u, v, attrs in g.edges(data=True)
        if g.nodes[u]["node"].frequency == g.nodes[v]["node"].frequency
    )

    components = []
    for nodes in nx.weakly_connected_components(same_rate):
        subgraph = same_rate.subgraph(nodes).copy()
        components.append((subgraph, g.nodes[next(iter(nodes))]["node"].frequency))
    return components


def collect_signals_for_ingress(nodes: list[Stage]) -> list[CanonicalName]:
    """ Every input no node produces, which the ingress must fetch. """
    inputs = {signal for node in nodes for signal in node.inputs}
    outputs = {signal for node in nodes for signal in node.outputs}
    return list(inputs - outputs)


def bin_signals_by_frequency(signals: list[CanonicalName], event_date: date) -> dict[float, list[CanonicalName]]:
    signal_bins: dict[float, list[CanonicalName]] = {}
    for signal in signals:
        _, _, _, frequency = InfluxDBLanguageLocalization.localize(signal, event_date)
        signal_bins.setdefault(frequency, []).append(signal)

    return signal_bins


def build_pipeline_from_nodes(nodes: list[Stage], prefix: str = "") -> list[Pipeline]:
    """
    The same-rate pipelines for the nodes, fastest first, then by their stages' names, so a run always lists (and
    names) them the same way. Each is named by its rate after the prefix: "10 Hz", then "10 Hz #2" for another at
    the same rate. One that is never scheduled (rate 0: a replay's fetch) is just the prefix.
    """
    subgraphs = sorted(
        same_rate_components(build_node_graph(nodes)),
        key=lambda component: (-component[1], sorted(component[0].nodes)),
    )

    pipelines = []
    seen_per_rate: dict[float, int] = {}
    for subgraph, frequency in subgraphs:
        seen_per_rate[frequency] = seen_per_rate.get(frequency, 0) + 1
        name = f"{prefix}{frequency:g} Hz" if frequency else prefix.strip()
        if seen_per_rate[frequency] > 1:
            name += f" #{seen_per_rate[frequency]}"
        pipelines.append(Pipeline(subgraph, frequency, name=name))
    return pipelines


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

    return build_pipeline_from_nodes(nodes), build_pipeline_from_nodes(ingress_nodes, prefix="Ingress ")
