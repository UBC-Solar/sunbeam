import networkx as nx
import pytest

from pipeline.pipeline_generator import (
    PipelineGenerator,
    build_node_graph,
    cross_rate_edges,
    same_rate_components,
)
from stage.stage import Stage
from state.frame import Frame, FrameView


def make_stage(name: str, inputs: list[str], outputs: list[str], frequency: float) -> Stage:
    # __init_subclass__ requires the ClassVars in the class body; build them
    # dynamically so each call can produce a distinct stage type.
    attrs = {
        "stage_name": name,
        "inputs": inputs,
        "outputs": outputs,
        "frequency": frequency,
        "run": lambda self, input_frame: Frame.from_view(input_frame),
    }
    cls = type(name, (Stage,), attrs)
    return cls()


class TestBuildNodeGraph:
    def test_edges_connect_producers_to_consumers(self):
        a = make_stage("A", [], ["x"], 10.0)
        b = make_stage("B", ["x"], ["y"], 10.0)
        c = make_stage("C", ["x", "y"], ["z"], 10.0)

        graph = build_node_graph([a, b, c])

        assert set(graph.edges) == {("A", "B"), ("A", "C"), ("B", "C")}
        assert list(nx.topological_sort(graph)) == ["A", "B", "C"]

    def test_external_inputs_create_no_edges(self):
        a = make_stage("A", ["from_ingress"], ["x"], 10.0)

        graph = build_node_graph([a])

        assert set(graph.nodes) == {"A"}
        assert set(graph.edges) == set()

    def test_duplicate_producer_rejected(self):
        a = make_stage("A", [], ["x"], 10.0)
        b = make_stage("B", [], ["x"], 10.0)

        with pytest.raises(ValueError, match="produced by multiple nodes"):
            build_node_graph([a, b])

    def test_cycle_rejected(self):
        a = make_stage("A", ["y"], ["x"], 10.0)
        b = make_stage("B", ["x"], ["y"], 10.0)

        with pytest.raises(ValueError, match="not a DAG"):
            build_node_graph([a, b])


class TestRatePartitioning:
    def test_cross_rate_edges_detected(self):
        a = make_stage("A", [], ["x"], 10.0)
        b = make_stage("B", ["x"], ["y"], 1.0)

        graph = build_node_graph([a, b])
        edges = cross_rate_edges(graph)

        assert len(edges) == 1
        assert edges[0][:3] == ("A", "B", "x")

    def test_same_rate_components_split_by_rate(self):
        a = make_stage("A", [], ["x"], 10.0)
        b = make_stage("B", ["x"], ["y"], 10.0)
        c = make_stage("C", ["y"], ["z"], 1.0)

        graph = build_node_graph([a, b, c])
        components = same_rate_components(graph)

        by_rate = {rate: sorted(sg.nodes) for sg, rate in components}
        assert by_rate == {10.0: ["A", "B"], 1.0: ["C"]}

    def test_single_rate_graph_is_one_component(self):
        a = make_stage("A", [], ["x"], 10.0)
        b = make_stage("B", ["x"], ["y"], 10.0)

        components = same_rate_components(build_node_graph([a, b]))

        assert len(components) == 1
        assert components[0][1] == 10.0


class TestCollectSignalsForIngress:
    def test_unproduced_inputs_are_ingress_signals(self):
        a = make_stage("A", ["raw_1", "raw_2"], ["x"], 10.0)
        b = make_stage("B", ["x", "raw_3"], ["y"], 10.0)

        signals = PipelineGenerator.collect_signals_for_ingress([a, b])

        assert sorted(signals) == ["raw_1", "raw_2", "raw_3"]

    def test_fully_internal_graph_needs_no_ingress(self):
        a = make_stage("A", [], ["x"], 10.0)
        b = make_stage("B", ["x"], ["y"], 10.0)

        assert PipelineGenerator.collect_signals_for_ingress([a, b]) == []


class TestBuildPipelineFromNodes:
    def test_one_pipeline_per_rate_group(self):
        a = make_stage("A", [], ["x"], 10.0)
        b = make_stage("B", ["x"], ["y"], 10.0)
        c = make_stage("C", ["y"], ["z"], 1.0)

        pipelines = PipelineGenerator.build_pipeline_from_nodes([a, b, c])

        assert sorted(p.frequency for p in pipelines) == [1.0, 10.0]
        names = {p.name for p in pipelines}
        assert names == {"A -> B", "C"}
