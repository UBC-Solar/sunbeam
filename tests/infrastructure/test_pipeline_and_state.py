from datetime import datetime

import pytest

from pipeline.pipeline import Pipeline
from pipeline.pipeline_generator import build_node_graph
from stage.stage import Stage
from state.frame import Frame, FrameView
from state.state import State


class SourceStage(Stage):
    stage_name = "Source"
    inputs = []
    outputs = ["a"]
    frequency = 10.0

    def run(self, input_frame: FrameView) -> Frame:
        frame = Frame.from_view(input_frame)
        frame.write("a", 21.0)
        return frame


class DoublerStage(Stage):
    stage_name = "Doubler"
    inputs = ["a"]
    outputs = ["b"]
    frequency = 10.0

    def run(self, input_frame: FrameView) -> Frame:
        frame = Frame.from_view(input_frame)
        frame.write("b", input_frame.read("a") * 2)
        return frame


class OrphanStage(Stage):
    stage_name = "Orphan"
    inputs = ["never_produced"]
    outputs = ["c"]
    frequency = 10.0

    def run(self, input_frame: FrameView) -> Frame:
        frame = Frame.from_view(input_frame)
        frame.write("c", 1.0)
        return frame


@pytest.fixture
def timestamp():
    return datetime(2026, 7, 18, 12, 0, 0)


class TestFrame:
    def test_write_then_read(self, timestamp):
        frame = Frame(timestamp)
        frame.write("x", 1.5)
        assert frame.read("x") == 1.5

    def test_read_missing_signal_raises(self, timestamp):
        with pytest.raises(KeyError):
            FrameView(timestamp).read("missing")

    def test_iteration_yields_items(self, timestamp):
        frame = Frame(timestamp)
        frame.write("x", 1.0)
        frame.write("y", 2.0)
        assert dict(frame) == {"x": 1.0, "y": 2.0}

    def test_as_view_preserves_values_and_timestamp(self, timestamp):
        frame = Frame(timestamp)
        frame.write("x", 3.0)
        view = frame.as_view()
        assert view.timestamp == timestamp
        assert view.read("x") == 3.0


class TestState:
    def test_as_frame_raises_for_missing_signal(self, timestamp):
        state = State()
        with pytest.raises(KeyError):
            state.as_frame(["missing"], timestamp)

    def test_round_trip_through_frame(self, timestamp):
        state = State({"a": 5.0})
        frame = state.as_frame(["a"], timestamp)
        assert frame.read("a") == 5.0

    def test_from_frame_stores_outputs(self, timestamp):
        state = State()
        frame = Frame(timestamp)
        frame.write("b", 7.0)

        state.from_frame(frame.as_view(), ["b"])

        assert state.as_frame(["b"], timestamp).read("b") == 7.0

    def test_from_frame_tolerates_missing_outputs(self, timestamp):
        state = State()
        frame = Frame(timestamp)

        # Declared output was never written; must not raise.
        state.from_frame(frame.as_view(), ["b"])

        with pytest.raises(KeyError):
            state.as_frame(["b"], timestamp)


class TestPipelineRun:
    def test_chain_executes_in_topological_order(self, timestamp):
        # Deliberately pass stages in reverse order: the graph must fix it.
        graph = build_node_graph([DoublerStage(), SourceStage()])
        pipeline = Pipeline(graph, frequency=10.0)

        assert pipeline.name == "Source -> Doubler"

        state = State()
        frames = list(pipeline.run(state, timestamp))

        assert len(frames) == 2
        assert state.as_frame(["b"], timestamp).read("b") == 42.0

    def test_stage_with_unmet_inputs_yields_nothing(self, timestamp):
        graph = build_node_graph([OrphanStage()])
        pipeline = Pipeline(graph, frequency=10.0)

        frames = list(pipeline.run(State(), timestamp))

        assert frames == []

    def test_second_run_reuses_state_from_first(self, timestamp):
        graph = build_node_graph([SourceStage()])
        source_pipeline = Pipeline(graph, frequency=10.0)
        consumer_pipeline = Pipeline(build_node_graph([DoublerStage()]), frequency=10.0)

        state = State()

        # Consumer alone can't run: its input isn't in state yet.
        assert list(consumer_pipeline.run(state, timestamp)) == []

        list(source_pipeline.run(state, timestamp))
        frames = list(consumer_pipeline.run(state, timestamp))

        assert len(frames) == 1
        assert frames[0].read("b") == 42.0

    def test_timing_recorded_per_stage(self, timestamp):
        graph = build_node_graph([SourceStage(), DoublerStage()])
        pipeline = Pipeline(graph, frequency=10.0)

        list(pipeline.run(State(), timestamp))
        snapshot = pipeline.timing.snapshot()

        assert snapshot["calls"] == {"Source": 1, "Doubler": 1}


class TestNotReadyGrace:
    def test_transient_not_ready_is_tolerated(self, timestamp):
        pipeline = Pipeline(
            build_node_graph([OrphanStage()]), frequency=10.0, not_ready_grace_s=10.0
        )

        # Repeated not-ready runs inside the grace period stay silent.
        assert list(pipeline.run(State(), timestamp)) == []
        assert list(pipeline.run(State(), timestamp)) == []

    def test_persistent_not_ready_raises_after_grace(self, timestamp):
        import time

        pipeline = Pipeline(
            build_node_graph([OrphanStage()]), frequency=10.0, not_ready_grace_s=0.05
        )

        list(pipeline.run(State(), timestamp))
        time.sleep(0.06)

        with pytest.raises(RuntimeError, match="missing input 'never_produced'"):
            list(pipeline.run(State(), timestamp))

    def test_successful_run_resets_the_grace_clock(self, timestamp):
        import time

        pipeline = Pipeline(
            build_node_graph([DoublerStage()]), frequency=10.0, not_ready_grace_s=0.05
        )
        state = State()

        list(pipeline.run(state, timestamp))

        # The input arrives before the grace expires; the pipeline recovers.
        source = Pipeline(build_node_graph([SourceStage()]), frequency=10.0)
        list(source.run(state, timestamp))
        assert len(list(pipeline.run(state, timestamp))) == 1

        # A prior stall must not poison later runs.
        time.sleep(0.06)
        assert len(list(pipeline.run(state, timestamp))) == 1


class TestStageContract:
    def test_subclass_must_declare_classvars(self):
        with pytest.raises(TypeError, match="must override 'stage_name'"):
            class MissingName(Stage):
                inputs = []
                outputs = []
                frequency = 1.0

                def run(self, input_frame):
                    pass
