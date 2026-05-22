from db.sunbeamdb.writer import EventWriter
from sqlalchemy import Engine

from pipeline.pipeline_generator import PipelineGenerator
from config import EventManager
from stage.stage_library import StageLibrary
from pipeline.scheduler import Scheduler
from datetime import datetime
from state.state import State


class Executor:
    def __init__(self, event_name: str, engine: Engine, reprocess: bool = False, debug: bool = False, debug_time: datetime = None):
        self._writer = EventWriter(event_name, engine, reprocess=reprocess)

        event_datetime: datetime = EventManager().get_event_date(event_name)
        pipeline_node_names = EventManager().get_stages_for_event(event_name)
        pipeline_nodes = StageLibrary.get_stages_by_names(pipeline_node_names)
        self._pipelines = PipelineGenerator.generate_pipeline_from_nodes(
            pipeline_nodes,
            event_datetime.date(),
            debug=debug,
            debug_time=debug_time
        )
        self._state = State()

        self._scheduler = Scheduler(self._pipelines)

    def _handle_pipeline_output(self, pipeline, frame, timestamp):
        self._writer.write_frame(frame)

    def run(self):
        self._scheduler.run_forever(self._state, on_output=self._handle_pipeline_output)