from db.sunbeamdb.writer import EventWriter
from db.sunbeamdb.queued_writer import QueuedEventWriter
from sqlalchemy import Engine

from pipeline.pipeline_generator import PipelineGenerator
from config import EventManager
from stage.stage_library import StageLibrary
from pipeline.scheduler import Scheduler
from datetime import datetime
from state.state import State


class Executor:
    def __init__(self, event_name: str, engine: Engine, reprocess: bool = False, debug: bool = False, debug_time: datetime = None):
        writer = EventWriter(event_name, engine, reprocess=reprocess)
        self._writer = QueuedEventWriter(writer)

        event_manager = EventManager()
        event_datetime: datetime = event_manager.get_event_date(event_name)
        pipeline_stage_names = event_manager.get_stages_for_event(event_name)
        stage_library = StageLibrary(event_manager.get_event_pipeline_edition(event_name))

        pipeline_stage_definitions = stage_library.get_stages_by_names(pipeline_stage_names)
        pipeline_stages = [stage() for stage in pipeline_stage_definitions]

        self._pipelines = PipelineGenerator.generate_pipeline_from_nodes(
            pipeline_stages,
            event_datetime.date(),
            debug=debug,
            debug_time=debug_time,
            stage_library=stage_library
        )
        self._state = State()

        self._scheduler = Scheduler(self._pipelines)

    def _handle_pipeline_output(self, pipeline, frame, timestamp):
        self._writer.write_frame(frame)

    def run(self):
        self._scheduler.run_forever(self._state, on_output=self._handle_pipeline_output)