from db.sunbeamdb.writer import EventWriter
from db.sunbeamdb.queued_writer import QueuedEventWriter
from sqlalchemy import Engine

from pipeline.pipeline_generator import RealtimePipelineGenerator, OfflinePipelineGenerator
from config import EventManager
from stage.stage_library import StageLibrary
from pipeline.timing import TimingStats
from pipeline.scheduler import Scheduler
from pipeline.output import OutputManager
from datetime import datetime
from state.state import State
import threading


class Executor:
    def __init__(self, event_name: str, engine: Engine, reprocess: bool = False, debug: bool = False, debug_time: datetime = None):
        writer = EventWriter(event_name, engine, reprocess=reprocess)
        self._writer = QueuedEventWriter(writer)

        event_manager = EventManager()
        event_start_datetime: datetime = event_manager.get_event_start_date(event_name)
        event_end_datetime: datetime = event_manager.get_event_end_date(event_name)

        is_past_event = event_manager.check_if_past_event(event_name=event_name, debug=debug)

        pipeline_stage_names = event_manager.get_stages_for_event(event_name)
        stage_library = StageLibrary(event_manager.get_event_pipeline_edition(event_name))

        pipeline_stage_definitions = stage_library.get_stages_by_names(pipeline_stage_names)
        pipeline_stages = [stage() for stage in pipeline_stage_definitions]

        self._pipelines, self._ingress_pipelines = None, None

        
        if is_past_event:
            self._pipelines, self._ingress_pipelines = OfflinePipelineGenerator.generate_pipeline_from_nodes(
                        pipeline_stages,
                        event_start_datetime,
                        event_end_datetime,
                        debug=debug,
                        debug_time=debug_time,
                        stage_library=stage_library
                    )
        else:
            self._pipelines, self._ingress_pipelines = RealtimePipelineGenerator.generate_pipeline_from_nodes(
                        pipeline_stages,
                        event_start_datetime,
                        event_end_datetime,
                        debug=debug,
                        debug_time=debug_time,
                        stage_library=stage_library,
                    )
        
        self._state = State()

        pipelines_by_name = {
            pipeline.name: pipeline
            for pipeline in [*self._pipelines, *self._ingress_pipelines]
        }

        self._timing = TimingStats(pipelines_by_name)
        self._compute_scheduler = Scheduler(self._pipelines, observer=self._timing)
        self._ingress_scheduler = Scheduler(self._ingress_pipelines, observer=self._timing)

    def _handle_pipeline_output(self, pipeline, frame, timestamp):
        self._writer.write_frame(frame)

    def _update_timing_display(self, live):
        if self._timing.should_print(interval_s=1.0):
            live.update(self._timing.snapshot_and_reset())

    def _run_ingress_scheduler(self):
        self._ingress_scheduler.run_forever(
            self._state,
            on_output=self._handle_pipeline_output,
            stop_on_error=True,
        )

    def run(self):
        ingress_thread = threading.Thread(target=self._run_ingress_scheduler, daemon=True)
        ingress_thread.start()

        with OutputManager(self._timing) as output_manager:
            self._compute_scheduler.run_forever(
                self._state,
                on_tick=output_manager.on_tick,
                on_output=self._handle_pipeline_output,
            )

if __name__ == '__main__':
    event_manager = EventManager()

    print(event_manager.get_event_start_date("FSGP_2024_Day_1"))
    print(event_manager.get_event_start_date("realtime"))