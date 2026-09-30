import threading
import time
from datetime import datetime, timedelta

from sqlalchemy import Engine

from config import EventManager
from db.sunbeamdb.queued_writer import QueuedEventWriter
from db.sunbeamdb.writer import EventWriter
from pipeline.output import ProgressReporter, TimingTableReporter
from pipeline.pipeline_generator import (
    OfflinePipelineGenerator,
    RealtimePipelineGenerator,
)
from pipeline.scheduler import OfflineScheduler, RealtimeScheduler, ReplayScheduler
from pipeline.timing import TimingStats
from stage.stage_library import StageLibrary
from state.state import State

# Warn if telemetry covers noticeably less than the event window
REPLAY_TRIM_WARNING = timedelta(seconds=5)


class Executor:
    def __init__(self, event_name: str, engine: Engine, reprocess: bool = False, debug: bool = False, debug_time: datetime | None = None):
        writer = EventWriter(event_name, engine, reprocess=reprocess)
        self._writer = QueuedEventWriter(writer)

        event_manager = EventManager()
        event_start_datetime: datetime = event_manager.get_event_start_date(event_name)
        event_end_datetime: datetime = event_manager.get_event_end_date(event_name)

        is_past_event = event_manager.check_if_past_event(event_name=event_name, debug=debug)

        if is_past_event:
            event_manager.clear_event(engine, event_name)

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

        self._event_name = event_name
        self._is_past_event = is_past_event
        self._event_start = event_start_datetime
        self._event_end = event_end_datetime

        if is_past_event:
            # The compute scheduler is built in _run_offline(), once ingress has told us what range the data covers
            self._ingress_scheduler = OfflineScheduler(self._ingress_pipelines, observer=self._timing, now_wall=event_start_datetime)
        else:
            self._ingress_scheduler = RealtimeScheduler(self._ingress_pipelines, observer=self._timing)
            self._compute_scheduler = RealtimeScheduler(self._pipelines, observer=self._timing)

    def _handle_pipeline_output(self, pipeline, frame, timestamp):
        self._writer.write_frame(frame)

    def _update_timing_display(self, live):
        if self._timing.should_print(interval_s=1.0):
            live.update(self._timing.snapshot_and_reset())

    def _run_ingress_scheduler(self):
        self._ingress_scheduler.run(
            self._state,
            on_output=self._handle_pipeline_output,
            stop_on_error=True,
        )

    def _make_reporter(self) -> ProgressReporter:
        return TimingTableReporter(self._timing)

    def _replay_bounds(self) -> tuple[datetime, datetime] | None:
        """ The part of the event window for which every ingressed signal has data. """
        data_bounds = self._state.timeseries_bounds()
        if data_bounds is None:
            return None

        data_start, data_end = data_bounds
        start = max(self._event_start, data_start)
        end = min(self._event_end, data_end)

        if end <= start:
            return None

        trimmed = (start - self._event_start) + (self._event_end - end)
        if trimmed > REPLAY_TRIM_WARNING:
            print(
                f"Warning: telemetry only covers {start:%Y-%m-%d %H:%M:%S} -> {end:%Y-%m-%d %H:%M:%S} "
                f"of event window {self._event_start:%Y-%m-%d %H:%M:%S} -> {self._event_end:%Y-%m-%d %H:%M:%S}; "
                f"processing the covered range only."
            )

        return start, end

    def _run_offline(self):
        self._run_ingress_scheduler()

        bounds = self._replay_bounds()
        if bounds is None:
            print(f"No telemetry found for {self._event_name}; nothing to process.")
            return

        start, end = bounds
        compute_scheduler = ReplayScheduler(self._pipelines, observer=self._timing, now_wall=start, end_wall=end)

        wall_start = time.monotonic()
        with self._make_reporter() as reporter:
            compute_scheduler.run(
                self._state,
                on_tick=reporter.on_tick,
                on_output=self._handle_pipeline_output,
            )

        print(f"Processed {self._event_name}: {start:%H:%M:%S} -> {end:%H:%M:%S} in {time.monotonic() - wall_start:.1f} s")

    def _run_realtime(self):
        ingress_thread = threading.Thread(target=self._run_ingress_scheduler, daemon=True)
        ingress_thread.start()

        with self._make_reporter() as reporter:
            self._compute_scheduler.run(
                self._state,
                on_tick=reporter.on_tick,
                on_output=self._handle_pipeline_output,
            )

    def run(self):
        try:
            if self._is_past_event:
                self._run_offline()
            else:
                self._run_realtime()
        finally:
            self._writer.close()

if __name__ == '__main__':
    event_manager = EventManager()

    print(event_manager.get_event_start_date("FSGP_2024_Day_1"))
    print(event_manager.get_event_start_date("realtime"))