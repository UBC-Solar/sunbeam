import threading
import time
from datetime import datetime, timedelta

from sqlalchemy import Engine

from config import EventManager
from db.sunbeamdb.queued_writer import QueuedEventWriter
from db.sunbeamdb.writer import EventWriter
from db.telemetrydb.realtime_ingress import DebugTimeProvider
from pipeline.output import IngressProgressReporter, TimingTableReporter
from pipeline.pipeline_generator import (
    IngressFactory,
    OfflineIngressFactory,
    RealtimeIngressFactory,
    generate_pipelines,
)
from pipeline.scheduler import RealtimeScheduler, ReplayScheduler
from pipeline.timing import TimingStats
from stage.stage_library import StageLibrary
from state.state import State

# Warn if telemetry covers noticeably less than the event window
REPLAY_TRIM_WARNING = timedelta(seconds=5)


class Executor:
    def __init__(self, event_name: str, engine: Engine, reprocess: bool = False, debug: bool = False, debug_time: datetime | None = None):
        self._event = EventManager().get_event(event_name)
        self._writer = QueuedEventWriter(EventWriter(event_name, engine, reprocess=reprocess))

        stage_library = StageLibrary(self._event.pipeline_edition)
        kwargs = {"event_name": event_name}
        pipeline_stages = [stage(**kwargs) for stage in stage_library.get_stages_by_names(self._event.stages)]

        # Past events are replayed from their recorded telemetry. Debug mode instead runs any event through the
        # realtime path, against a fake clock starting at debug_time.
        self._is_replay = self._event.is_past() and not debug
        self._ingress_reporter = IngressProgressReporter()

        ingress_factory: IngressFactory
        if self._is_replay:
            ingress_factory = OfflineIngressFactory(stage_library, self._event.starts_at, self._event.ends_at, observer=self._ingress_reporter)
        else:
            time_provider = DebugTimeProvider(start_time=debug_time) if debug else datetime
            ingress_factory = RealtimeIngressFactory(stage_library, self._event.starts_at.date(), time_provider)

        self._pipelines, self._ingress_pipelines = generate_pipelines(pipeline_stages, ingress_factory)
        self._state = State()

        pipelines_by_name = {
            pipeline.name: pipeline
            for pipeline in self._pipelines + self._ingress_pipelines
        }

        self._timing = TimingStats(pipelines_by_name)

    def _handle_pipeline_output(self, pipeline, frame, timestamp):
        self._writer.write_frame(frame)

    def _replay_bounds(self) -> tuple[datetime, datetime] | None:
        """ The part of the event window for which every ingressed signal has data. """
        data_bounds = self._state.timeseries_bounds()
        if data_bounds is None:
            return None

        event_start, event_end = self._event.starts_at, self._event.ends_at
        data_start, data_end = data_bounds
        start = max(event_start, data_start)
        end = min(event_end, data_end)

        if end <= start:
            return None

        trimmed = (start - event_start) + (event_end - end)
        if trimmed > REPLAY_TRIM_WARNING:
            print(
                f"Warning: telemetry only covers {start:%Y-%m-%d %H:%M:%S} -> {end:%Y-%m-%d %H:%M:%S} "
                f"of event window {event_start:%Y-%m-%d %H:%M:%S} -> {event_end:%Y-%m-%d %H:%M:%S}; "
                f"processing the covered range only."
            )

        return start, end

    def _run_replay(self):
        # Offline ingress fetches the whole event in one go, so it runs once up front rather than on a schedule
        with self._ingress_reporter:
            for pipeline in self._ingress_pipelines:
                for frame in pipeline.run(self._state, self._event.starts_at):
                    self._writer.write_frame(frame)

        bounds = self._replay_bounds()
        if bounds is None:
            print(f"No telemetry found for {self._event.name}; nothing to process.")
            return

        start, end = bounds
        compute_scheduler = ReplayScheduler(self._pipelines, observer=self._timing, now_wall=start, end_wall=end)

        wall_start = time.monotonic()
        with TimingTableReporter(self._timing) as reporter:
            compute_scheduler.run(
                self._state,
                on_tick=reporter.on_tick,
                on_output=self._handle_pipeline_output,
            )

        print("Waiting for database writes to finish...")
        self._writer.close()

        print(f"Processed {self._event.name}: {start:%H:%M:%S} -> {end:%H:%M:%S} in {time.monotonic() - wall_start:.1f} s")

    def _run_realtime(self):
        ingress_scheduler = RealtimeScheduler(self._ingress_pipelines, observer=self._timing)
        compute_scheduler = RealtimeScheduler(self._pipelines, observer=self._timing)

        ingress_thread = threading.Thread(
            target=ingress_scheduler.run,
            args=(self._state,),
            kwargs={"on_output": self._handle_pipeline_output, "stop_on_error": True},
            daemon=True,
        )
        ingress_thread.start()

        with TimingTableReporter(self._timing) as reporter:
            compute_scheduler.run(
                self._state,
                on_tick=reporter.on_tick,
                on_output=self._handle_pipeline_output,
            )

    def run(self):
        try:
            if self._is_replay:
                self._run_replay()
            else:
                self._run_realtime()
        finally:
            self._writer.close()  # No-op if the replay already closed it
