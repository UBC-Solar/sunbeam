import logging
import threading
import time
from collections.abc import Callable
from contextlib import nullcontext
from datetime import datetime, timedelta

from sqlalchemy import Engine

from config import EventManager
from config.context import Context
from config.events import EventConfig
from db.sunbeamdb.queued_writer import QueuedEventWriter
from db.sunbeamdb.writer import EventWriter
from db.telemetrydb.protocols import IngressObserver, TimeProvider
from db.telemetrydb.realtime_ingress import DebugTimeProvider
from orchestration.control import OrchestratedWorkerControl, ServerlessWorkerControl, WorkerControl
from pipeline.output import IngressProgressReporter, LoggingOutputManager, OutputManager, RichOutputManager
from pipeline.pipeline import Pipeline
from pipeline.pipeline_generator import (
    IngressFactory,
    OfflineIngressFactory,
    RealtimeIngressFactory,
    generate_pipelines,
)
from pipeline.protocols import FrameWriter
from pipeline.scheduler import RealtimeScheduler, ReplayScheduler
from pipeline.timing import TimingStats
from stage.stage_library import StageLibrary
from state.state import State

logger = logging.getLogger("sunbeam.worker")

# Warn if telemetry covers noticeably less than the event window
REPLAY_TRIM_WARNING = timedelta(seconds=5)


def replay_window_for(event: EventConfig) -> tuple[datetime, datetime] | None:
    """ The window to replay a past event over, or None to run it live. """
    if event.ends_at is None or not event.is_past():
        return None

    return event.starts_at, event.ends_at


def build_event_pipelines(
        event: EventConfig,
        *,
        replay_window: tuple[datetime, datetime] | None = None,
        ingress_observer: IngressObserver | None = None,
        stage_library: StageLibrary | None = None,
) -> tuple[list[Pipeline], list[Pipeline]]:
    """
    Build the compute and ingress pipelines for an event from its
    configuration (events.toml + stage registry). With a replay window the
    ingress fetches that whole window up front; otherwise it polls live,
    against a fake clock starting at telemetrydb.debug_time in debug mode.
    """
    stage_library = stage_library or StageLibrary(event.pipeline_edition)

    kwargs = {"event_name": event.name}
    pipeline_stages = [stage(**kwargs) for stage in stage_library.get_stages_by_names(event.stages)]

    logger.info(
        "Loaded %d stages for event %r: %s",
        len(pipeline_stages), event.name, event.stages,
    )

    ingress_factory: IngressFactory
    if replay_window is not None:
        start, end = replay_window
        ingress_factory = OfflineIngressFactory(stage_library, start, end, observer=ingress_observer)
    else:
        telemetry_db = Context().telemetry_db
        time_provider: TimeProvider
        if telemetry_db.debug:
            time_provider = DebugTimeProvider(start_time=datetime.fromisoformat(telemetry_db.debug_time))
        else:
            time_provider = datetime  # type: ignore[assignment]
        ingress_factory = RealtimeIngressFactory(stage_library, event.starts_at.date(), time_provider)

    return generate_pipelines(pipeline_stages, ingress_factory)


class Executor:
    def __init__(
            self,
            pipelines: list[Pipeline],
            ingress_pipelines: list[Pipeline],
            writer: FrameWriter,
            control: WorkerControl | None = None,
            *,
            output_manager: OutputManager | None = None,
            monotonic_ns: Callable[[], int] = time.monotonic_ns,
            sleep: Callable[[float], None] = time.sleep,
            replay_window: tuple[datetime, datetime] | None = None,
            ingress_reporter: IngressProgressReporter | None = None,
    ):
        """
        :param replay_window: (start, end) of a past event to replay as fast as possible, or None to run live.
            A replay runs every ingress pipeline once up front, then the compute pipelines over the window.
        :param ingress_reporter: Displays the up-front ingress queries of a replay; the ingress stages must have
            been built to report to it
        """
        self._control = control or ServerlessWorkerControl()
        self._writer = writer
        self._pipelines = pipelines
        self._ingress_pipelines = ingress_pipelines
        self._output_manager = output_manager
        self._monotonic_ns = monotonic_ns
        self._sleep = sleep
        self._replay_window = replay_window
        self._ingress_reporter = ingress_reporter
        self._state = State()

        logger.info(
            "Executor got %d compute pipeline(s) and %d ingress pipeline(s): %s",
            len(self._pipelines), len(self._ingress_pipelines),
            [p.name for p in [*self._pipelines, *self._ingress_pipelines]],
        )

        pipelines_by_name = {
            pipeline.name: pipeline
            for pipeline in [*self._pipelines, *self._ingress_pipelines]
        }

        self._timing = TimingStats(pipelines_by_name)
        self._stopped = False

        self._ingress_crashed = threading.Event()
        self._ingress_crash_message: str | None = None
        self._completed = threading.Event()
        self._ingress_threads: list[threading.Thread] = []

    @classmethod
    def from_event(
            cls,
            event_name: str,
            engine: Engine,
            reprocess: bool = False,
            control: WorkerControl | None = None,
    ) -> "Executor":
        event = EventManager().get_event(event_name)
        replay_window = replay_window_for(event)

        # A replay rewrites the whole event, so samples from any earlier run must be wiped first or they collide with
        # the new ones. Live runs only wipe when asked to, since they may be resuming an ongoing event.
        wipe = reprocess or replay_window is not None
        writer = QueuedEventWriter(EventWriter(event_name, engine, reprocess=wipe))

        ingress_reporter = IngressProgressReporter() if replay_window is not None else None
        pipelines, ingress_pipelines = build_event_pipelines(
            event, replay_window=replay_window, ingress_observer=ingress_reporter,
        )

        return cls(
            pipelines, ingress_pipelines, writer, control,
            replay_window=replay_window, ingress_reporter=ingress_reporter,
        )

    def is_stopped(self) -> bool:
        return self._stopped

    def signal_completion(self) -> None:
        """
        Mark the event as finished: both schedulers wind down and the worker
        reports success. A replay reaching the end of its window completes on
        its own; this is for anything else that detects end-of-data.
        """
        self._completed.set()

    def _handle_pipeline_output(self, pipeline, frame, timestamp):
        self._control.set_stage(pipeline.name)
        self._writer.write_frame(frame)

    def _should_stop(self) -> bool:
        return (
            self._control.should_stop()
            or self._ingress_crashed.is_set()
            or self._completed.is_set()
        )

    def _run_ingress_scheduler(self, scheduler: RealtimeScheduler):
        try:
            scheduler.run(
                self._state,
                on_output=self._handle_pipeline_output,
                stop_on_error=True,
                should_stop=self._should_stop,
            )
        except Exception as exc:
            logger.exception("Ingress scheduler crashed")
            self._ingress_crash_message = str(exc)
            self._ingress_crashed.set()

    def _resolve_output_manager(self) -> OutputManager:
        if self._output_manager is not None:
            return self._output_manager

        if isinstance(self._control, OrchestratedWorkerControl):
            return LoggingOutputManager(
                self._timing,
                self._control,
                writer_stats=getattr(self._writer, "stats", None),
            )

        return RichOutputManager(self._timing)

    def _replay_bounds(self, event_start: datetime, event_end: datetime) -> tuple[datetime, datetime] | None:
        """ The part of the event window for which every ingressed signal has data. """
        data_bounds = self._state.timeseries_bounds()
        if data_bounds is None:
            return None

        data_start, data_end = data_bounds
        start = max(event_start, data_start)
        end = min(event_end, data_end)

        if end <= start:
            return None

        trimmed = (start - event_start) + (event_end - end)
        if trimmed > REPLAY_TRIM_WARNING:
            logger.warning(
                "Telemetry only covers %s -> %s of event window %s -> %s; processing the covered range only.",
                f"{start:%Y-%m-%d %H:%M:%S}", f"{end:%Y-%m-%d %H:%M:%S}",
                f"{event_start:%Y-%m-%d %H:%M:%S}", f"{event_end:%Y-%m-%d %H:%M:%S}",
            )

        return start, end

    def _run_replay(self, event_start: datetime, event_end: datetime) -> None:
        # Offline ingress fetches the whole event in one go, so it runs once up front rather than on a schedule
        with self._ingress_reporter or nullcontext():
            for pipeline in self._ingress_pipelines:
                for frame in pipeline.run(self._state, event_start):
                    self._handle_pipeline_output(pipeline, frame, event_start)

        bounds = self._replay_bounds(event_start, event_end)
        if bounds is None:
            raise RuntimeError("No telemetry found for the event window; nothing to process.")

        start, end = bounds
        compute_scheduler = ReplayScheduler(
            self._pipelines, observer=self._timing,
            now_wall=start, end_wall=end, monotonic_ns=self._monotonic_ns,
        )

        wall_start = time.monotonic()
        logger.info("Starting replay of %s -> %s.", f"{start:%H:%M:%S}", f"{end:%H:%M:%S}")
        with self._resolve_output_manager() as output_manager:
            compute_scheduler.run(
                self._state,
                on_tick=output_manager.on_tick,
                on_output=self._handle_pipeline_output,
                should_stop=self._should_stop,
            )

        if not self._control.should_stop():
            # Reaching the end of the window is what completes a replay
            self.signal_completion()

        logger.info("Waiting for database writes to finish...")
        self._writer.close()
        logger.info("Replayed %s -> %s in %.1f s.", f"{start:%H:%M:%S}", f"{end:%H:%M:%S}", time.monotonic() - wall_start)

    def _run_realtime(self) -> None:
        # One scheduler (and one thread) per ingress pipeline: ingress runs are
        # blocking I/O, so a slow query in one frequency bin must not stall the
        # others. run() joins them on the way out.
        self._ingress_threads = [
            threading.Thread(
                target=self._run_ingress_scheduler,
                args=(self._make_realtime_scheduler([pipeline]),),
                name=f"sunbeam-ingress-{i}",
                daemon=True,
            )
            for i, pipeline in enumerate(self._ingress_pipelines)
        ]

        logger.info("Starting %d ingress scheduler(s).", len(self._ingress_threads))
        for thread in self._ingress_threads:
            thread.start()

        compute_scheduler = self._make_realtime_scheduler(self._pipelines)

        logger.info("Starting compute scheduler.")
        with self._resolve_output_manager() as output_manager:
            compute_scheduler.run(
                self._state,
                on_tick=output_manager.on_tick,
                on_output=self._handle_pipeline_output,
                should_stop=self._should_stop,
            )

    def _make_realtime_scheduler(self, pipelines: list[Pipeline]) -> RealtimeScheduler:
        return RealtimeScheduler(
            pipelines, observer=self._timing,
            monotonic_ns=self._monotonic_ns, sleep=self._sleep,
        )

    def _report_outcome(self) -> None:
        if self._ingress_crashed.is_set():
            self._control.complete(
                success=False,
                message=f"Ingress pipeline crashed: {self._ingress_crash_message}",
            )
            logger.error("Stopped due to ingress crash.")

        elif self._control.should_stop():
            self._control.complete(
                success=False,
                message="Worker stopped by server.",
            )
            logger.info("Stopped.")

        else:
            self._control.complete(
                success=True,
                message="Pipeline completed.",
            )
            logger.info("Complete!")

    def run(self):
        logger.info("Starting worker control.")
        self._control.start()

        try:
            if self._replay_window is not None:
                self._run_replay(*self._replay_window)
            else:
                self._run_realtime()

            self._report_outcome()

        except Exception as exc:
            self._control.complete(
                success=False,
                message=f"Worker failed: {exc}",
            )
            raise

        finally:
            # Whatever ended the compute scheduler (server stop, ingress
            # crash, completion, or a compute exception), tell the ingress
            # schedulers to exit too — otherwise the joins below wait out
            # their full timeout.
            self._control.request_stop()
            self._control.stop()
            for thread in self._ingress_threads:
                thread.join(timeout=5)
            self._writer.close()
