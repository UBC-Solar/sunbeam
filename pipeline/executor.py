import logging
import threading
import time
from collections.abc import Callable
from contextlib import ExitStack, nullcontext
from datetime import datetime, timedelta

from rich.console import Console
from sqlalchemy import Engine

from config.events import EventConfig
from db.sunbeamdb.queued_writer import QueuedEventWriter
from db.sunbeamdb.writer import EventWriter
from db.telemetrydb.protocols import IngressObserver, TimeProvider
from orchestration.control import ServerlessWorkerControl, WorkerControl
from pipeline.output import IngressProgressReporter, MetricsReporter, OutputManager, RichReplayOutputManager
from pipeline.pipeline import Pipeline
from pipeline.pipeline_generator import (
    IngressFactory,
    OfflineIngressFactory,
    RealtimeIngressFactory,
    generate_pipelines,
)
from pipeline.protocols import FrameWriter
from pipeline.replay_progress import ReplayProgress
from pipeline.scheduler import RealtimeScheduler, ReplayScheduler
from pipeline.timing import TimingStats
from stage.stage_library import StageLibrary
from state.state import State

logger = logging.getLogger("sunbeam.worker")

# Warn if telemetry covers noticeably less than the event window
REPLAY_TRIM_WARNING = timedelta(seconds=5)

# How often a replay reports its progress to the server (2 Hz), since one can be over in seconds
REPLAY_REPORT_INTERVAL_S = 0.5


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
    ingress fetches that whole window up front; otherwise it polls live.
    """
    stage_library = stage_library or StageLibrary(event.pipeline_edition)

    pipeline_stages = [stage(event_name=event.name) for stage in stage_library.get_stages_by_names(event.stages)]

    logger.info(
        "Loaded %d stages for event %r: %s",
        len(pipeline_stages), event.name, event.stages,
    )

    ingress_factory: IngressFactory
    if replay_window is not None:
        start, end = replay_window
        ingress_factory = OfflineIngressFactory(stage_library, start, end, observer=ingress_observer)
    else:
        time_provider: TimeProvider = datetime  # type: ignore[assignment]
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
            interactive: bool | None = None,
    ):
        """
        :param replay_window: (start, end) of a past event to replay as fast as possible, or None to run live.
            A replay runs every ingress pipeline once up front, then the compute pipelines over the window.
        :param output_manager: Replaces the display and server reporting chosen by _output_managers (for tests)
        :param ingress_reporter: Displays the up-front ingress queries of a replay; the ingress stages must have
            been built to report to it
        :param interactive: Whether output goes to a terminal that can be redrawn in place; detected if None
        """
        self._control = control or ServerlessWorkerControl()
        self._writer = writer
        self._pipelines = pipelines
        self._ingress_pipelines = ingress_pipelines
        self._output_manager = output_manager
        self._interactive = Console().is_terminal if interactive is None else interactive
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

        self._timing = TimingStats({p.name: p for p in [*self._pipelines, *self._ingress_pipelines]})

        # Set by a realtime ingress thread that crashed, which stops the compute scheduler
        self._ingress_error: Exception | None = None
        self._ingress_threads: list[threading.Thread] = []

    @classmethod
    def from_event(
            cls,
            event: EventConfig,
            engine: Engine,
            reprocess: bool = False,
            control: WorkerControl | None = None,
    ) -> "Executor":
        replay_window = replay_window_for(event)

        # A replay rewrites the whole event, so samples from any earlier run must be wiped first or they collide with
        # the new ones. Live runs only wipe when asked to, since they may be resuming an ongoing event.
        wipe = reprocess or replay_window is not None
        writer = QueuedEventWriter(EventWriter(event.name, engine, reprocess=wipe))

        ingress_reporter = IngressProgressReporter() if replay_window is not None else None
        pipelines, ingress_pipelines = build_event_pipelines(
            event, replay_window=replay_window, ingress_observer=ingress_reporter,
        )

        return cls(
            pipelines, ingress_pipelines, writer, control,
            replay_window=replay_window, ingress_reporter=ingress_reporter,
        )

    def run(self) -> None:
        """ Run until the replay window ends, the server says to stop, or something fails; then report which. """
        logger.info("Starting worker control.")
        self._control.start()

        error: Exception | None = None
        stopped_by_server = False
        try:
            if self._replay_window is not None:
                self._run_replay(*self._replay_window)
            else:
                self._run_realtime()
            stopped_by_server = self._control.should_stop() and self._ingress_error is None
        except Exception as exc:
            error = exc

        # Whatever ended the compute scheduler, tell the ingress schedulers to exit too, and wait for them
        self._control.request_stop()
        for thread in self._ingress_threads:
            thread.join(timeout=5)

        # Only report once everything is written, so a run is never shown as completed before its data is
        try:
            self._control.set_message("Waiting for database writes to finish")
            self._writer.close()
        except Exception as exc:
            if error is None:
                error = exc
            else:
                logger.exception("Database writer also failed")

        self._report_outcome(error, stopped_by_server)
        self._control.stop()

        if error is not None:
            raise error

    def _report_outcome(self, error: Exception | None, stopped_by_server: bool) -> None:
        if error is not None:
            success, message = False, f"Worker failed: {error}"
        elif self._ingress_error is not None:
            success, message = False, f"Ingress pipeline crashed: {self._ingress_error}"
        elif stopped_by_server:
            success, message = False, "Worker stopped by server."
        else:
            success, message = True, "Pipeline completed."

        (logger.info if success else logger.error)(message)
        self._control.complete(success=success, message=message)

    def _handle_pipeline_output(self, pipeline, frame, timestamp):
        self._control.set_stage(pipeline.name)
        self._writer.write_frame(frame)

    def _should_stop(self) -> bool:
        return self._control.should_stop() or self._ingress_error is not None

    def _output_managers(self, replay: ReplayProgress | None = None) -> list[OutputManager]:
        """
        Server reports always (ServerlessWorkerControl drops them). In a terminal: a progress bar for a replay, or
        the live timing table for a realtime run. Otherwise: log lines.
        """
        if self._output_manager is not None:
            return [self._output_manager]

        writer_stats = getattr(self._writer, "stats", None)

        if replay is None:
            return [MetricsReporter(
                self._timing, self._control,
                report_interval_s=1.0 if self._interactive else 5.0,
                log_interval_s=None if self._interactive else 5.0,
                table=self._interactive,
                writer_stats=writer_stats,
            )]

        reporter = MetricsReporter(
            self._timing, self._control,
            report_interval_s=REPLAY_REPORT_INTERVAL_S,
            log_interval_s=None if self._interactive else 5.0,
            writer_stats=writer_stats,
            replay=replay,
        )
        if self._interactive:
            return [RichReplayOutputManager(replay, writer_stats=writer_stats), reporter]
        return [reporter]

    def _run_scheduler(self, scheduler, managers: list[OutputManager]) -> None:
        with ExitStack() as stack:
            entered = [stack.enter_context(manager) for manager in managers]

            def on_tick(timestamp: datetime) -> None:
                for manager in entered:
                    manager.on_tick(timestamp)

            scheduler.run(
                self._state,
                on_tick=on_tick,
                on_output=self._handle_pipeline_output,
                should_stop=self._should_stop,
            )

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
        self._control.set_message("Fetching telemetry from InfluxDB")
        with self._ingress_reporter or nullcontext():
            for pipeline in self._ingress_pipelines:
                for frame in pipeline.run(self._state, event_start):
                    self._handle_pipeline_output(pipeline, frame, event_start)

        bounds = self._replay_bounds(event_start, event_end)
        if bounds is None:
            raise RuntimeError("No telemetry found for the event window; nothing to process.")

        start, end = bounds
        scheduler = ReplayScheduler(
            self._pipelines, observer=self._timing,
            now_wall=start, end_wall=end, monotonic_ns=self._monotonic_ns,
        )

        wall_start = time.monotonic()
        logger.info("Starting replay of %s -> %s.", f"{start:%H:%M:%S}", f"{end:%H:%M:%S}")
        self._control.set_message(f"Replaying {start:%Y-%m-%d %H:%M:%S} -> {end:%H:%M:%S} UTC")
        self._run_scheduler(scheduler, self._output_managers(ReplayProgress(start, end)))
        logger.info("Replayed %s -> %s in %.1f s.", f"{start:%H:%M:%S}", f"{end:%H:%M:%S}", time.monotonic() - wall_start)

    def _run_realtime(self) -> None:
        # One scheduler (and one thread) per ingress pipeline: ingress runs are blocking I/O, so a slow query in one
        # frequency bin must not stall the others. run() joins them on the way out.
        self._ingress_threads = [
            threading.Thread(
                target=self._run_ingress,
                args=(self._make_realtime_scheduler([pipeline]),),
                name=f"sunbeam-ingress-{i}",
                daemon=True,
            )
            for i, pipeline in enumerate(self._ingress_pipelines)
        ]

        logger.info("Starting %d ingress scheduler(s).", len(self._ingress_threads))
        for thread in self._ingress_threads:
            thread.start()

        logger.info("Starting compute scheduler.")
        self._run_scheduler(self._make_realtime_scheduler(self._pipelines), self._output_managers())

    def _run_ingress(self, scheduler: RealtimeScheduler) -> None:
        try:
            scheduler.run(self._state, on_output=self._handle_pipeline_output, should_stop=self._should_stop)
        except Exception as exc:
            logger.exception("Ingress scheduler crashed")
            self._ingress_error = exc

    def _make_realtime_scheduler(self, pipelines: list[Pipeline]) -> RealtimeScheduler:
        return RealtimeScheduler(
            pipelines, observer=self._timing,
            monotonic_ns=self._monotonic_ns, sleep=self._sleep,
        )
