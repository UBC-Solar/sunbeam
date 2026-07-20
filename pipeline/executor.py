from db.sunbeamdb.writer import EventWriter
from db.sunbeamdb.queued_writer import QueuedEventWriter
from sqlalchemy import Engine

from pipeline.pipeline import Pipeline
from pipeline.pipeline_generator import PipelineGenerator
from pipeline.protocols import FrameWriter
from config import EventManager
from stage.stage_library import StageLibrary
from pipeline.timing import TimingStats
from pipeline.scheduler import Scheduler
from pipeline.output import RichOutputManager, LoggingOutputManager
from state.state import State
import logging
import threading
import time
from collections.abc import Callable
from typing import Optional

from orchestration.control import ServerlessWorkerControl, OrchestratedWorkerControl, WorkerControl

logger = logging.getLogger("sunbeam.worker")


def build_event_pipelines(
        event_name: str,
        event_manager: Optional[EventManager] = None,
        stage_library: Optional[StageLibrary] = None,
) -> tuple[list[Pipeline], list[Pipeline]]:
    """
    Build the compute and ingress pipelines for an event from its
    configuration (events.toml + stage registry).
    """
    event_manager = event_manager or EventManager()
    event_datetime = event_manager.get_event_date(event_name)
    pipeline_stage_names = event_manager.get_stages_for_event(event_name)
    stage_library = stage_library or StageLibrary(event_manager.get_event_pipeline_edition(event_name))

    pipeline_stage_definitions = stage_library.get_stages_by_names(pipeline_stage_names)
    kwargs = {"event_name": event_name}
    pipeline_stages = [stage(**kwargs) for stage in pipeline_stage_definitions]

    logger.info(
        "Loaded %d stages for event %r: %s",
        len(pipeline_stages), event_name, pipeline_stage_names,
    )

    return PipelineGenerator.generate_pipeline_from_nodes(
        pipeline_stages,
        event_datetime.date(),
        stage_library=stage_library
    )


class Executor:
    def __init__(
            self,
            pipelines: list[Pipeline],
            ingress_pipelines: list[Pipeline],
            writer: FrameWriter,
            control: Optional[WorkerControl] = None,
            *,
            output_manager=None,
            monotonic_ns: Callable[[], int] = time.monotonic_ns,
            sleep: Callable[[float], None] = time.sleep,
    ):
        self._control = control or ServerlessWorkerControl()
        self._writer = writer
        self._pipelines = pipelines
        self._ingress_pipelines = ingress_pipelines
        self._output_manager = output_manager
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
        self._compute_scheduler = Scheduler(
            self._pipelines, observer=self._timing,
            monotonic_ns=monotonic_ns, sleep=sleep,
        )
        # One scheduler (and later, one thread) per ingress pipeline: ingress
        # runs are blocking I/O, so a slow query in one frequency bin must not
        # stall the others.
        self._ingress_schedulers = [
            Scheduler(
                [pipeline], observer=self._timing,
                monotonic_ns=monotonic_ns, sleep=sleep,
            )
            for pipeline in self._ingress_pipelines
        ]

        self._ingress_crashed = threading.Event()
        self._ingress_crash_message: Optional[str] = None
        self._completed = threading.Event()

    @classmethod
    def from_event(
            cls,
            event_name: str,
            engine: Engine,
            reprocess: bool = False,
            control: Optional[WorkerControl] = None,
    ) -> "Executor":
        writer = QueuedEventWriter(EventWriter(event_name, engine, reprocess=reprocess))
        pipelines, ingress_pipelines = build_event_pipelines(event_name)

        return cls(pipelines, ingress_pipelines, writer, control)

    def is_stopped(self) -> bool:
        return self._stopped

    def signal_completion(self) -> None:
        """
        Mark the event as finished: both schedulers wind down and the worker
        reports success. Intended to be called by whatever eventually detects
        end-of-data (e.g. an offline ingress running past the event's end).
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

    def _run_ingress_scheduler(self, scheduler: Scheduler):
        try:
            scheduler.run_forever(
                self._state,
                on_output=self._handle_pipeline_output,
                stop_on_error=True,
                should_stop=self._should_stop,
            )
        except Exception as exc:
            logger.exception("Ingress scheduler crashed")
            self._ingress_crash_message = str(exc)
            self._ingress_crashed.set()

    def _resolve_output_manager(self):
        if self._output_manager is not None:
            return self._output_manager

        if isinstance(self._control, OrchestratedWorkerControl):
            return LoggingOutputManager(
                self._timing,
                self._control,
                writer_stats=getattr(self._writer, "stats", None),
            )

        return RichOutputManager(self._timing)

    def run(self):
        logger.info("Starting worker control.")
        self._control.start()

        logger.info("Starting %d ingress scheduler(s).", len(self._ingress_schedulers))
        ingress_threads = [
            threading.Thread(
                target=self._run_ingress_scheduler,
                args=(scheduler,),
                name=f"sunbeam-ingress-{i}",
                daemon=True,
            )
            for i, scheduler in enumerate(self._ingress_schedulers)
        ]
        for thread in ingress_threads:
            thread.start()

        output_manager_cm = self._resolve_output_manager()

        try:
            logger.info("Starting compute scheduler.")
            with output_manager_cm as output_manager:
                self._compute_scheduler.run_forever(
                    self._state,
                    on_tick=output_manager.on_tick,
                    on_output=self._handle_pipeline_output,
                    should_stop=self._should_stop,
                )

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
            for thread in ingress_threads:
                thread.join(timeout=5)
            self._writer.close()