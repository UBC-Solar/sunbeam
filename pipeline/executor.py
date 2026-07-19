from db.sunbeamdb.writer import EventWriter
from db.sunbeamdb.queued_writer import QueuedEventWriter
from sqlalchemy import Engine

from pipeline.pipeline_generator import PipelineGenerator
from config import EventManager
from stage.stage_library import StageLibrary
from pipeline.timing import TimingStats
from pipeline.scheduler import Scheduler
from pipeline.output import RichOutputManager, LoggingOutputManager
from datetime import datetime
from state.state import State
import logging
import threading
from typing import Optional

from orchestration.control import ServerlessWorkerControl, OrchestratedWorkerControl, WorkerControl

logger = logging.getLogger("sunbeam.worker")

class Executor:
    def __init__(
            self,
            event_name: str,
            engine: Engine,
            reprocess: bool = False,
            control: Optional[WorkerControl] = None
    ):
        self._control = control or ServerlessWorkerControl()

        writer = EventWriter(event_name, engine, reprocess=reprocess)
        self._writer = QueuedEventWriter(writer)

        event_manager = EventManager()
        event_datetime: datetime = event_manager.get_event_date(event_name)
        pipeline_stage_names = event_manager.get_stages_for_event(event_name)
        stage_library = StageLibrary(event_manager.get_event_pipeline_edition(event_name))

        pipeline_stage_definitions = stage_library.get_stages_by_names(pipeline_stage_names)
        kwargs = {"event_name": event_name}
        pipeline_stages = [stage(**kwargs) for stage in pipeline_stage_definitions]

        logger.info(
            "Loaded %d stages for event %r: %s",
            len(pipeline_stages), event_name, pipeline_stage_names,
        )

        self._pipelines, self._ingress_pipelines = PipelineGenerator.generate_pipeline_from_nodes(
            pipeline_stages,
            event_datetime.date(),
            stage_library=stage_library
        )
        self._state = State()

        logger.info(
            "Generated %d compute pipeline(s) and %d ingress pipeline(s): %s",
            len(self._pipelines), len(self._ingress_pipelines),
            [p.name for p in [*self._pipelines, *self._ingress_pipelines]],
        )

        pipelines_by_name = {
            pipeline.name: pipeline
            for pipeline in [*self._pipelines, *self._ingress_pipelines]
        }

        self._timing = TimingStats(pipelines_by_name)
        self._stopped = False
        self._compute_scheduler = Scheduler(self._pipelines, observer=self._timing)
        self._ingress_scheduler = Scheduler(self._ingress_pipelines, observer=self._timing)

        self._ingress_crashed = threading.Event()
        self._ingress_crash_message: Optional[str] = None

    def is_stopped(self) -> bool:
        return self._stopped

    def _handle_pipeline_output(self, pipeline, frame, timestamp):
        self._control.set_stage(pipeline.name)
        self._writer.write_frame(frame)

    def _should_stop(self) -> bool:
        return self._control.should_stop() or self._ingress_crashed.is_set()

    def _run_ingress_scheduler(self):
        try:
            self._ingress_scheduler.run_forever(
                self._state,
                on_output=self._handle_pipeline_output,
                stop_on_error=True,
                should_stop=self._control.should_stop,
            )
        except Exception as exc:
            logger.exception("Ingress scheduler crashed")
            self._ingress_crash_message = str(exc)
            self._ingress_crashed.set()

    def run(self):
        logger.info("Starting worker control.")
        self._control.start()

        logger.info("Starting ingress scheduler.")
        ingress_thread = threading.Thread(target=self._run_ingress_scheduler, daemon=True)
        ingress_thread.start()

        if isinstance(self._control, OrchestratedWorkerControl):
            output_manager_cm = LoggingOutputManager(self._timing, self._control)
        else:
            output_manager_cm = RichOutputManager(self._timing)

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
            self._control.stop()
            # Only works well if ingress scheduler can actually exit.
            ingress_thread.join(timeout=5)
