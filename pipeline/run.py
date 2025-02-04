from data_tools import DataSource
import logging
from prefect import flow
from logs import log_directory
from data_tools.utils import configure_logger
from data_source import DataSourceFactory
from stage import Context, PowerStage, StageResult, IngressStage
from pipeline.configure import build_config, build_stage_graph


logger = logging.getLogger("sunbeam")
configure_logger(logger, log_directory / "sunbeam.log")


@flow(log_prints=True)
def run_sunbeam(git_target="pipeline"):
    sunbeam_config, data_source_config, ingress_config, targets, events = build_config()

    stages_to_run = sunbeam_config.stages_to_run

    required_stages = build_stage_graph(stages_to_run)

    logger.info(f"Executing stages in order: {" -> ".join(required_stages)}")

    data_source: DataSource = DataSourceFactory.build(data_source_config.data_source_type, data_source_config)
    context: Context = Context(git_target, data_source, required_stages)

    ingress_stage: IngressStage = IngressStage(context, ingress_config)

    ingress_outputs: StageResult = IngressStage.run(ingress_stage, targets, events)

    # We will process each event separately.
    for event in events:
        event_name = event.name
        event_ingress_outputs = ingress_outputs[event_name]

        power_stage: PowerStage = PowerStage(context, event_name)
        pack_power, motor_power = PowerStage.run(
            power_stage,
            event_ingress_outputs["TotalPackVoltage"],
            event_ingress_outputs["PackCurrent"],
            event_ingress_outputs["BatteryCurrent"],
            event_ingress_outputs["BatteryVoltage"],
        )


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    run_sunbeam()
