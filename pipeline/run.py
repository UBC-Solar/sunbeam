from data_tools import DataSource
from prefect import flow
import os
import sys
sys.path.append(os.getcwd())
from logs import SunbeamLogger
from data_source import DataSourceFactory
from stage import Context, PowerStage, StageResult, IngressStage, EnergyStage
from pipeline.configure import build_config, build_stage_graph


logger = SunbeamLogger("sunbeam")


@flow(log_prints=True)
def run_sunbeam(git_target="pipeline"):
    sunbeam_config, data_source_config, ingress_config, targets, events = build_config()

    stages_to_run = sunbeam_config.stages_to_run

    required_stages = build_stage_graph(stages_to_run)

    logger.info(f"Executing stages in order: {" -> ".join(required_stages)}")

    data_source: DataSource = DataSourceFactory.build(data_source_config.data_source_type, data_source_config)
    context: Context = Context(git_target, data_source, required_stages)  # Set the global context

    ingress_stage: IngressStage = IngressStage(ingress_config)

    ingress_outputs: StageResult = IngressStage.run(ingress_stage, targets, events)

    # We will process each event separately.
    for event in events:
        event_name = event.name
        event_ingress_outputs = ingress_outputs[event_name]

        power_stage: PowerStage = PowerStage(event_name)
        pack_power, motor_power = PowerStage.run(
            power_stage,
            ingress_outputs[event_name]["TotalPackVoltage"],
            ingress_outputs[event_name]["PackCurrent"],
            ingress_outputs[event_name]["BatteryVoltage"],
            ingress_outputs[event_name]["BatteryCurrent"],
            ingress_outputs[event_name]["BatteryCurrentDirection"],
        )

        energy_stage: EnergyStage = EnergyStage(event_name)
        pack_energy, = EnergyStage.run(energy_stage, pack_power)


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    run_sunbeam()
