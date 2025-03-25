from data_tools import DataSource
from prefect import flow
from logs import SunbeamLogger
from data_source import DataSourceFactory
from stage import Context, PowerStage, IngressStage, EnergyStage
from pipeline.configure import build_config, build_stage_graph
from stage.efficiency_stage import EfficiencyStage

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

    ingress_outputs: dict = IngressStage.run(ingress_stage, targets, events)

    # We will process each event separately.
    for event in events:
        event_name = event.name

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
        integrated_pack_power, energy_vol_extrapolated, energy_from_integrated_power = EnergyStage.run(
            energy_stage,
            ingress_outputs[event_name]["VoltageofLeast"],
            pack_power
        )
        efficiency_stage: EfficiencyStage = EfficiencyStage(event_name)
        efficiency_5min, efficiency_1h, efficiency_lap_distance = EfficiencyStage.run(
            efficiency_stage,
            ingress_outputs[event_name]["VehicleVelocity"],
            motor_power
        )


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    run_sunbeam()
