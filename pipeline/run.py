from data_tools import DataSource
from prefect import flow
from logs import SunbeamLogger
from data_source import DataSourceFactory
from pipeline.configure import build_config, build_stage_graph
from stage import (Context, IngressStage, EnergyStage, PowerStage,
                   WeatherStage, EfficiencyStage, LocalizationStage)

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

        power_stage: PowerStage = PowerStage(event)
        pack_power, motor_power = PowerStage.run(
            power_stage,
            ingress_outputs[event.name]["TotalPackVoltage"],
            ingress_outputs[event.name]["PackCurrent"],
            ingress_outputs[event.name]["BatteryVoltage"],
            ingress_outputs[event.name]["BatteryCurrent"],
            ingress_outputs[event.name]["BatteryCurrentDirection"],
        )

        energy_stage: EnergyStage = EnergyStage(event)
        integrated_pack_power, energy_vol_extrapolated, energy_from_integrated_power = EnergyStage.run(
            energy_stage,
            ingress_outputs[event.name]["VoltageofLeast"],
            pack_power,
            ingress_outputs[event.name]["TotalPackVoltage"],
            ingress_outputs[event.name]["PackCurrent"]
        )

        localization_stage: LocalizationStage = LocalizationStage(event)
        (lap_index, track_index, lap_index_integrated_speed,
         lap_index_spreadsheet, track_distance_spreadsheet, track_index_spreadsheet) = LocalizationStage.run(
            localization_stage,
            ingress_outputs[event.name]["VehicleVelocity"]
        )

        efficiency_stage: EfficiencyStage = EfficiencyStage(event)
        efficiency_5min, efficiency_1h, efficiency_lap_distance = EfficiencyStage.run(
            efficiency_stage,
            ingress_outputs[event.name]["VehicleVelocity"],
            motor_power,
            lap_index
        )

        weather_stage: WeatherStage = WeatherStage(event)
        (air_temperature, azimuth, dhi, dni, ghi, precipitation_rate,
         wind_direction_10m, wind_speed_10m, zenith) = WeatherStage.run(
            weather_stage,
        )


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    run_sunbeam()
