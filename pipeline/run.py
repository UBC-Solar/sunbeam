from data_tools import DataSource
from prefect import flow
from logs import SunbeamLogger
from data_source import DataSourceFactory
from pipeline.configure import build_config
from dotenv import load_dotenv
from stage import (Context, IngressStage, EnergyStage, PowerStage,
                   WeatherStage, EfficiencyStage, LocalizationStage, CleanupStage, ArrayStage)

logger = SunbeamLogger("sunbeam")


load_dotenv()


@flow(log_prints=True)
def run_sunbeam(git_target="pipeline", ingress_to_skip=None, stages_to_skip=None):
    if stages_to_skip is None:
        stages_to_skip = []

    if ingress_to_skip is None:
        ingress_to_skip = []

    sunbeam_config, data_source_config, ingress_config, targets, events = build_config()

    data_source: DataSource = DataSourceFactory.build(data_source_config.data_source_type, data_source_config)
    context: Context = Context(git_target, data_source, stages_to_skip)  # Set the global context

    ingress_stage: IngressStage = IngressStage(ingress_config)

    ingress_outputs: dict = IngressStage.run(ingress_stage, targets, events, ingress_to_skip)

    # We will process each event separately.
    for event in events:

        cleanup_stage: CleanupStage = CleanupStage(event)
        speed_mps, = CleanupStage.run(
            cleanup_stage,
            ingress_outputs[event.name]["VehicleVelocity"],
            ingress_outputs[event.name]["MotorRotatingSpeed"],
        )

        power_stage: PowerStage = PowerStage(event)
        pack_power, motor_power = PowerStage.run(
            power_stage,
            ingress_outputs[event.name]["TotalPackVoltage"],
            ingress_outputs[event.name]["PackCurrent"],
            ingress_outputs[event.name]["BatteryVoltage"],
            ingress_outputs[event.name]["BatteryCurrent"],
            ingress_outputs[event.name]["BatteryCurrentDirection"],
        )

        array_stage: ArrayStage = ArrayStage(event)
        array_power, = ArrayStage.run(
            array_stage,
            ingress_outputs[event.name]["ArrayVoltageStringA"],
            ingress_outputs[event.name]["ArrayVoltageStringB"],
            ingress_outputs[event.name]["ArrayVoltageStringC"],
            ingress_outputs[event.name]["ArrayCurrentStringA"],
            ingress_outputs[event.name]["ArrayCurrentStringB"],
            ingress_outputs[event.name]["ArrayCurrentStringC"],
        )

        energy_stage: EnergyStage = EnergyStage(event)
        (
            integrated_pack_power,
            energy_vol_extrapolated,
            energy_from_integrated_power,
            unfiltered_soc,
            soc
        ) = EnergyStage.run(
            energy_stage,
            ingress_outputs[event.name]["VoltageofLeast"],
            pack_power,
            ingress_outputs[event.name]["TotalPackVoltage"],
            ingress_outputs[event.name]["PackCurrent"]
        )

        localization_stage: LocalizationStage = LocalizationStage(event)
        (lap_index, track_index, lap_index_integrated_speed, lap_index_spreadsheet, track_distance_spreadsheet,
         track_index_spreadsheet, gps_latitude, gps_longitude, track_index_gps) = LocalizationStage.run(
            localization_stage,
            ingress_outputs[event.name]["GPSLatitude"],
            ingress_outputs[event.name]["GPSLongitude"],
            speed_mps,
        )

        efficiency_stage: EfficiencyStage = EfficiencyStage(event)
        efficiency_5min, efficiency_1h, efficiency_lap_distance = EfficiencyStage.run(
            efficiency_stage,
            speed_mps,
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
