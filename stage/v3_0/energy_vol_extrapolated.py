from data_tools.localization import CanonicalName
from state.frame import Frame, FrameView
from typing import ClassVar
from stage.stage import Stage
from scipy.interpolate import CubicSpline
import tomllib
from pathlib import Path

class EnergyVOLExtrapolated(Stage):
    stage_name: ClassVar[str] = "EnergyVOLExtrapolated"
    inputs: ClassVar[list[str]] = [CanonicalName.MinimumModuleVoltage]
    outputs: ClassVar[list[str]] = [CanonicalName.EnergyVOLExtrapolated]
    frequency_hz: ClassVar[float] = 10

    def __init__(self, **kwargs):
        config_path = Path(__file__).parent / "energy" / "battery_configuration.toml"
        with open(config_path, "rb") as f:
            config = tomllib.load(f)

        voltage_wh_lookup = config["voltage_wh_lookup"]
        self.voltage_to_energy = CubicSpline(voltage_wh_lookup[0], voltage_wh_lookup[1])
        self.cells_in_module = config["cells_in_module"]
        self.modules_in_pack = config["modules_in_pack"]

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)
        min_voltage = input_frame.read(CanonicalName.MinimumModuleVoltage)

        energy_per_cell = float(self.voltage_to_energy(min_voltage))
        total_energy = energy_per_cell * self.cells_in_module * self.modules_in_pack

        new_frame.write(CanonicalName.EnergyVOLExtrapolated, total_energy)
        return new_frame

