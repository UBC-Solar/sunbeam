from data_tools.localization import CanonicalName
from state.frame import Frame, FrameView
from typing import ClassVar
from stage.stage import Stage


class Efficiency(Stage):
    stage_name: ClassVar[str] = "Efficiency"
    inputs: ClassVar[list[str]] = [CanonicalName.MotorPower, CanonicalName.VehicleSpeed]
    outputs: ClassVar[list[str]] = [CanonicalName.MotorEfficiency]
    frequency_hz: ClassVar[float] = 5

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        motor_power = input_frame.read(CanonicalName.MotorPower)
        vehicle_speed = input_frame.read(CanonicalName.VehicleSpeed)

        motor_efficiency = motor_power / vehicle_speed
        new_frame.write(CanonicalName.MotorEfficiency, motor_efficiency)

        return new_frame
