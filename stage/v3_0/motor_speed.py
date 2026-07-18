from data_tools.localization import CanonicalName
from state.frame import Frame, FrameView
from typing import ClassVar
from stage.stage import Stage


class MotorSpeed(Stage):
    stage_name: ClassVar[str] = "MotorSpeed"
    inputs: ClassVar[list[str]] = [CanonicalName.VehicleSpeed]
    outputs: ClassVar[list[str]] = [CanonicalName.MotorPower]
    frequency: ClassVar[float] = 5

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        motor_power = 10
        new_frame.write(CanonicalName.MotorPower, motor_power)

        return new_frame
