from data_tools.localization import CanonicalName
from state.frame import Frame, FrameView
from typing import ClassVar
from stage.stage import Stage


class Power(Stage):
    stage_name: ClassVar[str] = "Power"
    inputs: ClassVar[list[str]] = [CanonicalName.PackVoltage, CanonicalName.MotorCurrent]
    outputs: ClassVar[list[str]] = [CanonicalName.MotorPower]
    frequency: ClassVar[float] = 5

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        motor_voltage = input_frame.read(CanonicalName.PackVoltage)
        motor_current = input_frame.read(CanonicalName.MotorCurrent)

        motor_power = motor_voltage * motor_current
        new_frame.write(CanonicalName.MotorPower, motor_power)

        return new_frame
