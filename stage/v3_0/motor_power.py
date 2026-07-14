from data_tools.localization import CanonicalName
from state.frame import Frame, FrameView
from typing import ClassVar
from stage.stage import Stage


class MotorPower(Stage):
    stage_name: ClassVar[str] = "MotorPower"
    inputs: ClassVar[list[str]] = [CanonicalName.PackVoltage, CanonicalName.MotorCurrent, CanonicalName.MotorCurrentDirection]
    outputs: ClassVar[list[str]] = [CanonicalName.MotorPower]
    frequency_hz: ClassVar[float] = 5

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        motor_voltage = input_frame.read(CanonicalName.PackVoltage)
        motor_current = input_frame.read(CanonicalName.MotorCurrent)
        motor_current_direction = input_frame.read(CanonicalName.MotorCurrentDirection)

        # motor_current_direction is 1 if negative (regen) and 0 if positive (driving)
        # the linear function -2x + 1 maps 1 to -1 and 0 to 1,
        # resulting in a number that represents the sign/direction of the current
        motor_current_sign = motor_current_direction * -2 + 1

        motor_power = motor_voltage * motor_current * motor_current_sign
        new_frame.write(CanonicalName.MotorPower, motor_power)

        return new_frame
