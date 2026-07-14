from data_tools.localization import CanonicalName
from state.frame import Frame, FrameView
from typing import ClassVar
from stage.stage import Stage

class PackPower(Stage):
    stage_name: ClassVar[str] = "PackPower"
    inputs: ClassVar[list[str]] = [CanonicalName.PackVoltage, CanonicalName.PackCurrent]
    outputs: ClassVar[list[str]] = [CanonicalName.PackPower]
    frequency_hz: ClassVar[float] = 5

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        pack_voltage = input_frame.read(CanonicalName.PackVoltage)
        pack_current = input_frame.read(CanonicalName.PackCurrent)

        pack_power = pack_voltage * pack_current
        new_frame.write(CanonicalName.PackPower, pack_power)

        return new_frame