from data_tools.localization import CanonicalName
from state.frame import Frame, FrameView
from typing import ClassVar
from stage.stage import Stage

class Array(Stage):
    stage_name: ClassVar[str] = "Array"
    inputs: ClassVar[list[str]] = [
        CanonicalName.MPPTInputVoltageA,
        CanonicalName.MPPTInputCurrentA,
        CanonicalName.MPPTInputVoltageB,
        CanonicalName.MPPTInputCurrentB,
        CanonicalName.MPPTInputVoltageC,
        CanonicalName.MPPTInputCurrentC,
    ]
    outputs: ClassVar[list[str]] = [CanonicalName.ArrayPower]
    frequency: ClassVar[float] = 4

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        v_a = input_frame.read(CanonicalName.MPPTInputVoltageA)
        i_a = input_frame.read(CanonicalName.MPPTInputCurrentA)
        v_b = input_frame.read(CanonicalName.MPPTInputVoltageB)
        i_b = input_frame.read(CanonicalName.MPPTInputCurrentB)

        array_power = (v_a * i_a) + (v_b * i_b)
                                                   
        new_frame.write(CanonicalName.ArrayPower, array_power)

        return new_frame
