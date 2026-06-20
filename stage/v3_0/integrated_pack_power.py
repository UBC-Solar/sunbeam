from data_tools.localization import CanonicalName
from state.frame import Frame, FrameView
from typing import ClassVar
from stage.stage import Stage

class IntegratedPackPower(Stage):
    stage_name: ClassVar[str] = "IntegratedPackPower"
    inputs: ClassVar[list[str]] = [CanonicalName.PackPower]
    outputs: ClassVar[list[str]] = [CanonicalName.IntegratedPackPower]
    frequency: ClassVar[float] = 5 

    def __init__(self):
        self.total = 0
        self.last_timestamp = None

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)
        pack_power = input_frame.read(CanonicalName.PackPower)
        seconds_per_hour = 3600

        if self.last_timestamp is None:
            dt = 1.0 / self.frequency # for the first tick, estimate the period as we don't have two timestamps yet
        else:
            dt = (input_frame.timestamp - self.last_timestamp).total_seconds()

        self.total += pack_power * dt / seconds_per_hour
        self.last_timestamp = input_frame.timestamp
        new_frame.write(CanonicalName.IntegratedPackPower, self.total)

        return new_frame
