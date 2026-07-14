from data_tools.localization import CanonicalName
from state.frame import Frame, FrameView
from typing import ClassVar
from stage.stage import Stage

class EnergyFromIntegratedPower(Stage):
    stage_name: ClassVar[str] = "EnergyFromIntegratedPower"
    inputs: ClassVar[list[str]] = [CanonicalName.IntegratedPackPower, CanonicalName.EnergyVOLExtrapolated]
    outputs: ClassVar[list[str]] = [CanonicalName.EnergyFromIntegratedPower]
    frequency_hz: ClassVar[float] = 5

    def __init__(self, **kwargs):
        self.initial_energy = None

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)
        ipp = input_frame.read(CanonicalName.IntegratedPackPower)
        eve = input_frame.read(CanonicalName.EnergyVOLExtrapolated)

        if self.initial_energy is None:
            self.initial_energy = eve
        
        current_energy = self.initial_energy - ipp

        new_frame.write(CanonicalName.EnergyFromIntegratedPower, current_energy)

        return new_frame
