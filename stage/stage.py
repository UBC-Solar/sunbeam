from typing import ClassVar
from abc import ABC, abstractmethod

from data_tools.localization import CanonicalName

from state.frame import Frame, FrameView


class Stage(ABC):
    stage_name: ClassVar[str]
    inputs: ClassVar[list[CanonicalName]]
    outputs: ClassVar[list[CanonicalName]]
    frequency: ClassVar[float]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        if "stage_name" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must override 'stage_name'")

        if "inputs" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must override 'inputs'")

        if "outputs" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must override 'outputs'")

        if "frequency" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must override 'frequency'")

    @abstractmethod
    def run(self, input_frame: FrameView) -> Frame: ...
