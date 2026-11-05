from typing import ClassVar
from abc import ABC, abstractmethod

from data_tools.localization import CanonicalName

from state.frame import Frame, FrameView


class Stage(ABC):
    stage_name: ClassVar[str]
    inputs: ClassVar[list[CanonicalName]]
    outputs: ClassVar[list[CanonicalName]]
    frequency: ClassVar[float]

    # A stage must write every declared output on every run, or the pipeline raises. Realtime ingress is the
    # exception: it omits a signal that had no data this poll (and tracks how long that lasts itself).
    may_omit_outputs: ClassVar[bool] = False

    def __init__(self, **kwargs):
        pass

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        # Inherited counts, so an edition can subclass another edition's stage and change only what differs
        for attribute in ("stage_name", "inputs", "outputs", "frequency"):
            if not hasattr(cls, attribute):
                raise TypeError(f"{cls.__name__} must override {attribute!r}")

    @abstractmethod
    def run(self, input_frame: FrameView) -> Frame: ...
