from typing import ClassVar
from abc import ABC, abstractmethod
from state.frame import Frame, FrameView


class Node(ABC):
    node_name: ClassVar[str]
    inputs: ClassVar[list[str]]
    outputs: ClassVar[list[str]]
    rate: ClassVar[float]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        if "node_name" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must override 'node_name'")

        if "inputs" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must override 'inputs'")

        if "outputs" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must override 'outputs'")

        if "frequency" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must override 'frequency'")

    @abstractmethod
    def run(self, input_frame: FrameView) -> Frame: ...
