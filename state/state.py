from data_tools.localization import CanonicalName

from state.frame import FrameView, Frame
from datetime import datetime


class State:
    def __init__(self, values: dict[CanonicalName, float] = None):
        self._values = {} if not values else values

    def as_frame(self, signals: list[CanonicalName], timestamp: datetime) -> FrameView:
        frame = Frame(timestamp)

        for signal in signals:
            frame.write(signal, self._values[signal])

        return frame.as_view()

    def from_frame(self, frame: FrameView, signals: list[CanonicalName]) -> "State":
        for signal in signals:
            self._values[signal] = frame.read(signal)

        return self
