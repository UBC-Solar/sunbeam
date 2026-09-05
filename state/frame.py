from data_tools.localization import CanonicalName
from datetime import datetime

class FrameView:
    def __init__(self, timestamp: datetime, values: dict[CanonicalName, float] = None):
        self._values = {} if not values else values
        self.timestamp = timestamp

    def read(self, signal: CanonicalName):
        return self._values[signal]

    def __repr__(self):
        return f"Frame({self.timestamp}) with {len(self._values)} values"

    def __str__(self):
        internal_str = [f"{str(signal)}: {float(value):.1f} \n" for signal, value in self._values.items()]
        return f"-- {self.timestamp} -- \n{"".join(internal_str)}"

    def __iter__(self):
        return iter(self._values.items())


class Frame(FrameView):
    def write(self, signal: CanonicalName, value: float) -> None:
        self._values[signal] = value

    def as_view(self) -> FrameView:
        return FrameView(self.timestamp, values=self._values)

    @staticmethod
    def from_view(view: FrameView) -> "Frame":
        return Frame(view.timestamp)
