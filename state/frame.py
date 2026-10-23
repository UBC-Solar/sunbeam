from data_tools.localization import CanonicalName
from data_tools.collections import TimeSeries
from datetime import datetime
from numbers import Real

def _format_value(value) -> str:
    # Offline ingress frames hold whole TimeSeries rather than single samples
    return f"{value:.1f}" if isinstance(value, Real) else f"<{type(value).__name__} of {len(value)} points>"


class FrameView:
    def __init__(self, timestamp: datetime, values: dict[CanonicalName, float] = None):
        self._values = {} if not values else values
        self.timestamp = timestamp

    def read(self, signal: CanonicalName):
        return self._values[signal]

    def __repr__(self):
        return f"Frame({self.timestamp}) with {len(self._values)} values"

    def __str__(self):
        internal_str = [f"{str(signal)}: {_format_value(value)} \n" for signal, value in self._values.items()]
        return f"-- {self.timestamp} -- \n{"".join(internal_str)}"

    def __iter__(self):
        return iter(self._values.items())


class Frame(FrameView):
    def write(self, signal: CanonicalName, value: float | TimeSeries) -> None:
        self._values[signal] = value

    def as_view(self) -> FrameView:
        return FrameView(self.timestamp, values=self._values)

    @staticmethod
    def from_view(view: FrameView) -> "Frame":
        return Frame(view.timestamp)
