from data_tools.localization import CanonicalName
from data_tools.collections import TimeSeries

from state.frame import FrameView, Frame
from datetime import datetime
import threading

class State:
    def __init__(self, values: dict[CanonicalName, float] = None):
        self._values = {} if not values else values
        self._lock = threading.RLock()

    def as_frame(self, signals: list[CanonicalName], timestamp: datetime) -> FrameView:
        with self._lock:
            frame = Frame(timestamp)

            for signal in signals:
                if not isinstance(signal, TimeSeries):
                    frame.write(signal, self._values[signal])
                    print(f"This is not a TimeSeries, value is: {signal}")
                else:
                    frame.write(signal[timestamp], self._values[signal])
                    print(f"This is a TimeSeries, value is: {signal}, time is: {timestamp}")

            return frame.as_view()

    def from_frame(self, frame: FrameView, signals: list[CanonicalName]) -> "State":
        with self._lock:
            for signal in signals:
                try:
                    self._values[signal] = frame.read(signal)

                except KeyError:  # Failed to get output, likely should raise some concern in the future
                    continue
            return self
