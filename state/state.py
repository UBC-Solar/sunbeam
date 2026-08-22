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
                print(f"Signal: {signal}")
                print(f"Values: {self._values}")
                value = self._values[signal]
            
                if isinstance(value, TimeSeries):
                    print(f"{signal} (Timeseries): {value[timestamp]} @ {timestamp} -> frame")
                    frame.write(signal, value[timestamp])
                else:
                    frame.write(signal, value)  

            return frame.as_view()

    def from_frame(self, frame: FrameView, signals: list[CanonicalName]) -> "State":
        with self._lock:
            for signal in signals:
                try:
                    self._values[signal] = frame.read(signal)

                except KeyError:  # Failed to get output, likely should raise some concern in the future
                    continue
            return self
