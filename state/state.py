import threading
from datetime import datetime

from data_tools.collections import TimeSeries
from data_tools.localization import CanonicalName

from state.frame import Frame, FrameView


class State:
    def __init__(self, values: dict[CanonicalName, float] | None = None):
        self._values = {} if not values else values
        self._lock = threading.RLock()

    def as_frame(self, signals: list[CanonicalName], timestamp: datetime) -> FrameView:
        with self._lock:
            frame = Frame(timestamp)

            for signal in signals:
                value = self._values[signal]
            
                if isinstance(value, TimeSeries):
                    # Commented out debug code to track what is being written to frames
                    # print("==================")
                    # print(f"{signal} (Timeseries): {value[timestamp]} @ {timestamp} -> frame")
                    frame.write(signal, value[timestamp])
                else:
                    # print("==================")
                    # print(f"{signal} (Float): {value} @ {timestamp} -> frame")
                    frame.write(signal, value)  

            return frame.as_view()

    def timeseries_bounds(self) -> tuple[datetime, datetime] | None:
        """ Returns the time range over which every TimeSeries in the state has data

        :return tuple[datetime, datetime] | None: (latest start, earliest stop), or None if the state holds no TimeSeries
        """
        with self._lock:
            series = [value for value in self._values.values() if isinstance(value, TimeSeries)]

            if not series:
                return None

            return max(ts.start for ts in series), min(ts.stop for ts in series)

    def from_frame(self, frame: FrameView, signals: list[CanonicalName]) -> "State":
        with self._lock:
            for signal in signals:
                try:
                    self._values[signal] = frame.read(signal)

                except KeyError:  # Failed to get output, likely should raise some concern in the future
                    continue
            return self
