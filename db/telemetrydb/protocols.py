from datetime import timezone, datetime
from typing import Protocol

class TimeProvider(Protocol):
    def now(self, tz: timezone) -> datetime: ...