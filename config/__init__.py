from pathlib import Path


EVENTS_PATH = Path(__file__).parent / "events.toml"
VEHICLES_PATH = Path(__file__).parent / "vehicles.toml"

from .vehicles import VehicleManager
from .events import EventManager
from .signals import SignalManager

__all__ = [
    "EVENTS_PATH",
    "VEHICLES_PATH",
    "VehicleManager",
    "EventManager",
    "SignalManager",
]
