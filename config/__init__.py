from pathlib import Path


EVENTS_PATH = Path(__file__).parent / "events.toml"
VEHICLES_PATH = Path(__file__).parent / "vehicles.toml"
CONTEXT_PATH = Path(__file__).parent / "context.toml"

from .vehicles import VehicleManager
from .events import EventManager
from .signals import SignalManager

__all__ = [
    "EVENTS_PATH",
    "VEHICLES_PATH",
    "CONTEXT_PATH",
    "VehicleManager",
    "EventManager",
    "SignalManager",
]
