from pathlib import Path


VEHICLES_PATH = Path(__file__).parent / "vehicles.toml"

from .vehicles import VehicleManager
from .signals import SignalManager

__all__ = [
    "VEHICLES_PATH",
    "VehicleManager",
    "SignalManager",
]
