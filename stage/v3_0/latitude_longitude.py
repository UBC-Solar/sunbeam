from data_tools.localization import CanonicalName
from state.frame import Frame, FrameView
from typing import ClassVar
from stage.stage import Stage

from pathlib import Path
import numpy as np
import tomllib

COORDINATES_DIR = Path(__file__).parent / "localization"


class LatitudeLongitude(Stage):
    stage_name: ClassVar[str] = "LatitudeLongitude"
    inputs: ClassVar[list[str]] = [CanonicalName.LatitudeRaw, CanonicalName.LongitudeRaw]
    outputs: ClassVar[list[str]] = [CanonicalName.LatitudeFiltered, CanonicalName.LongitudeFiltered]
    frequency: ClassVar[float] = 3

    def __init__(self, event_name: str | None = None, **kwargs):
        self.min_lat = self.max_lat = self.min_lon = self.max_lon = None
        if event_name is None:
            return
        
        coords_path = COORDINATES_DIR / event_name / "coords.toml" 
        if not coords_path.exists():
            return
        
        with open(coords_path, "rb") as f:
            data = tomllib.load(f)
        
        # abs because telemetry GPS data removes the sign for lat/lon. we remove sign off track coords to match
        coords = np.abs(np.array(data["coordinates"]))
        
        # create a box with 0.01deg lat/lon padding around track
        # 1deg is ~100km (pi/180 * earth radius)
        padding = 0.01 # ~1km 
        self.min_lat = np.min(coords[:, 0]) - padding
        self.max_lat = np.max(coords[:, 0]) + padding
        self.min_lon = np.min(coords[:, 1]) - padding
        self.max_lon = np.max(coords[:, 1]) + padding



    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)
        latitude = input_frame.read(CanonicalName.LatitudeRaw)
        longitude = input_frame.read(CanonicalName.LongitudeRaw)

        if self.in_bounds(latitude, longitude):
            new_frame.write(CanonicalName.LatitudeFiltered, latitude)
            new_frame.write(CanonicalName.LongitudeFiltered, longitude)

        return new_frame # if empty frame. accounted for?

    def in_bounds(self, lat: float, lon: float) -> bool:
        if None in (self.min_lat, self.max_lat, self.min_lon, self.max_lon):
            return True
        else:
            return (
                self.min_lat <= lat <= self.max_lat and
                self.min_lon <= lon <= self.max_lon
            )