import datetime

from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import File, FileType, CanonicalPath
from data_tools.query import SolcastClient, SolcastPeriod, SolcastOutput
from data_tools import Event, TimeSeries
from prefect import task
import copy
import numpy as np
from numpy.typing import NDArray

NCM_MOTORSPORTS_PARK_LAT = 37.00293525
NCM_MOTORSPORTS_PARK_LON = -86.36660289

outputs = {
    SolcastOutput.wind_speed_10m: {
        "name": "WindSpeed10m",
        "description": "Wind speed at 10m above ground level.",
        "units": ""
    },
    SolcastOutput.wind_direction_10m: {
        "name": "WindDirection10m",
        "description": "Wind direction at 10m above ground level."
                       "Zero means true north. Varies from 0 to 360."
                       "A value of 270 means the wind is coming from the west",
        "units": "deg"
    },
    SolcastOutput.dhi: {
        "name": "DHI",
        "description": "The diffuse irradiance received on a horizontal surface. Also referred to as Diffuse Sky Radiation. The diffuse component is irradiance that is scattered by the atmosphere.",
        "units": ""
    },
    SolcastOutput.dni: {
        "name": "DNI",
        "description": "Irradiance received from the direction of the sun (10th percentile clearness). Also referred to as beam radiation.",
        "units": ""
    },
    SolcastOutput.ghi: {
        "name": "GHI",
        "description": "Total irradiance on a horizontal surface. The sum of direct and diffuse irradiance components received on a horizontal surface.",
        "units": ""
    },
    SolcastOutput.air_temperature: {
        "name": "AirTemperature",
        "description": "The air temperature at 2 meters above surface level.",
        "units": ""
    },
    SolcastOutput.precipitation_rate: {
        "name": "PrecipitationRate",
        "description": "Precipitation rate. An estimate of the average precipitation rate during the selected period - not an accumulated value.",
        "units": ""
    },
    SolcastOutput.zenith: {
        "name": "Zenith",
        "description": "The angle between the direction of the sun, and the zenith (directly overhead)."
                       "The zenith angle is 90 degrees at sunrise and sunset,"
                       "and 0 degrees when the sun is directly overhead.",
        "units": "deg"
    },
    SolcastOutput.azimuth: {
        "name": "Azimuth",
        "description": "The angle between the horizontal direction of the sun, and due north,"
                       "with negative angles eastwards and positive values westward ."
                       "Varies from -180 to 180. A value of -90 means the sun is in the east,"
                       "0 means north, and 90 means west.",
        "units": "deg"
    }
}
ordered_outputs = sorted(list(outputs.keys()))


class WeatherStage(Stage):
    @classmethod
    def get_stage_name(cls):
        return "weather"

    @staticmethod
    def dependencies():
        return []

    @staticmethod
    @task(name="Weather")
    def run(self) -> tuple[FileLoader, ...]:
        """
        Run the weather stage. Obtains the following values from Solcast, if the event is within +- 7 days
        of real time:
        - AirTemperature
        - Azimuth
        - DHI
        - DNI
        - GHI
        - PrecipitationRate
        - WindDirection10m
        - WindSpeed10m
        - Zenith

        :param self: an instance of WeatherStage to be run
        :returns: A tuple of FileLoaders with the data shown above, in alphabetical order.
                  Data is in `TimeSeries` format, or `None` if an error was encountered during querying.
        """
        return super().run(self)

    @property
    def event_name(self) -> str:
        return self._event.name

    @property
    def event(self) -> Event:
        """Get a copy of this stage's event"""
        return copy.deepcopy(self._event)

    def __init__(self, event: Event):
        """
        :param event: the event currently being processed
        """
        super().__init__()
        self._event = event

    def extract(self) -> tuple[NDArray, ...] | None:
        """Get a tuple of ndarrays corresponding to the requested outputs"""

        client = SolcastClient()

        start_time = self._event.start
        end_time = self._event.stop

        try:
            query_outputs: tuple[NDArray, ...] | None = client.query(
                NCM_MOTORSPORTS_PARK_LAT,
                NCM_MOTORSPORTS_PARK_LON,
                SolcastPeriod.PT5M,
                ordered_outputs,
                0,
                start_time,
                end_time,
                return_datetime=True
            )
        except ValueError as e:
            self.logger.error(f"Failed to query weather for {self.event_name}! \n {e}")
            query_outputs: tuple[NDArray | None, ...] = tuple([None for _ in ordered_outputs])

        return query_outputs

    def wrap_queried_data(self,
                          x_axis: np.ndarray[datetime.datetime],
                          solcast_output: SolcastOutput, data: NDArray) -> TimeSeries | None:
        if data is None: return None
        meta = {
            "start": x_axis[0],
            "stop": x_axis[-1],
            "period": (x_axis[1] - x_axis[0]).total_seconds(),
            "length": (x_axis[-1] - x_axis[0]).total_seconds(),
            "units": outputs[solcast_output]["units"],
        }
        ts = TimeSeries(data, meta)
        return ts

    def transform(self, *query_outputs) -> tuple[TimeSeries | None, ...]:
        """Transform the ndarrays into Results pointing to Timeseries"""

        x_axis: np.ndarray[datetime.datetime] = query_outputs[0]
        output_arrays = query_outputs[1:]

        results = tuple([self.wrap_queried_data(x_axis, solcast_output, arr)
                         for arr, solcast_output in zip(output_arrays, ordered_outputs)])

        return results

    def get_fileloader(self, ts_data, solcast_output):
        """Create a fileloader for a timeseries of solcast data"""

        file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=WeatherStage.get_stage_name(),
                name=outputs[solcast_output]["name"],
            ),
            file_type=FileType.TimeSeries,
            data=ts_data,
            description=outputs[solcast_output]["description"]
        )
        loader = self.context.data_source.store(file)
        self.logger.info(f"Successfully loaded {outputs[solcast_output]["name"]}!")

        return loader

    def load(self, *query_results) -> tuple[FileLoader, ...]:
        return tuple([self.get_fileloader(ts_data, solcast_output)
                      for ts_data, solcast_output in zip(query_results, ordered_outputs)])


stage_registry.register_stage(WeatherStage.get_stage_name(), WeatherStage)
