from data_tools.schema import FileLoader
from data_tools import Event
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from data_tools.collections import TimeSeries
from data_tools.lap_tools import FSGPDayLaps
from prefect import task
from numpy.typing import NDArray
from physics.environment.gis.gis import GIS
import numpy as np
import copy


NCM_LAP_LEN_M = 5033.62413472


fsgp_lap_days = {
    "FSGP_2024_Day_1": 1,
    "FSGP_2024_Day_2": 2,
    "FSGP_2024_Day_3": 3
}


class LocalizationStage(Stage):
    @classmethod
    def get_stage_name(cls):
        return "localization"

    @staticmethod
    def dependencies():
        return ["ingress"]

    @staticmethod
    @task(name="Localization")
    def run(self,
            gps_latitude_df_loader: FileLoader,
            gps_longitude_df_loader: FileLoader,
            vehicle_velocity_loader: FileLoader) -> tuple[FileLoader, ...]:
        """
        Run the localization stage, which computes various metrics relating to the car's location in a track race.

        Outputs will be FileLoaders pointing to None for non-track events, or if the prerequisite data is unavailable.
        1. LapIndex
            The best available LapIndex data for the event. Prioritizes LapIndexSpreadsheet > LapIndexIntegratedSpeed.
        2. TrackIndex
            The best available TrackIndex data for the event. Currently only TrackIndexSpreadsheet is available, but GPS
            TrackIndex is coming soon.
        3. LapIndexIntegratedSpeed
            Do not use this unless it is the only option. Integrates speed and tiles by track length to approximate
            the lap index we are on at any given time. Lap index is the integer number of laps we have completed
            around the track (starting at zero).
        4. LapIndexSpreadsheet
            Uses data from the FSGP timing spreadsheet (via FSGPDayLaps) to determine lap index.
            Lap index is the integer number of laps we have completed around the track
            at any given time (starting at zero).
        5. TrackDistanceSpreadsheet
            Uses data from the FSGP timing spreadsheet (via FSGPDayLaps) to determine lap splits, then
            integrates speed over the current lap to determine distance travelled along the track.
        6. TrackIndexSpreadsheet
            Integrates speed within a lap to determine the car's coordinate within the track. We use a list of
            lat/lon pairs to represent a track's indices, and round to the nearest one. References the FSGP timing
            spreadsheet (via FSGPDayLaps) for lap start/stop times.
        7. GPSLatitude
            Latitude TimeSeries of the car in degrees, filtered for anomalies.
        8. GPSLongitude
            Latitude TimeSeries of the car in degrees, filtered for anomalies.

        :param self: an instance of LocalizationStage to be run
        :param FileLoader gps_latitude_df_loader: loader to GPSLatitude dataframe from Ingress
        :param FileLoader gps_longitude_df_loader: loader to GPSLongitude dataframe from Ingress
        :param FileLoader vehicle_velocity_loader: loader to VehicleVelocity from Ingress
        :returns: LapIndexIntegratedSpeed, LapIndexSpreadsheet, TrackDistanceSpreadsheet, TrackIndexSpreadsheet (FileLoaders pointing to TimeSeries)
        """
        return super().run(self, gps_latitude_df_loader, gps_longitude_df_loader, vehicle_velocity_loader)

    @property
    def event_name(self) -> str:
        return self._event_name

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
        self._event_name = event.name

    def extract(self,
                gps_latitude_df_loader: FileLoader,
                gps_longitude_df_loader: FileLoader,
                vehicle_velocity_loader: FileLoader) -> tuple[Result, Result, Result, Result]:
        gps_latitude_df_result: Result = gps_latitude_df_loader()
        gps_longitude_df_result: Result = gps_longitude_df_loader()
        vehicle_velocity_result: Result = vehicle_velocity_loader()

        try:
            coords_result = Result.Ok(np.abs(np.array(self.stage_data[self.event_name]["coords"]["coordinates"])))

        except KeyError:
            coords_result = Result.Err(KeyError(f"No coordinates found for event {self._event_name}! \n"))

        return gps_latitude_df_result, gps_longitude_df_result, vehicle_velocity_result, coords_result

    def transform(self, gps_latitude_df_result, gps_longitude_df_result, vehicle_velocity_result, coords_result) -> tuple[Result, ...]:
        if not coords_result:
            self.logger.error(f"Localization Stage for {self.event_name} has no coordinates!")
            gps_latitude_result = Result.Err(RuntimeError("Failed to process GPSLatitude!"))
            gps_longitude_result = Result.Err(RuntimeError("Failed to process GPSLongitude!"))
            track_index_gps_result = Result.Err(RuntimeError("Failed to process TrackIndexGPS!"))
            lap_index_integrated_speed_result = Result.Err(RuntimeError("Failed to process LapIndexIntegratedSpeed!"))
            lap_index_spreadsheet_result = Result.Err(RuntimeError("Failed to process LapIndexSpreadsheet!"))
            track_distance_spreadsheet_result = Result.Err(RuntimeError("Failed to process TrackDistanceSpreadsheet!"))
            track_index_spreadsheet_result = Result.Err(RuntimeError("Failed to process TrackIndexSpreadsheet!"))
            lap_index_result = lap_index_integrated_speed_result
            track_index_result = track_index_spreadsheet_result

            return (
                lap_index_result,
                track_index_result,
                lap_index_integrated_speed_result,
                lap_index_spreadsheet_result,
                track_distance_spreadsheet_result,
                track_index_spreadsheet_result,
                gps_latitude_result,
                gps_longitude_result,
                track_index_gps_result
            )

        coords = coords_result.unwrap()

        # GPS lat, lon, track index
        if not (gps_latitude_df_result and gps_longitude_df_result):
            self.logger.error(f"Failed to unwrap GPS results!")
            gps_latitude_result = Result.Err(RuntimeError("Failed to process GPSLatitude!"))
            gps_longitude_result = Result.Err(RuntimeError("Failed to process GPSLongitude!"))
            track_index_gps_result = Result.Err(RuntimeError("Failed to process TrackIndexGPS!"))

        else:
            # !!!! FIX THE INVERTED VALUES !!!! - FIXME
            gps_latitude_df = gps_longitude_df_result.unwrap().data.rename(
                columns={'GPSLongitude': 'GPSLatitude'})
            gps_longitude_df = gps_latitude_df_result.unwrap().data.rename(
                columns={'GPSLatitude': 'GPSLongitude'})

            # create a box with 0.01deg lat/lon padding around track
            # 1deg is ~100km (pi/180 * earth radius)
            padding_deg = 0.1  # ~10km
            min_latitude = np.min(coords[:, 0] - padding_deg)
            max_latitude = np.max(coords[:, 0] + padding_deg)
            min_longitude = np.min(coords[:, 1] - padding_deg)
            max_longitude = np.max(coords[:, 1] + padding_deg)

            valid_latitude = np.logical_and(
                gps_latitude_df.GPSLatitude > min_latitude,
                gps_latitude_df.GPSLatitude < max_latitude
            )
            valid_longitude = np.logical_and(
                gps_longitude_df.GPSLongitude > min_longitude,
                gps_longitude_df.GPSLongitude < max_longitude
            )
            filtered_gps_indices = np.logical_and(valid_latitude, valid_longitude)
            filtered_gps_latitude_df = gps_latitude_df.loc[filtered_gps_indices]
            filtered_gps_longitude_df = gps_longitude_df.loc[filtered_gps_indices]

            gps_latitude_ts = TimeSeries.from_query_dataframe(
                filtered_gps_latitude_df, 0.25, "GPSLatitude", "deg")
            gps_longitude_ts = TimeSeries.from_query_dataframe(
                filtered_gps_longitude_df, 0.25, "GPSLongitude", "deg")

            gps_latitude_result = Result.Ok(gps_latitude_ts)
            gps_longitude_result = Result.Ok(gps_longitude_ts)

            track_index_gps_ts = self._get_gps_track_index(gps_latitude_ts, gps_longitude_ts, coords)
            track_index_gps_result = Result.Ok(track_index_gps_ts)


        # lap index integrated speed, lap/track index from spreadsheet
        try:
            vehicle_velocity_ts: TimeSeries = vehicle_velocity_result.unwrap().data

            lap_index_integrated_speed_result = self._get_lap_index_integrated_speed(vehicle_velocity_ts)

            lap_index_spreadsheet_ts = self._get_lap_index_spreadsheet(self.event, vehicle_velocity_ts)
            lap_index_spreadsheet_result = Result.Ok(lap_index_spreadsheet_ts)

            track_distance_spreadsheet_ts, track_index_spreadsheet_ts = self._get_track_index_spreadsheet(
                self.event, lap_index_spreadsheet_ts, vehicle_velocity_ts, coords
            )
            track_distance_spreadsheet_result = Result.Ok(track_distance_spreadsheet_ts)
            track_index_spreadsheet_result = Result.Ok(track_index_spreadsheet_ts)
        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap vehicle velocity result! \n {e}")
            lap_index_integrated_speed_result = Result.Err(RuntimeError("Failed to process LapIndexIntegratedSpeed!"))
            lap_index_spreadsheet_result = Result.Err(RuntimeError("Failed to process LapIndexSpreadsheet!"))
            track_distance_spreadsheet_result = Result.Err(RuntimeError("Failed to process TrackDistanceSpreadsheet!"))
            track_index_spreadsheet_result = Result.Err(RuntimeError("Failed to process TrackIndexSpreadsheet!"))

        # determine the best lap and track index available
        if lap_index_spreadsheet_result:
            lap_index_result = lap_index_spreadsheet_result
        else:
            lap_index_result = lap_index_integrated_speed_result
        track_index_result = track_index_spreadsheet_result

        return (lap_index_result,
                track_index_result,
                lap_index_integrated_speed_result,
                lap_index_spreadsheet_result,
                track_distance_spreadsheet_result,
                track_index_spreadsheet_result,
                gps_latitude_result,
                gps_longitude_result,
                track_index_gps_result)

    def load(self,
             lap_index_result,
             track_index_result,
             lap_index_integrated_speed_result,
             lap_index_spreadsheet_result,
             track_distance_spreadsheet_result,
             track_index_spreadsheet_result,
             gps_latitude_result,
             gps_longitude_result,
             track_index_gps_result) -> tuple[FileLoader, ...]:

        file_details = {
            "LapIndex": {
                "data": lap_index_result.unwrap() if lap_index_result else None,
                "description": "The best available LapIndex data for the event. "
                               "Prioritizes LapIndexSpreadsheet > LapIndexIntegratedSpeed."
            },
            "TrackIndex": {
                "data": track_index_result.unwrap() if track_index_result else None,
                "description": "The best available TrackIndex data for the event. Currently only TrackIndexSpreadsheet "
                               "is available, but GPS TrackIndex is coming soon."
            },
            "LapIndexIntegratedSpeed": {
                "data": lap_index_integrated_speed_result.unwrap() if lap_index_integrated_speed_result else None,
                "description": f"Estimate of the FSGP lap index in this event as a function of time. "
                               f"Value is estimated by integrating VehicleVelocity and tiling the result over the FSGP lap "
                               f"length of {NCM_LAP_LEN_M} meters."
            },
            "LapIndexSpreadsheet": {
                "data": lap_index_spreadsheet_result.unwrap() if lap_index_spreadsheet_result else None,
                "description": "Uses data from the FSGP timing spreadsheet (via FSGPDayLaps) to determine lap index."
                               "Lap index is the integer number of laps we have completed around the track"
                               "at any given time (starting at zero)."
            },
            "TrackDistanceSpreadsheet": {
                "data": track_distance_spreadsheet_result.unwrap() if track_distance_spreadsheet_result else None,
                "description": "Uses data from the FSGP timing spreadsheet (via FSGPDayLaps) to determine lap splits, then "
                               "integrates speed over the current lap to determine distance travelled along the track."
            },
            "TrackIndexSpreadsheet": {
                "data": track_index_spreadsheet_result.unwrap() if track_index_spreadsheet_result else None,
                "description": "Uses data from the FSGP timing spreadsheet (via FSGPDayLaps) to determine lap splits, then "
                               "integrates speed over the current lap to determine track index."
            },
            "GPSLatitude": {
                "data": gps_latitude_result.unwrap() if gps_latitude_result else None,
                "description": "Latitude TimeSeries of the car in degrees, filtered for anomalies."
            },
            "GPSLongitude": {
                "data": gps_longitude_result.unwrap() if gps_longitude_result else None,
                "description": "Longitude TimeSeries of the car in degrees, filtered for anomalies."
            },
            "TrackIndexGPS": {
                "data": track_index_gps_result.unwrap() if track_index_gps_result else None,
                "description": "Track index based on nearest filtered GPS coordinates."
            }
        }

        file_loaders = []

        for name, details in file_details.items():
            file = File(
                canonical_path=CanonicalPath(
                    origin=self.context.title,
                    event=self.event_name,
                    source=self.get_stage_name(),
                    name=name,
                ),
                file_type=FileType.TimeSeries,
                data=details["data"],
                description=details["description"]
            )

            loader = self.context.data_source.store(file)
            self.logger.info(f"Successfully loaded {name}!")
            file_loaders.append(loader)

        return tuple(file_loaders)

    @staticmethod
    def _get_gps_track_index(lat: TimeSeries, lon: TimeSeries, coords: NDArray) -> TimeSeries:
        """Determine the closest Track Indices based on GPS coordinate cartesian distance.
        Note: Telemetry GPS data removes the sign for lat/lon (e.g Vancouver is -123.1deg but gets 123.1deg).
              This should be fine since we will not be travelling across 0deg (equator / prime meridian).
        """
        # abs_coords has dimension (n_track_indices, 2), where the second dimension is lat/lon
        # lat and lon have dimension (t)

        # Broadcast subtract to create a matrix with dimension (t, n_track_indices)
        lat_diffs = coords[:, 0][None, :] - lat[:, None]
        lon_diffs = coords[:, 1][None, :] - lon[:, None]

        # Create a matrix with dimension (t, n_track_indices) with cartesian distance values,
        # then take the minimum index along axis 1 to get the closest index.
        squared_distances = lat_diffs ** 2 + lon_diffs ** 2  # (t, n_track_indices)
        track_indices = np.argmin(squared_distances, axis=1)

        # convert to TimeSeries
        track_index_gps = lat.promote(track_indices)
        track_index_gps.name = "TrackIndexGPS"
        track_index_gps.units = "Track Index"

        return track_index_gps


    @staticmethod
    def _get_lap_index_integrated_speed(vehicle_velocity_ts: TimeSeries) -> Result[TimeSeries]:
        integrated_velocity_m = np.cumsum(vehicle_velocity_ts) * vehicle_velocity_ts.period
        lap_index_integrated_speed = vehicle_velocity_ts.promote(
            np.array([int(dist_m // NCM_LAP_LEN_M) for dist_m in integrated_velocity_m]))
        lap_index_integrated_speed.name = "LapIndexIntegratedSpeed"
        lap_index_integrated_speed.units = "Laps"
        return Result.Ok(lap_index_integrated_speed)

    @staticmethod
    def _get_track_index_spreadsheet(event: Event, lap_index_spreadsheet: TimeSeries, vehicle_velocity_ts: TimeSeries, coords: NDArray):
        if event.name not in fsgp_lap_days.keys():
            return None, None  # result is not defined

        route_data = {
            "path": coords,
            "elevations": np.zeros(len(coords)),
            "time_zones": np.zeros(len(coords)),
            "num_unique_coords": (len(coords) - 1)}

        # Create a GIS object
        starting_coords = [37.00107373, -86.36854755] # TODO: Unhard-code this??
        gis = GIS(route_data, starting_coords, current_coord=starting_coords)

        # get indices in time for when a new lap begins
        # set nans to -1 so that we have an increase when we begin lap 0
        lap_starts = np.nonzero(np.diff(np.nan_to_num(lap_index_spreadsheet, nan=-1)))[0] + np.array(1)
        velocity_split = np.split(vehicle_velocity_ts, lap_starts)
        velocity_by_lap = velocity_split[1:-1] # discard times outside of when we are doing laps

        track_distance: list[NDArray] = []
        track_indices: list[NDArray] = []
        for lap_velocity_array in velocity_by_lap:
            lap_total_distance_m = np.sum(lap_velocity_array) * vehicle_velocity_ts.period
            track_distance.append(np.cumsum(lap_velocity_array) * vehicle_velocity_ts.period)
            norm_factor = NCM_LAP_LEN_M / lap_total_distance_m  # normalize such that all laps appear to travel the same distance
            lap_distance_per_tick = lap_velocity_array * norm_factor * vehicle_velocity_ts.period
            track_indices.append(gis.calculate_closest_gis_indices(lap_distance_per_tick))

        track_distance_flat = np.concatenate(
            [np.full_like(velocity_split[0], np.nan)] + track_distance + [np.full_like(velocity_split[-1], np.nan)]
        )
        track_distance_ts = vehicle_velocity_ts.promote(track_distance_flat)
        track_distance_ts.name = "TrackDistanceSpreadsheet"
        track_distance_ts.units = "m"

        track_index_flat = np.concatenate(
            [np.zeros_like(velocity_split[0])] + track_indices + [np.full_like(velocity_split[-1], np.nan)]
        )
        track_index_ts = vehicle_velocity_ts.promote(track_index_flat)
        track_index_ts.name = "TrackIndexSpreadsheet"
        track_index_ts.units = "Track Index"
        return track_distance_ts, track_index_ts

    @staticmethod
    def _get_lap_index_spreadsheet(event: Event, vehicle_velocity_ts: TimeSeries):
        if event.name not in fsgp_lap_days.keys():
            return None  # result is not defined

        # we don't actually need vehicle_velocity here, but we need the time data & promote method
        # unfortunately the times are off by 7h (vancouver time offset)
        timezone_fix = 60 * 60 * 7
        unix_times: NDArray[float] = vehicle_velocity_ts.unix_x_axis + timezone_fix

        lap_info = FSGPDayLaps(fsgp_lap_days[event.name])
        num_laps = lap_info.get_lap_count()
        race_start_unix = lap_info.get_start_utc(1).timestamp()
        lap_finishes_unix: list[float] = [
            lap_info.get_finish_utc(lap_idx + 1).timestamp() for lap_idx in range(num_laps)
        ]
        lap_indices: NDArray = np.zeros(unix_times.shape)
        for i, time in enumerate(unix_times):
            if (time < race_start_unix) or (time > lap_finishes_unix[-1]):
                lap_indices[i] = np.nan
            else:
                lap_indices[i] = np.count_nonzero(time > np.array(lap_finishes_unix))

        lap_indices_ts = vehicle_velocity_ts.promote(lap_indices)
        lap_indices_ts.name = "LapIndexSpreadsheet"
        lap_indices_ts.units = "Laps"
        return lap_indices_ts

    def skip_stage(self):
        pass


stage_registry.register_stage(LocalizationStage.get_stage_name(), LocalizationStage)
