from data_tools.schema import FileLoader
from data_tools import Event
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from data_tools.collections import TimeSeries
from data_tools.lap_tools import FSGPDayLaps
from prefect import task
from numpy.typing import NDArray
from scipy import integrate
from physics.environment.gis.gis import GIS
import numpy as np
import copy


NCM_LAP_LEN_M = 5033.62413472

reverse_coords = [[ 37.00107373, -86.36854755],
    [ 37.0011529 , -86.36837867],
    [ 37.00122817, -86.3682181 ],
    [ 37.00133071, -86.36801267],
    [ 37.00143614, -86.36779264],
    [ 37.00152389, -86.3675912 ],
    [ 37.00160574, -86.36740819],
    [ 37.00167596, -86.36725066],
    [ 37.00175285, -86.36709064],
    [ 37.00183166, -86.36691875],
    [ 37.00192538, -86.36670617],
    [ 37.00200136, -86.36653034],
    [ 37.00208623, -86.36635086],
    [ 37.00215644, -86.36619701],
    [ 37.00222549, -86.36603626],
    [ 37.00229839, -86.3658645 ],
    [ 37.00237732, -86.36569622],
    [ 37.00245038, -86.36553914],
    [ 37.00252912, -86.36537128],
    [ 37.00259904, -86.36521818],
    [ 37.00266755, -86.36507091],
    [ 37.00274639, -86.36490341],
    [ 37.00283342, -86.36471029],
    [ 37.00291248, -86.36454704],
    [ 37.00298517, -86.36439075],
    [ 37.0030636 , -86.36423803],
    [ 37.00313338, -86.36408574],
    [ 37.00320937, -86.36393701],
    [ 37.00330797, -86.36377724],
    [ 37.00343811, -86.36368662],
    [ 37.00357758, -86.36365019],
    [ 37.00372489, -86.36360692],
    [ 37.00388711, -86.36356354],
    [ 37.00405472, -86.36352621],
    [ 37.00423763, -86.36348621],
    [ 37.00437129, -86.36338074],
    [ 37.00448184, -86.36323899],
    [ 37.00457515, -86.36307953],
    [ 37.0047012 , -86.36286956],
    [ 37.00486024, -86.36273924],
    [ 37.00505061, -86.36270756],
    [ 37.00527945, -86.36272947],
    [ 37.00548802, -86.36263566],
    [ 37.00565341, -86.36245496],
    [ 37.00573513, -86.3621647 ],
    [ 37.00568611, -86.36182869],
    [ 37.00548782, -86.36157939],
    [ 37.0052881 , -86.36149696],
    [ 37.00511652, -86.36149669],
    [ 37.0049746 , -86.36158761],
    [ 37.00485989, -86.3616726 ],
    [ 37.00469955, -86.3617696 ],
    [ 37.00451492, -86.36178471],
    [ 37.00435852, -86.36173599],
    [ 37.00419576, -86.36162316],
    [ 37.00409127, -86.36146652],
    [ 37.00404463, -86.3612301 ],
    [ 37.00407232, -86.36097832],
    [ 37.00415922, -86.36078578],
    [ 37.00426711, -86.36066944],
    [ 37.00439509, -86.36060407],
    [ 37.00452844, -86.36057503],
    [ 37.00466778, -86.36054604],
    [ 37.004833  , -86.36050987],
    [ 37.00499495, -86.36047743],
    [ 37.00514229, -86.36044484],
    [ 37.00524902, -86.36041601],
    [ 37.00541074, -86.36037856],
    [ 37.00558676, -86.36034575],
    [ 37.00578957, -86.36038183],
    [ 37.00596102, -86.36045445],
    [ 37.00607154, -86.36065091],
    [ 37.0061651 , -86.36090615],
    [ 37.00626234, -86.3611639 ],
    [ 37.00637312, -86.36147776],
    [ 37.00642557, -86.36179897],
    [ 37.00644459, -86.36216748],
    [ 37.00637929, -86.36250433],
    [ 37.00629836, -86.36273906],
    [ 37.00622229, -86.36291634],
    [ 37.00611158, -86.36309997],
    [ 37.00600738, -86.36323379],
    [ 37.00589169, -86.3633461 ],
    [ 37.00576471, -86.36344689],
    [ 37.00564905, -86.36353398],
    [ 37.00551878, -86.36359932],
    [ 37.00537978, -86.36365448],
    [ 37.00525188, -86.36370605],
    [ 37.0051236 , -86.36376501],
    [ 37.00497727, -86.36382802],
    [ 37.00484855, -86.36391978],
    [ 37.00471266, -86.36403361],
    [ 37.0045783 , -86.36422396],
    [ 37.00447907, -86.36439954],
    [ 37.00435695, -86.36453807],
    [ 37.00424813, -86.36465022],
    [ 37.00411061, -86.36480228],
    [ 37.00397977, -86.36492984],
    [ 37.0038682 , -86.36504946],
    [ 37.00376295, -86.36516126],
    [ 37.00365436, -86.36527329],
    [ 37.00354759, -86.36539144],
    [ 37.00341675, -86.36552332],
    [ 37.00329206, -86.3656715 ],
    [ 37.00316095, -86.36581567],
    [ 37.00303256, -86.36594441],
    [ 37.00291203, -86.36607157],
    [ 37.00278014, -86.36622802],
    [ 37.00269333, -86.36641303],
    [ 37.00260606, -86.36659437],
    [ 37.00252845, -86.36675984],
    [ 37.00243487, -86.36695271],
    [ 37.00234795, -86.36716654],
    [ 37.00226102, -86.36735999],
    [ 37.00218046, -86.36754144],
    [ 37.00210026, -86.36771854],
    [ 37.00202358, -86.36787901],
    [ 37.00194406, -86.36806693],
    [ 37.00185835, -86.36825428],
    [ 37.00177578, -86.36843604],
    [ 37.00169902, -86.36860048],
    [ 37.0016732 , -86.3687928 ],
    [ 37.001679  , -86.3689974 ],
    [ 37.00168488, -86.36919051],
    [ 37.00169122, -86.36938766],
    [ 37.00169469, -86.3695562 ],
    [ 37.00170118, -86.36974238],
    [ 37.00171055, -86.36993991],
    [ 37.00167198, -86.37010864],
    [ 37.00158558, -86.37021273],
    [ 37.00148934, -86.37030898],
    [ 37.00139295, -86.37041757],
    [ 37.00129011, -86.37051816],
    [ 37.00120286, -86.37061913],
    [ 37.00112253, -86.37073184],
    [ 37.00106144, -86.37085264],
    [ 37.00099678, -86.37100197],
    [ 37.00092255, -86.37116696],
    [ 37.00083249, -86.37137665],
    [ 37.00073506, -86.37159438],
    [ 37.00065426, -86.37177611],
    [ 37.00057398, -86.37196168],
    [ 37.00060097, -86.37214635],
    [ 37.00074272, -86.37221467],
    [ 37.0009059 , -86.37222018],
    [ 37.00107657, -86.37218846],
    [ 37.00122503, -86.37211976],
    [ 37.00133803, -86.37204677],
    [ 37.00145467, -86.37192099],
    [ 37.00156457, -86.37178347],
    [ 37.00167114, -86.37162599],
    [ 37.00176434, -86.37145299],
    [ 37.00183833, -86.37126821],
    [ 37.00188646, -86.37112396],
    [ 37.0019441 , -86.37095574],
    [ 37.00197296, -86.370787  ],
    [ 37.00202945, -86.37067751],
    [ 37.00209668, -86.37048098],
    [ 37.00218305, -86.37031655],
    [ 37.00228558, -86.37016005],
    [ 37.00238818, -86.37002764],
    [ 37.00249377, -86.36991909],
    [ 37.00259639, -86.36983868],
    [ 37.00270864, -86.36976266],
    [ 37.00281411, -86.3697033 ],
    [ 37.00292264, -86.36964769],
    [ 37.00303847, -86.369599  ],
    [ 37.00316942, -86.36952703],
    [ 37.00332578, -86.36939126],
    [ 37.00342743, -86.36921198],
    [ 37.00346211, -86.36901195],
    [ 37.00343231, -86.36879509],
    [ 37.00336736, -86.36861758],
    [ 37.00327983, -86.36847602],
    [ 37.00316589, -86.36828932],
    [ 37.00305696, -86.36810259],
    [ 37.00296937, -86.36793662],
    [ 37.00293802, -86.36773416],
    [ 37.00295792, -86.36753614],
    [ 37.00301643, -86.36739089],
    [ 37.00307192, -86.36724611],
    [ 37.00312788, -86.36711056],
    [ 37.00320856, -86.36698638],
    [ 37.00331523, -86.36684361],
    [ 37.00340519, -86.36672019],
    [ 37.00350264, -86.36658791],
    [ 37.00361135, -86.36649982],
    [ 37.00374245, -86.36645177],
    [ 37.00387034, -86.3664758 ],
    [ 37.00395668, -86.36655587],
    [ 37.00402368, -86.36670753],
    [ 37.00407807, -86.36686779],
    [ 37.00412607, -86.36701603],
    [ 37.00418686, -86.36718831],
    [ 37.00427315, -86.36742386],
    [ 37.00436296, -86.3676927 ],
    [ 37.00442144, -86.36790543],
    [ 37.00445858, -86.36815496],
    [ 37.00447715, -86.36847342],
    [ 37.00444541, -86.36882644],
    [ 37.00434238, -86.36911386],
    [ 37.00425654, -86.36927291],
    [ 37.00418289, -86.36939303],
    [ 37.00410293, -86.36950912],
    [ 37.00399731, -86.36962489],
    [ 37.00390147, -86.36973254],
    [ 37.00379293, -86.36984054],
    [ 37.00369701, -86.36994104],
    [ 37.00359474, -86.37004514],
    [ 37.0034838 , -86.3701598 ],
    [ 37.00338477, -86.37024376],
    [ 37.00327609, -86.37032788],
    [ 37.00318982, -86.37040789],
    [ 37.00306205, -86.37049992],
    [ 37.00294389, -86.37059192],
    [ 37.00283177, -86.37070006],
    [ 37.00272312, -86.37083897],
    [ 37.00264003, -86.37094293],
    [ 37.00255683, -86.37103895],
    [ 37.00249914, -86.37115127],
    [ 37.00243497, -86.37126771],
    [ 37.00236746, -86.3713964 ],
    [ 37.00230001, -86.37152106],
    [ 37.00223561, -86.37164596],
    [ 37.00217132, -86.37175865],
    [ 37.0021135 , -86.37187932],
    [ 37.00204601, -86.37200003],
    [ 37.00196791, -86.37211434],
    [ 37.00188761, -86.3722312 ],
    [ 37.00180726, -86.37233202],
    [ 37.00173651, -86.37241676],
    [ 37.00165284, -86.37250555],
    [ 37.00155067, -86.3726109 ],
    [ 37.00144762, -86.37271211],
    [ 37.00136704, -86.37279282],
    [ 37.00126713, -86.37288565],
    [ 37.00115123, -86.37299929],
    [ 37.00103518, -86.37310829],
    [ 37.0009191 , -86.37321738],
    [ 37.00082561, -86.37329809],
    [ 37.00072863, -86.37339546],
    [ 37.00061364, -86.37350099],
    [ 37.00051698, -86.37360214],
    [ 37.00042667, -86.37368311],
    [ 37.00032338, -86.37378434],
    [ 37.00022342, -86.37387665],
    [ 37.00012018, -86.3739737 ],
    [ 37.00002051, -86.37405916],
    [ 36.99991404, -86.37415222],
    [ 36.99981076, -86.37423248],
    [ 36.99970432, -86.37430523],
    [ 36.99958169, -86.37432571],
    [ 36.99946928, -86.37430399],
    [ 36.99934964, -86.37428072],
    [ 36.99922365, -86.37425686],
    [ 36.99908793, -86.37422888],
    [ 36.99899069, -86.37413638],
    [ 36.99896465, -86.37397888],
    [ 36.9989808 , -86.37380904],
    [ 36.99900664, -86.37362303],
    [ 36.99904589, -86.37342887],
    [ 36.9990885 , -86.37323096],
    [ 36.99912743, -86.3730292 ],
    [ 36.99916641, -86.37284378],
    [ 36.99921127, -86.37261758],
    [ 36.99924349, -86.37245211],
    [ 36.99932082, -86.37230251],
    [ 36.99932082, -86.37230251],
    [ 36.99940337, -86.37214797],
    [ 36.99948814, -86.37199725],
    [ 36.99958754, -86.37189043],
    [ 36.99970528, -86.37178738],
    [ 36.99982625, -86.37169183],
    [ 36.99997073, -86.37159181],
    [ 37.00011471, -86.3714889 ],
    [ 37.00027357, -86.37138907],
    [ 37.0003852 , -86.37123775],
    [ 37.00042033, -86.37099884],
    [ 37.00038811, -86.3707825 ],
    [ 37.00031195, -86.37062837],
    [ 37.00026491, -86.37041498],
    [ 37.00030254, -86.37020247],
    [ 37.00038472, -86.3700261 ],
    [ 37.0004699 , -86.36984594],
    [ 37.00056383, -86.36964375],
    [ 37.00064015, -86.3694857 ],
    [ 37.00070993, -86.36934481],
    [ 37.0008098 , -86.36912411],
    [ 37.00090662, -86.36891836],
    [ 37.00098578, -86.36874579],
    [ 37.00107373, -86.36854755]
  ]
coords = reverse_coords[::-1]
route_data = {
    "path" : coords,
    "elevations" : np.zeros(len(coords)),
    "time_zones" : np.zeros(len(coords)),
    "num_unique_coords" : (len(coords) - 1) }
# Create a GIS object
starting_coords = [37.00107373, -86.36854755]
gis = GIS(route_data, starting_coords, current_coord = starting_coords)

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
    def run(self, vehicle_velocity_loader: FileLoader) -> tuple[FileLoader, ...]:
        """
        Run the localization stage, which computes various metrics relating to the car's location in a track race.

        Outputs will be FileLoaders pointing to None for non-track events, or if the prerequisite data is unavailable.
        1. LapIndexIntegratedSpeed
            Do not use this unless it is the only option. Integrates speed and tiles by track length to approximate
            the lap index we are on at any given time. Lap index is the integer number of laps we have completed
            around the track (starting at zero).
        2. LapIndexSpreadsheet
            Uses data from the FSGP timing spreadsheet (via FSGPDayLaps) to determine lap index.
            Lap index is the integer number of laps we have completed around the track
            at any given time (starting at zero).
        3. TrackIndexSpreadsheet
            Integrates speed within a lap to determine the car's coordinate within the track. We use a list of
            lat/lon pairs to represent a track's indices, and round to the nearest one. References the FSGP timing
            spreadsheet (via FSGPDayLaps) for lap start/stop times.

        :param self: an instance of LocalizationStage to be run
        :param FileLoader vehicle_velocity_loader: loader to VehicleVelocity from Ingress
        :returns: LapIndexIntegratedSpeed, LapIndexSpreadsheet, TrackIndexSpreadsheet (FileLoaders pointing to TimeSeries)
        """
        return super().run(self, vehicle_velocity_loader)

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

    def extract(self, vehicle_velocity_loader: FileLoader) -> tuple[Result]:
        vehicle_velocity_result: Result = vehicle_velocity_loader()
        return (vehicle_velocity_result,)

    @staticmethod
    def _get_lap_index_integrated_speed(vehicle_velocity_ts: TimeSeries) -> Result[TimeSeries]:
        integrated_velocity_m = np.cumsum(vehicle_velocity_ts) * vehicle_velocity_ts.period
        lap_index_integrated_speed = vehicle_velocity_ts.promote(
            np.array([int(dist_m // NCM_LAP_LEN_M) for dist_m in integrated_velocity_m]))
        lap_index_integrated_speed.name = "LapIndexIntegratedSpeed"
        lap_index_integrated_speed.units = "Laps"
        return Result.Ok(lap_index_integrated_speed)

    @staticmethod
    def _get_lap_index_spreadsheet(event: Event, vehicle_velocity_ts: TimeSeries):
        if event.name not in fsgp_lap_days.keys():
            return Result.Ok(None)  # result is not defined

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
            if (time < race_start_unix):
                lap_indices[i] = np.nan
            else:
                lap_indices[i] = np.count_nonzero(time > np.array(lap_finishes_unix))

        lap_indices_ts = vehicle_velocity_ts.promote(lap_indices)
        lap_indices_ts.name = "LapIndexSpreadsheet"
        lap_indices_ts.units = "Laps"
        return lap_indices_ts

    @staticmethod
    def _get_track_index_spreadsheet(event: Event, lap_index_spreadsheet: TimeSeries, vehicle_velocity_ts: TimeSeries):
        if event.name not in fsgp_lap_days.keys():
            return Result.Ok(None)  # result is not defined

        breakpoint()
        # get indices in time for when a new lap begins
        lap_starts = np.nonzero(np.diff(np.nan_to_num(lap_index_spreadsheet)))[0]
        velocity_by_lap = np.split(vehicle_velocity_ts, lap_starts)[1:]  # discard times before the first lap started

        track_indices: list[NDArray] = []
        for lap_velocity_array in velocity_by_lap:
            lap_cumulative_distance_m = integrate.cumulative_simpson(lap_velocity_array, dx=vehicle_velocity_ts.period)
            norm_factor = NCM_LAP_LEN_M / lap_cumulative_distance_m[-1]  # normalize such that all laps appear to travel the same distance
            lap_norm_distance_m = lap_cumulative_distance_m * norm_factor
            breakpoint()
            track_indices.append(gis.calculate_closest_gis_indices(lap_norm_distance_m))

        track_indices_flat = np.concatenate(track_indices)
        track_indices_ts = vehicle_velocity_ts.promote(track_indices_flat)
        track_indices_ts.name = "TrackIndexSpreadsheet"
        track_indices_ts.units = "Track Index"
        return track_indices_ts

    def transform(self, vehicle_velocity_result) -> tuple[Result, Result, Result]:
        try:
            vehicle_velocity_ts: TimeSeries = vehicle_velocity_result.unwrap().data
            lap_index_integrated_speed_result = self._get_lap_index_integrated_speed(vehicle_velocity_ts)
            lap_index_spreadsheet_ts = self._get_lap_index_spreadsheet(self.event, vehicle_velocity_ts)
            lap_index_spreadsheet_result = Result.Ok(lap_index_spreadsheet_ts)
            track_index_spreadsheet_result = self._get_track_index_spreadsheet(
                self.event, lap_index_spreadsheet_ts, vehicle_velocity_ts
            )
        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap vehicle velocity result! \n {e}")
            lap_index_integrated_speed_result = Result.Err(RuntimeError("Failed to process LapIndexIntegratedSpeed!"))
            lap_index_spreadsheet_result = Result.Err(RuntimeError("Failed to process LapIndexSpreadsheet!"))
            track_index_spreadsheet_result = Result.Err(RuntimeError("Failed to process TrackIndexSpreadsheet!"))
        return lap_index_integrated_speed_result, lap_index_spreadsheet_result, track_index_spreadsheet_result

    def load(self,
             lap_index_integrated_speed_result,
             lap_index_spreadsheet_result,
             track_index_spreadsheet_result) -> tuple[FileLoader, FileLoader, FileLoader]:

        lap_index_integrated_speed_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="LapIndexIntegratedSpeed",
            ),
            file_type=FileType.TimeSeries,
            data=lap_index_integrated_speed_result.unwrap() if lap_index_integrated_speed_result else None,
            description="Estimate of the FSGP lap index in this event as a function of time. "
                        "Value is estimated by integrating VehicleVelocity and tiling the result over the FSGP lap "
                        f"length of {NCM_LAP_LEN_M} meters."
        )

        lap_index_spreadsheet_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="LapIndexSpreadsheet",
            ),
            file_type=FileType.TimeSeries,
            data=lap_index_spreadsheet_result.unwrap() if lap_index_spreadsheet_result else None,
            description="Uses data from the FSGP timing spreadsheet (via FSGPDayLaps) to determine lap index."
                        "Lap index is the integer number of laps we have completed around the track"
                        "at any given time (starting at zero)."
        )

        track_index_spreadsheet_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=self.get_stage_name(),
                name="TrackIndexSpreadsheet",
            ),
            file_type=FileType.TimeSeries,
            data=track_index_spreadsheet_result.unwrap() if track_index_spreadsheet_result else None,
            description="Uses data from the FSGP timing spreadsheet (via FSGPDayLaps) to determine lap splits, then "
                        "integrates speed over the current lap to determine track index."
        )

        lap_index_integrated_speed_loader = self.context.data_source.store(lap_index_integrated_speed_file)
        self.logger.info(f"Successfully loaded LapIndexIntegratedSpeed!")

        lap_index_spreadsheet_loader = self.context.data_source.store(lap_index_spreadsheet_file)
        self.logger.info(f"Successfully loaded LapIndexSpreadsheet!")

        track_index_spreadsheet_loader = self.context.data_source.store(track_index_spreadsheet_file)
        self.logger.info(f"Successfully loaded TrackIndexSpreadsheet!")

        return lap_index_integrated_speed_loader, lap_index_spreadsheet_loader, lap_index_integrated_speed_loader # placeholder 3rd value!


stage_registry.register_stage(LocalizationStage.get_stage_name(), LocalizationStage)
