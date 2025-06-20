import matplotlib.pyplot as plt
import dill
import pathlib
from datetime import datetime, timedelta

from dateutil import tz

if __name__ == '__main__':

    event_name = "realtime"

    stage_dir = "localization"
    file_name = "TrackIndexGPS.bin"
    path_to_bin = pathlib.Path("../fs_data/pipeline") / event_name / stage_dir / file_name

    with open(path_to_bin, 'rb') as f:
        data = dill.load(f).data

    vancouver_times = [dt - timedelta(0, 7*3600) for dt in data.datetime_x_axis]

    plt.plot(vancouver_times, data)
    plt.title("Stadium Parking Lot Track Index")
    plt.xlabel("Time")
    plt.ylabel("Index")
    plt.xticks(rotation=90)
    plt.show()
    # plt.hist(data, bins='auto')
    # plt.show()