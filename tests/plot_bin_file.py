import matplotlib.pyplot as plt
import dill
import pathlib

if __name__ == '__main__':

    event_name = "FSGP_2024_Day_2"

    stage_dir = "localization"
    file_name = "TrackIndexSpreadsheet.bin"
    path_to_bin = pathlib.Path("../fs_data/pipeline") / event_name / stage_dir / file_name

    stage_dir2 = "localization"
    file_name2 = "LapIndexSpreadsheet.bin"
    path_to_bin2 = pathlib.Path("../fs_data/pipeline") / event_name / stage_dir2 / file_name2

    with open(path_to_bin, 'rb') as f:
        data = dill.load(f).data
    with open(path_to_bin2, 'rb') as f:
        data2 = dill.load(f).data

    data, data2 = data.align(data, data2)

    plt.plot(data.x_axis, data, label="track index")
    plt.plot(data2.x_axis, data2, label="lap index")
    plt.xlabel("Time")
    plt.ylabel("")
    plt.show()