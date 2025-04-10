import matplotlib.pyplot as plt
import dill
import pathlib

if __name__ == '__main__':

    event_name = "FSGP_2024_Day_1"
    stage_dir = "localization"
    file_name = "LapIndexSpreadsheet.bin"
    path_to_bin = pathlib.Path("../fs_data/pipeline") / event_name / stage_dir / file_name
    path_to_bin2 = pathlib.Path("../fs_data/pipeline") / event_name / stage_dir / "LapIndexIntegratedSpeed.bin"

    with open(path_to_bin, 'rb') as f:
        data = dill.load(f).data
    with open(path_to_bin2, 'rb') as f:
        data2 = dill.load(f).data

    plt.plot(data2.x_axis, data2, label="integrated speed")
    plt.plot(data.x_axis, data, label="from spreadsheet")
    plt.xlabel("Time")
    plt.ylabel("Lap Index")
    plt.legend()
    # plt.title(f"{file_name} for {event_name}")
    plt.show()