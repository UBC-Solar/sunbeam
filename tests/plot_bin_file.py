import matplotlib.pyplot as plt
import dill
import pathlib

if __name__ == '__main__':

    event_name = "ASC_2024_Day_1"
    stage_dir = "localization"
    file_name = "LapIndexIntegratedSpeed.bin"
    path_to_bin = pathlib.Path("../fs_data/pipeline") / event_name / stage_dir / file_name

    with open(path_to_bin, 'rb') as f:
        data = dill.load(f).data

    plt.plot(data)
    plt.title(f"{file_name} for {event_name}")
    plt.show()