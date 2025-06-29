import matplotlib.pyplot as plt
import dill
import pathlib

def load_data(event_name, stage_dir, file_name):
    path_to_bin = pathlib.Path("../fs_data/pipeline") / event_name / stage_dir / file_name
    with open(path_to_bin, 'rb') as f:
        return dill.load(f).data

if __name__ == '__main__':

    event_name = "realtime"
    stage_dir = "cleanup"
    file_name = "SpeedMPS.bin"
    data1 = load_data(event_name, stage_dir, file_name)

    stage_dir = "ingress"
    file_name = "VehicleVelocity.bin"
    speed = load_data(event_name, stage_dir, file_name)

    plt.plot(data1.datetime_x_axis, data1, label="SpeedMPS")
    plt.legend(loc='best')
    plt.xlabel("Time")
    plt.ylabel("Degrees")
    plt.xticks(rotation=90)
    plt.show()