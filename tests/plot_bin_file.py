import matplotlib.pyplot as plt
import dill
import os
from data_tools.collections import TimeSeries

if __name__ == '__main__':

    things_to_plot = [
        'dni.bin', 'ghi.bin', 'dhi.bin',
    ]

    data = []
    for thing in things_to_plot:
        with open(os.path.join(os.getcwd(), 'fs_data', 'pipeline', 'realtime', 'weather', thing), 'rb') as f:
            data.append(dill.load(f).data)

    for thing, ts in zip(things_to_plot, data):
        plt.plot(ts.datetime_x_axis, ts, label=thing)
    plt.legend()
    plt.title(r'Solcast GHI, DHI, and DNI. GHI $\approx$ $\text{DHI + DNI} \cos(\theta)$')
    plt.show()