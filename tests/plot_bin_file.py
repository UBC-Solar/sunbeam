import matplotlib.pyplot as plt
import dill
import os

with open(os.path.join(os.getcwd(), 'EfficiencyLapDist.bin'), 'rb') as f:
    data = dill.load(f).data

plt.plot(data)
plt.show()