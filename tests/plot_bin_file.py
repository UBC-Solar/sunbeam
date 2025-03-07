import matplotlib.pyplot as plt
import dill
import os

pow_dir = os.path.normpath(os.getcwd())
with open(os.path.join(pow_dir, 'EfficiencyLapDist.bin'), 'rb') as f:
    data = dill.load(f).data

plt.plot(data)
plt.show()