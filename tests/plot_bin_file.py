import matplotlib.pyplot as plt
import dill
import os

with open(os.path.join(os.getcwd(), 'EfficiencyLapDist.bin'), 'rb') as f:
    data = dill.load(f).data

plt.plot(data)
plt.title('EfficiencyLapDist.bin - FSGP 2024 Day 1')
plt.xlabel('Lap Index, Day 1')
plt.ylabel('Efficiency, J/m')
plt.show()