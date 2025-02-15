from data_tools import DBClient
import matplotlib.pyplot as plt
from datetime import datetime
import pytz
from config import config_directory
import toml as tomllib
import dill
import os

# 1. Load Sunbeam Motor Power

pow_dir = os.path.normpath(os.path.join(os.getcwd(), 'fs_data/pipeline/FSGP_2024_Day_1/power'))
print("Loading from folder: ", pow_dir)

with open(os.path.join(pow_dir, 'MotorPower.bin'), 'rb') as f:
    motor_power_sunbeam = dill.load(f).data

# 2. Get Influx Motor power (same method as we have been using for data_analysis)

client = DBClient()
with open(config_directory / "events.toml") as events_file:
    events_config = tomllib.load(events_file)
start: datetime = datetime.strptime(events_config["event"][0]["start"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
stop: datetime = datetime.strptime(events_config["event"][0]["stop"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)

# Note: fields are called 'battery' but they are indeed motor fields.
#       they are labeled as so since they are from the MCB and recording the motor power from the battery
motor_current = client.query_time_series(start, stop, "BatteryCurrent")
motor_voltage = client.query_time_series(start, stop, "BatteryVoltage")
motor_current_direction = client.query_time_series(start, stop, "BatteryCurrentDirection")

motor_current, motor_voltage, motor_current_direction = motor_current.align(
    motor_current, motor_voltage, motor_current_direction)

# motor_current_direction is 1 if negative (regen) and 0 if positive (driving)
# the linear function -2x + 1 maps 1 to -1 and 0 to 1,
# resulting in a number that represents the sign/direction of the current
motor_current_sign = motor_current_direction * -2 + 1

motor_power_manual = motor_current.promote(motor_voltage * motor_current * motor_current_sign)

motor_power_sunbeam, motor_power_manual = motor_power_sunbeam.align(motor_power_manual, motor_power_manual)

plt.plot(motor_power_sunbeam, label='sunbeam')
plt.plot(motor_power_manual, label='manually determined (influx)')
plt.legend()
plt.show()