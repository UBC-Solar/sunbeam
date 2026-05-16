from client.realtime_ingress import RealtimeIngress, TimeProvider
from data_tools.localization import InfluxDBLanguageLocalization, CanonicalName
from state.frame import Frame, FrameView
from datetime import timezone
from typing import ClassVar
from stage.node import Node


class Ingress(Node):
    inputs: ClassVar[list[str]] = []

    def __init__(self, output_signals: list[str], frequency: float, time_provider: TimeProvider) -> None:
        super().__init__()
        self._output_signals = output_signals
        self._frequency = frequency

        self._localized_output_signals = []
        self._localized_signal_to_signal = {}
        for signal in self._output_signals:
            field, _, _, _ = InfluxDBLanguageLocalization.localize(signal, time_provider.now(timezone.utc).date())
            self._localized_output_signals.append(field)
            self._localized_signal_to_signal[field] = signal

        self._ingress = RealtimeIngress(fields=self._localized_output_signals, time_provider=time_provider)


    @property
    def frequency(self) -> float:
        return self._frequency

    @property
    def outputs(self) -> list[str]:
        return self._output_signals

    @property
    def node_name(self):
        return f"Ingress_{self._frequency}Hz"

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        values = self._ingress.get_last_values()

        for field, data in values.items():
            try:
                new_frame.write(self._localized_signal_to_signal[field], data['value'])
            except TypeError:
                pass

        return new_frame


class Power(Node):
    node_name: ClassVar[str] = "Power"
    inputs: ClassVar[list[str]] = [CanonicalName.PackVoltage, CanonicalName.MotorCurrent]
    outputs: ClassVar[list[str]] = [CanonicalName.MotorPower]
    frequency: ClassVar[float] = 5

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        motor_voltage = input_frame.read(CanonicalName.PackVoltage)
        motor_current = input_frame.read(CanonicalName.MotorCurrent)

        motor_power = motor_voltage * motor_current
        new_frame.write(CanonicalName.MotorPower, motor_power)

        return new_frame


class Efficiency(Node):
    node_name: ClassVar[str] = "Efficiency"
    inputs: ClassVar[list[str]] = [CanonicalName.MotorPower, CanonicalName.VehicleSpeed]
    outputs: ClassVar[list[str]] = [CanonicalName.MotorEfficiency]
    frequency: ClassVar[float] = 5

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        motor_power = input_frame.read(CanonicalName.MotorPower)
        vehicle_speed = input_frame.read(CanonicalName.VehicleSpeed)

        motor_efficiency = motor_power / vehicle_speed
        new_frame.write(CanonicalName.MotorEfficiency, motor_efficiency)

        return new_frame
