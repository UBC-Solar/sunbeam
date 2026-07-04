from db.telemetrydb.offline_ingress import OfflineIngressQuerier, TimeProvider
from data_tools.localization import InfluxDBLanguageLocalization
from state.frame import Frame, FrameView
from datetime import timezone
from typing import ClassVar
from stage.stage import Stage


class OfflineIngress(Stage):
    inputs: ClassVar[list[str]] = []

    def __init__(self, output_signals: list[str], frequency: float, time_provider: TimeProvider, bucket: str = None, organization: str = None, token: str = None, url: str = None) -> None:
        super().__init__()
        self._output_signals = output_signals
        self._frequency = frequency

        self._localized_output_signals = []
        self._localized_signal_to_signal = {}
        for signal in self._output_signals:
            field, _, _, _ = InfluxDBLanguageLocalization.localize(signal, time_provider.now(timezone.utc).date())
            self._localized_output_signals.append(field)
            self._localized_signal_to_signal[field] = signal

        self._ingress = OfflineIngressQuerier(
            fields=self._localized_output_signals,
            time_provider=time_provider,
            bucket=bucket,
            organization=organization,
            url=url,
            token=token
        )


    @property
    def frequency(self) -> float:
        return self._frequency

    @property
    def outputs(self) -> list[str]:
        return self._output_signals

    @property
    def stage_name(self):
        return f"Offline_Ingress_{self._frequency}Hz"

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        values = self._ingress.get_last_values()

        for field, data in values.items():
            try:
                new_frame.write(self._localized_signal_to_signal[field], data['value'])
            except TypeError:
                pass

        return new_frame
