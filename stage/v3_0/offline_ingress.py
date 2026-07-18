from db.telemetrydb.offline_ingress import OfflineIngressQuerier
from db.telemetrydb.protocols import TimeProvider
from data_tools.localization import InfluxDBLanguageLocalization # type: ignore
from state.frame import Frame, FrameView
from datetime import timezone
from typing import ClassVar
from stage.stage import Stage


class OfflineIngress(Stage):
    inputs: ClassVar[list[str]] = []

    def __init__(self, output_signals: list[str], time_provider: TimeProvider, bucket: str = None, organization: str = None, token: str = None, url: str = None) -> None:
        super().__init__()
        self._output_signals = output_signals

        self._localized_output_signals = []
        self._localized_signal_to_signal = {}
        for signal in self._output_signals:
            field, _, _, _ = InfluxDBLanguageLocalization.localize(signal, time_provider.now(timezone.utc).date())
            self._localized_output_signals.append(field)
            self._localized_signal_to_signal[field] = signal

        self._ingress = OfflineIngressQuerier(
            fields=self._localized_output_signals,
            bucket=bucket,
            organization=organization,
            url=url,
            token=token
        )

    @property
    def outputs(self) -> list[str]:
        return self._output_signals

    @property
    def stage_name(self):
        return f"Offline_Ingress_{self._frequency}Hz"

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        values = self._ingress.get_last_values() # Remove this please

        for field, data in values.items():
            try:
                new_frame.write(self._localized_signal_to_signal[field], data['value'])
            except TypeError:
                pass

        return new_frame
