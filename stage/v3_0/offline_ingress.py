from db.telemetrydb.offline_ingress import OfflineIngressQuerier
from db.telemetrydb.protocols import TimeProvider
from data_tools.localization import InfluxDBLanguageLocalization # type: ignore
from state.frame import Frame, FrameView
from datetime import timezone
from typing import ClassVar
from stage.stage import Stage
import random


class OfflineIngress(Stage):
    inputs: ClassVar[list[str]] = []

    def __init__(self, output_signals: list[str], time_provider: TimeProvider, event_start_date, event_end_date, bucket: str = None, organization: str = None, token: str = None, url: str = None) -> None:
        super().__init__()
        self._output_signals = output_signals
        self._frequency = 0
        self._start_date = event_start_date
        self._end_date = event_end_date
        self._stage_name = f"Offline_Ingress{random.randint(1, 1000)}"

        self._localized_output_signals = []
        self._localized_signal_to_signal = {}
        for signal in self._output_signals:
            print(signal) # Delete This
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
    def frequency(self):
        return self._frequency

    @property
    def stage_name(self):
        return self._stage_name

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        values = self._ingress.get_values_between(self._start_date, self._end_date)

        for field, data in values.items():
            try:
                new_frame.write(self._localized_signal_to_signal[field], data)
            except TypeError:
                pass

        return new_frame
