import uuid
from datetime import UTC
from typing import ClassVar

from data_tools.localization import InfluxDBLanguageLocalization  # type: ignore

from db.telemetrydb.offline_ingress import OfflineIngressQuerier
from db.telemetrydb.protocols import TimeProvider
from stage.stage import Stage
from state.frame import Frame, FrameView


class OfflineIngress(Stage):
    inputs: ClassVar[list[str]] = []

    def __init__(self, output_signals: list[str], time_provider: TimeProvider, event_start_date, event_end_date, bucket: str | None = None, organization: str | None = None, token: str | None = None, url: str | None = None) -> None:
        super().__init__()
        self._output_signals = output_signals
        self._frequency = 0
        self._start_date = event_start_date
        self._end_date = event_end_date
        self._stage_name = f"Offline_Ingress{uuid.uuid4()}"

        self._localized_output_signals = []
        self._localized_signal_to_signal = {}
        for signal in self._output_signals:
            field, _, _, _ = InfluxDBLanguageLocalization.localize(signal, time_provider.now(UTC).date())
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
