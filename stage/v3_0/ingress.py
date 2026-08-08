from db.telemetrydb.realtime_ingress import RealtimeIngress, TimeProvider
from data_tools.localization import InfluxDBLanguageLocalization
from state.frame import Frame, FrameView
from datetime import timezone
from typing import ClassVar, Optional
from collections.abc import Callable
from stage.stage import Stage
import time


class MissingDataTracker:
    """
    Tracks, per signal, how long the telemetry source has been returning no
    data. Brief gaps are normal (queries race the data landing in InfluxDB);
    a signal missing continuously for longer than the grace period is not.
    """

    def __init__(self, grace_s: float = 1.0, monotonic: Callable[[], float] = time.monotonic):
        self._grace_s = grace_s
        self._monotonic = monotonic
        self._missing_since: dict[str, float] = {}

    def observe(self, signal: str, has_data: bool) -> None:
        if has_data:
            self._missing_since.pop(signal, None)
        else:
            self._missing_since.setdefault(signal, self._monotonic())

    def overdue(self) -> list[str]:
        now = self._monotonic()
        return sorted(
            signal
            for signal, since in self._missing_since.items()
            if now - since > self._grace_s
        )


class Ingress(Stage):
    inputs: ClassVar[list[str]] = []

    def __init__(
        self,
        output_signals: list[str],
        frequency: float,
        time_provider: TimeProvider,
        bucket: Optional[str] = None,
        organization: Optional[str] = None,
        token: Optional[str] = None,
        url: Optional[str] = None,
        ingress_client: Optional[RealtimeIngress] = None,
        data_grace_s: float = 1.0,
    ) -> None:
        super().__init__()
        self._output_signals = output_signals
        self._frequency = frequency
        self._tracker = MissingDataTracker(grace_s=data_grace_s)

        self._localized_output_signals = []
        self._localized_signal_to_signal = {}
        for signal in self._output_signals:
            field, _, _, _ = InfluxDBLanguageLocalization.localize(signal, time_provider.now(timezone.utc).date())
            self._localized_output_signals.append(field)
            self._localized_signal_to_signal[field] = signal

        self._ingress = ingress_client or RealtimeIngress(
            fields=self._localized_output_signals,
            time_provider=time_provider,
            bucket=bucket,
            organization=organization,
            url=url,
            token=token
        )

    # The Stage contract declares these as class attributes; ingress is the
    # one stage whose identity is per-instance (frequency bin), hence the
    # property overrides.
    @property
    def frequency(self) -> float:  # type: ignore[override]
        return self._frequency

    @property
    def outputs(self) -> list[str]:  # type: ignore[override]
        return self._output_signals

    @property
    def stage_name(self):  # type: ignore[override]
        return f"Ingress_{self._frequency}Hz"

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        values = self._ingress.get_last_values()

        for field, data in values.items():
            signal = self._localized_signal_to_signal[field]
            value = None if data is None else data.get("value")

            self._tracker.observe(signal, has_data=value is not None)

            if value is not None:
                new_frame.write(signal, value)

        overdue = self._tracker.overdue()
        if overdue:
            raise RuntimeError(
                f"{self.stage_name}: no data for signal(s) {overdue} for "
                f"longer than the grace period. Telemetry source is not "
                f"delivering."
            )

        return new_frame
