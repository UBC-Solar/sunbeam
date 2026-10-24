from datetime import datetime
from typing import ClassVar

from data_tools.localization import CanonicalName  # type: ignore

from db.telemetrydb.offline_ingress import OfflineIngressQuerier
from db.telemetrydb.protocols import IngressObserver
from stage.stage import Stage
from state.frame import Frame, FrameView


class OfflineIngress(Stage):
    """ Fetches every output signal over the whole event window at once, as TimeSeries. Runs once, not on a schedule. """
    stage_name: ClassVar[str] = "Offline_Ingress"
    inputs: ClassVar[list[str]] = []
    frequency: ClassVar[float] = 0

    def __init__(self, output_signals: list[CanonicalName], event_start_date: datetime, event_end_date: datetime, bucket: str | None = None, organization: str | None = None, token: str | None = None, url: str | None = None, observer: IngressObserver | None = None) -> None:
        super().__init__()
        self._output_signals = output_signals
        self._start_date = event_start_date
        self._end_date = event_end_date

        # The querier localizes canonical names to InfluxDB fields itself, using the event's date
        self._ingress = OfflineIngressQuerier(
            fields=self._output_signals,
            bucket=bucket,
            organization=organization,
            url=url,
            token=token,
            observer=observer,
        )

    @property
    def outputs(self) -> list[str]:
        return self._output_signals

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)

        for field, series in self._ingress.get_values_between(self._start_date, self._end_date).items():
            new_frame.write(field, series)

        return new_frame
