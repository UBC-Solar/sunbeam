import time
from collections.abc import Iterable
from datetime import datetime

from data_tools.collections import TimeSeries
from data_tools.query import InfluxDBClient
from data_tools.localization import CanonicalName

from db.telemetrydb.protocols import IngressObserver


class IngressError(RuntimeError):
    """ Raised when telemetry for an offline event could not be fetched. """


class OfflineIngressQuerier:
    """ Fetches each field's whole TimeSeries over a window, one query per field, reporting progress to an
    optional IngressObserver. """

    def __init__(
            self,
            bucket: str | None = None,
            organization: str | None = None,
            url: str | None = None,
            token: str | None = None,
            timeout_s: float = 30.0,
            fields: Iterable[CanonicalName] | None = None,
            observer: IngressObserver | None = None,
    ):
        self._bucket = bucket
        self._organization = organization
        self._url = url
        self._token = token
        self._fields = tuple(fields) if fields is not None else ()
        self._observer = observer

        self._client = InfluxDBClient(
            url=self._url,
            influxdb_token=self._token,
            influxdb_org=self._organization,
            timeout=timeout_s  # data_tools takes seconds and converts to ms itself
        )

    def get_values_between(
            self,
            start_time: datetime,
            stop_time: datetime
            ) -> dict[CanonicalName, TimeSeries]:
        _out: dict[CanonicalName, TimeSeries] = {}

        for _field in self._fields:
            if self._observer is not None:
                self._observer.on_query_start(str(_field))

            query_start = time.monotonic()
            try:
                series = self._client.query_time_series(start_time, stop_time, _field, bucket=self._bucket)
            except Exception as e:
                if self._observer is not None:
                    self._observer.on_query_failed(str(_field), e)
                raise IngressError(f"Failed to query {_field} between {start_time} and {stop_time} from InfluxDB: {e}") from e

            if self._observer is not None:
                self._observer.on_query_done(str(_field), len(series), time.monotonic() - query_start)

            _out[_field] = series

        return _out

    def close(self):
        self._client.close()

