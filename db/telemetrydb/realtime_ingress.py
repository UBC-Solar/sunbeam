from data_tools.localization import TemporalLocalization # type: ignore
from db.telemetrydb.protocols import TimeProvider
from datetime import datetime, timedelta, timezone
from influxdb_client import InfluxDBClient
from typing import Any
from typing import Iterable


class RealtimeIngressQuerier:
    """
    Fast path for:
      'get the last value of these three fields before timestamp T'

    Notes:
    - Reuses one InfluxDBClient and one QueryApi.
    - Uses one Flux query for all fields.
    - Uses query_stream() to avoid heavier table/dataframe materialization.
    """

    def __init__(
            self,
            bucket: str | None = None,
            organization: str | None = None,
            url: str | None = None,
            token: str | None = None,
            timeout_s: float = 1.0,
            fields: Iterable[str] | None = None,
            time_provider: TimeProvider = datetime
    ):
        self._bucket = bucket
        self._organization = organization
        self._url = url
        self._token = token
        self._fields = tuple(fields) if fields is not None else ()

        self._client = InfluxDBClient(
            url=self._url,
            token=self._token,
            org=self._organization,
            timeout=timeout_s * 1000,
            enable_gzip=False,
        )
        self._query_api = self._client.query_api()

        self.now = lambda: time_provider.now(timezone.utc)
        self._timezone_fix = TemporalLocalization.localize(self.now())

    @staticmethod
    def _flux_time(dt: datetime) -> str:
        """ Creates a timestamp for Flux, query language of InfluxDB

        :param datetime dt: original timestamp
        :raises ValueError: Timestamp must be timezone aware
        :return str: Timestamp for Flux
        """        
        if dt.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _get_fields_filter(self):
        field_filter = " or ".join(
            f'r["_field"] == "{_field}"' for _field in self._fields
        )
        return field_filter

    def _build_last_value_query(self, stop_time: datetime) -> str:
        """ Creates the InfluxDB query for the 10 seconds before the stop time

        :param datetime stop_time: Stop time for the query
        :return str: Influx DB query
        """        
        stop_flux = self._flux_time(stop_time - self._timezone_fix)
        start_time = self._flux_time(stop_time - self._timezone_fix - timedelta(seconds=10))

        field_filter = self._get_fields_filter()

        return f'''
                    from(bucket: "{self._bucket}")
                      |> range(start: {start_time}, stop: {stop_flux})
                      |> filter(fn: (r) => {field_filter})
                      |> last()
                      |> keep(columns: ["_field", "_value", "_time"])
                    '''.strip()

    def _process_query(self, query: str):
        """ Processes the InfluxDB query and returns a list of fields alongside their values and time.

        :param str query: InfluxDB query
        :return dict[str, dict[str, Any] | None]: List of fields alongside their values and time
        """        
        _out: dict[str, dict[str, Any] | None] = {
            _field: None for _field in self._fields
        }

        try:
            stream = self._query_api.query_stream(query=query, org=self._organization)

            if stream is None:
                return _out  # nothing returned at all

            for record in stream:
                if record is None:
                    continue

                _field = record.get_field()
                if _field in _out:
                    _out[_field] = {
                        "time": record.get_time() + self._timezone_fix,
                        "value": record.get_value(),
                    }

        except Exception as e:
            return _out

        return _out

    def get_last_values(self) -> dict[str, dict[str, Any] | None]:
        """ Returns the latest values from InfluxDB

        :return dict[str, dict[str, Any] | None]: List of fields alongside their values and time
        """        
        query = self._build_last_value_query(self.now())
        out = self._process_query(query)
        return out

    def close(self) -> None:
        self._client.close()


if __name__ == "__main__":
    import sys
    import time

    from config.context import Context

    telemetry_db = Context().telemetry_db
    reader = RealtimeIngressQuerier(
            bucket=telemetry_db.bucket,
            organization=telemetry_db.organization,
            url=telemetry_db.database_url,
            token=telemetry_db.token,
            fields=(
                "TotalPackVoltage",
                "PackCurrent",
                "VehicleVelocity",
                "AcceleratorPosition",
                "MechBrakePressed",
                "BatteryCurrent",
                "BatteryVoltage",
            ),
    )

    try:
        while True:
            loop_start = time.perf_counter()

            values = reader.get_last_values()

            total_time_ms = (time.perf_counter() - loop_start) * 1000

            # Clear screen and move cursor to top
            sys.stdout.write("\033[H\033[J")

            print("=== Live Telemetry (Last Values) ===")
            print(f"Query time: {total_time_ms:.2f} ms\n")

            for field, data in values.items():
                if data is None:
                    print(f"{field}: None")
                else:
                    print(f"{field}: {data['value']} @ {data['time']}")

            sys.stdout.flush()

            # Maintain ~0.05s loop
            elapsed = time.perf_counter() - loop_start
            time.sleep(max(0.0, 0.05 - elapsed))

    except KeyboardInterrupt:
        reader.close()
        print("\nStopped.")