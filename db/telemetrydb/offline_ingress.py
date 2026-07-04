from data_tools.collections import TimeSeries
from data_tools.query import InfluxDBClient
from datetime import datetime
from typing import Iterable


class OfflineIngress:
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
            bucket: str = "CAN_log",
            organization: str = "8a0b66d77a331e96",
            url: str = "http://influxdb.telemetry.ubcsolar.com",
            token: str = "s4Z9_S6_O09kDzYn1KZcs7LVoCA2cVK9_ObY44vR4xMh-wYLSWBkypS0S0ZHQgBvEV2A5LgvQ1IKr8byHes2LA==",
            timeout_s: float = 1.0,
            fields: Iterable[str] = None,
    ):
        self._bucket = bucket
        self._organization = organization
        self._url = url
        self._token = token
        self._fields = fields

        self._client = InfluxDBClient(
            url=self._url,
            influxdb_token=self._token,
            influxdb_org=self._organization,
            timeout=timeout_s * 1000
        )

    def get_values_between(
            self,
            start_time: datetime,
            stop_time: datetime
            ) -> dict[str, TimeSeries]:
        _out: dict[str, TimeSeries | None] = {
            _field: None for _field in self._fields
        }

        for _field in _out.keys():
            _out[_field] = self._client.query_time_series(start_time, stop_time, _field)

        return _out

    def close(self):
        self._client.close()


if __name__ == "__main__":
    from datetime import timezone

    reader = OfflineIngress(
        bucket="CAN_log",
        organization="8a0b66d77a331e96",
        url="http://influxdb.telemetry.ubcsolar.com",
        token="s4Z9_S6_O09kDzYn1KZcs7LVoCA2cVK9_ObY44vR4xMh-wYLSWBkypS0S0ZHQgBvEV2A5LgvQ1IKr8byHes2LA==",
        fields=(
            "TotalPackVoltage",
            "PackCurrent",
            "VehicleVelocity",
            "AcceleratorPosition",
            "MechBrakePressed",
            "BatteryCurrent",
            "BatteryVoltage",
        )
    )

    values = reader.get_values_between(datetime(2024, 7, 16, 14, 00, tzinfo=timezone.utc), datetime(2024, 7, 16, 15, 00, tzinfo=timezone.utc))

    values["TotalPackVoltage"].plot()

    reader.close()