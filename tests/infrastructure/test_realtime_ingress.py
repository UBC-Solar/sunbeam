from datetime import datetime, timedelta, timezone

from db.telemetrydb.realtime_ingress import LOOKBACK, RealtimeIngressQuerier

NOW = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)


class FixedTimeProvider:
    def now(self, tz):
        return NOW.astimezone(tz)


class FakeInflux:
    """ Stands in for data_tools' InfluxDBClient. """

    def __init__(self, latest=None, error=None):
        self.latest = latest or {}
        self.error = error
        self.calls = []

    def query_last_values(self, bucket, fields, start, stop):
        self.calls.append((bucket, tuple(fields), start, stop))
        if self.error is not None:
            raise self.error
        return {field: self.latest.get(field) for field in fields}


def make_querier(fake: FakeInflux) -> RealtimeIngressQuerier:
    querier = RealtimeIngressQuerier(
        bucket="CAN_log", organization="org", url="http://influx:8086", token="token",
        fields=["PackCurrent", "VehicleVelocity"], time_provider=FixedTimeProvider(),
    )
    querier._client = fake
    return querier


def test_latest_values_over_the_lookback():
    stamp = NOW - timedelta(seconds=1)
    fake = FakeInflux(latest={"PackCurrent": (stamp, 2.5)})

    values = make_querier(fake).get_last_values()

    assert values == {"PackCurrent": {"time": stamp, "value": 2.5}, "VehicleVelocity": None}
    assert fake.calls == [("CAN_log", ("PackCurrent", "VehicleVelocity"), NOW - LOOKBACK, NOW)]


def test_failed_query_is_no_data():
    values = make_querier(FakeInflux(error=TimeoutError("slow"))).get_last_values()

    assert values == {"PackCurrent": None, "VehicleVelocity": None}
