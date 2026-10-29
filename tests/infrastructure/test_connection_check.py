from datetime import UTC, datetime

from db.telemetrydb.connection_check import check_influx, event_window

NOW = datetime(2026, 7, 1, 15, 0, tzinfo=UTC)
WINDOW = (datetime(2026, 7, 1, 12, 0, tzinfo=UTC), datetime(2026, 7, 1, 13, 0, tzinfo=UTC))


class FakeInflux:
    """ Just enough of data_tools' InfluxDBClient for check_influx. """

    def __init__(self, *, ping: bool | Exception = True, bucket_found=True, has_data=True, query_error=None):
        self._ping = ping
        self._bucket_found = bucket_found
        self._has_data = has_data
        self._query_error = query_error
        self.windows = []
        self.timeouts = []
        self.closed = 0

    def __call__(self, **kwargs):
        self.kwargs = kwargs
        self.timeouts.append(kwargs["timeout"])
        return self

    def ping(self):
        if isinstance(self._ping, Exception):
            raise self._ping
        return self._ping

    def bucket_exists(self, bucket):
        return self._bucket_found

    def has_data(self, bucket, start, stop):
        self.windows.append((start, stop))
        if self._query_error is not None:
            raise self._query_error
        return self._has_data

    def close(self):
        self.closed += 1


def run(fake, window=None):
    return check_influx("http://influx:8086", "org", "CAN_log", "token", window, client_factory=fake)


class TestCheckInflux:
    def test_passes_every_step(self):
        fake = FakeInflux()

        result = run(fake, WINDOW)

        assert result.ok
        assert [step.name for step in result.steps] == ["connect", "bucket", "data"]
        assert fake.closed == 2  # the ping/bucket client and the data client
        assert fake.kwargs["influxdb_token"] == "token"

    def test_checks_time_out_fast_but_the_data_query_gets_longer(self):
        fake = FakeInflux()

        run(fake, WINDOW)

        assert fake.timeouts == [5.0, 15.0]

    def test_without_a_window_data_is_not_checked(self):
        fake = FakeInflux(has_data=False)

        result = run(fake)

        assert result.ok
        assert [step.name for step in result.steps] == ["connect", "bucket"]
        assert fake.windows == []

    def test_unreachable_server_stops_at_connect(self):
        result = run(FakeInflux(ping=ConnectionError("refused")), WINDOW)

        assert not result.ok
        assert [step.name for step in result.steps] == ["connect"]
        assert "refused" in result.detail

    def test_missing_bucket_names_it(self):
        result = run(FakeInflux(bucket_found=False), WINDOW)

        assert not result.ok
        assert result.detail.startswith("bucket: bucket 'CAN_log' not found")

    def test_no_data_in_window_fails(self):
        result = run(FakeInflux(has_data=False), WINDOW)

        assert not result.ok
        assert result.detail == "data: no data in 'CAN_log' from 2026-07-01T12:00:00Z to 2026-07-01T13:00:00Z"

    def test_data_is_looked_for_over_the_window(self):
        fake = FakeInflux()

        run(fake, WINDOW)

        assert fake.windows == [WINDOW]

    def test_query_error_fails_the_data_step(self):
        result = run(FakeInflux(query_error=RuntimeError("bad flux")), WINDOW)

        assert result.detail == "data: query failed: bad flux"


class TestEventWindow:
    def test_past_event_is_the_whole_event(self):
        assert event_window(*WINDOW, now=NOW) == WINDOW

    def test_live_event_runs_to_now(self):
        assert event_window(WINDOW[0], None, now=NOW) == (WINDOW[0], NOW)

    def test_event_ending_later_runs_to_now(self):
        assert event_window(WINDOW[0], datetime(2026, 7, 2, tzinfo=UTC), now=NOW) == (WINDOW[0], NOW)

    def test_future_event_has_no_window(self):
        assert event_window(datetime(2026, 7, 2, tzinfo=UTC), None, now=NOW) is None

    def test_naive_times_are_utc(self):
        # SQLite hands back naive datetimes; they're stored as UTC
        assert event_window(datetime(2026, 7, 1, 12, 0), None, now=NOW) == (WINDOW[0], NOW)
