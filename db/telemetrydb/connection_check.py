"""
Checks that an InfluxDB connection works, and optionally that it can see
data over an event's window. Shared by the server (testing a connection
from the dashboard, and before launching a worker with it) and the worker
preflight, so both fail for the same reasons with the same messages.
"""
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass
class CheckStep:
    name: str
    ok: bool
    detail: str


@dataclass
class CheckResult:
    steps: list[CheckStep] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(step.ok for step in self.steps)

    @property
    def detail(self) -> str:
        """ The first failure, or the last step's detail when everything passed. """
        for step in self.steps:
            if not step.ok:
                return f"{step.name}: {step.detail}"
        return self.steps[-1].detail if self.steps else ""


def _as_utc(dt: datetime) -> datetime:
    # SQLite (the default test suite) drops tzinfo; stored values are UTC
    return dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)


def event_window(
    starts_at: datetime,
    ends_at: datetime | None,
    now: datetime | None = None,
) -> tuple[datetime, datetime] | None:
    """
    The span of an event that should already have data: all of it for a
    past event, start-to-now for a live one, and None for one that hasn't
    started yet.
    """
    now = _as_utc(now or datetime.now(UTC))
    start = _as_utc(starts_at)
    if start >= now:
        return None

    stop = now if ends_at is None else min(_as_utc(ends_at), now)
    return start, stop


def _flux_time(dt: datetime) -> str:
    return _as_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


def check_influx(
    url: str,
    organization: str,
    bucket: str,
    token: str,
    window: tuple[datetime, datetime] | None = None,
    *,
    timeout_s: float = 5.0,
    data_timeout_s: float = 15.0,
    client_factory=None,
) -> CheckResult:
    """
    Ping the server, find the bucket, then (with a window) require at least
    one point in it. Stops at the first failing step.

    :param timeout_s: For the ping and bucket lookup, which answer at once from a healthy server
    :param data_timeout_s: For the data query, which scans the window and can legitimately take longer
    :param client_factory: Builds the InfluxDBClient; tests pass a fake
    """
    result = CheckResult()

    if client_factory is None:
        from influxdb_client.client.influxdb_client import InfluxDBClient as client_factory

    try:
        client = client_factory(url=url, token=token, org=organization, timeout=int(timeout_s * 1000))
    except Exception as exc:
        result.steps.append(CheckStep("connect", False, f"failed to construct client: {exc}"))
        return result

    try:
        try:
            reachable = client.ping()
        except Exception as exc:
            reachable, error = False, str(exc)
        else:
            error = "server did not respond to ping"
        if not reachable:
            result.steps.append(CheckStep("connect", False, f"{url}: {error}"))
            return result
        result.steps.append(CheckStep("connect", True, f"{url} reachable"))

        try:
            found = client.buckets_api().find_bucket_by_name(bucket)
        except Exception as exc:
            result.steps.append(CheckStep("bucket", False, str(exc)))
            return result
        if found is None:
            result.steps.append(CheckStep(
                "bucket", False,
                f"bucket {bucket!r} not found (check the token, organization, and bucket name)",
            ))
            return result
        result.steps.append(CheckStep("bucket", True, f"bucket {bucket!r} found"))

        if window is None:
            return result

        start, stop = window
        query = (
            f'from(bucket: "{bucket}") '
            f'|> range(start: {_flux_time(start)}, stop: {_flux_time(stop)}) '
            f'|> limit(n: 1)'
        )
        span = f"{_flux_time(start)} to {_flux_time(stop)}"
        try:
            # The client's timeout covers every request it makes, so the slower query gets a client of its own
            data_client = client_factory(url=url, token=token, org=organization, timeout=int(data_timeout_s * 1000))
            try:
                # Streamed so the first record is enough; nothing else is read
                records = data_client.query_api().query_stream(query, org=organization)
                has_data = next(iter(records), None) is not None
            finally:
                data_client.close()
        except Exception as exc:
            result.steps.append(CheckStep("data", False, f"query failed: {exc}"))
            return result
        if not has_data:
            result.steps.append(CheckStep("data", False, f"no data in {bucket!r} from {span}"))
            return result
        result.steps.append(CheckStep("data", True, f"found data in {bucket!r} from {span}"))

        return result
    finally:
        client.close()
