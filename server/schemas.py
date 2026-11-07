import uuid
from datetime import datetime
from pydantic import BaseModel, ConfigDict
from db.sunbeamdb.models import ConnectionKind, EventStatus, WorkerKind, WorkerStatus


class LaunchWorkerRequest(BaseModel):
    # The worker runs the event's own pipeline edition
    event_id: int
    # The InfluxDB connection the worker reads from; None means the default connection
    influx_connection_id: int | None = None
    # A git branch, tag, or full commit SHA to build the worker from; None means the server's own copy of the code
    source_ref: str | None = None
    # Wipe the event's earlier samples first. Always applied for replays (past events), whatever this says
    reprocess: bool = False


class EventCreate(BaseModel):
    name: str
    vehicle_id: int
    description: str | None = None
    starts_at: datetime
    # None for an ongoing event, which a worker runs live
    ends_at: datetime | None = None
    pipeline_edition: str
    stages: list[str]


class EventRead(BaseModel):
    id: int
    name: str
    vehicle_id: int
    description: str | None
    starts_at: datetime
    ends_at: datetime | None
    pipeline_edition: str
    stages: list[str]
    # Those the server's copy of the code doesn't have, so the event can only be launched from a git ref that does
    unregistered_stages: list[str]
    status: EventStatus
    # The event has ended: a worker launched for it replays it, always wiping its earlier samples first
    is_replay: bool
    # A worker run for the event hasn't reached a terminal status
    running: bool
    # Samples stored for the event; recounted at most every 5 s while it's ongoing, otherwise when its status changes
    sample_count: int


class VehicleCreate(BaseModel):
    name: str
    description: str | None = None


class VehicleUpdate(BaseModel):
    """ Only the fields sent change. """
    name: str | None = None
    description: str | None = None


class VehicleRead(BaseModel):
    id: int
    name: str
    description: str | None
    # A vehicle can only be deleted once it has none
    event_count: int


class PipelineEditionRead(BaseModel):
    id: str
    name: str
    # This copy of the code's stages for the edition, but ingress (the pipeline adds it itself)
    stages: list[str]


class EventUpdate(BaseModel):
    """ Only the fields sent change; send ends_at as null to make the event ongoing. """
    name: str | None = None
    vehicle_id: int | None = None
    description: str | None = None
    starts_at: datetime | None = None
    ends_at: datetime | None = None
    pipeline_edition: str | None = None
    stages: list[str] | None = None


class ConnectionFields(BaseModel):
    url: str
    organization: str
    bucket: str
    token: str


class ConnectionCreate(ConnectionFields):
    name: str
    is_default: bool = False


class ConnectionUpdate(BaseModel):
    name: str | None = None
    url: str | None = None
    organization: str | None = None
    bucket: str | None = None
    token: str | None = None


class ConnectionRead(ConnectionFields):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    kind: ConnectionKind
    is_default: bool
    created_at: datetime
    updated_at: datetime
    last_tested_at: datetime | None
    last_test_ok: bool | None
    last_test_detail: str | None


class ConnectionTestRequest(ConnectionFields):
    # Also require data over this event's window (see db.telemetrydb.connection_check.event_window)
    event_id: int | None = None


class ConnectionTestStep(BaseModel):
    name: str
    ok: bool
    detail: str


class ConnectionTestResult(BaseModel):
    ok: bool
    detail: str
    steps: list[ConnectionTestStep]
    # The span checked for data; None when no event was given, or it hasn't started
    window_start: datetime | None = None
    window_end: datetime | None = None


class WorkerRunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_id: int
    pipeline_edition: str
    image_tag: str | None
    status: WorkerStatus
    kind: WorkerKind

    host: str | None
    container_id: str | None
    container_name: str | None

    status_message: str | None
    stop_requested: bool
    influx_connection_id: int | None
    influx_connection_name: str | None
    source_ref: str | None
    source_sha: str | None
    reprocess: bool

    created_at: datetime
    started_at: datetime | None
    last_heartbeat_at: datetime | None
    stopped_at: datetime | None
    failure_reason: str | None


class SignalInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    unit: str | None
    frequency: float | None
    source: str


class SignalDataResponse(BaseModel):
    event_name: str
    signal: str
    unit: str | None
    frequency: float | None
    source: str  # raw, derived
    description: str | None

    start: datetime
    end: datetime
    count: int
    truncated: bool

    # Columnar, ascending; timestamps are epoch milliseconds.
    timestamps: list[int]
    values: list[float | None]


class WorkerHeartbeatRequest(BaseModel):
    status: WorkerStatus = WorkerStatus.RUNNING
    status_message: str | None = None
    host: str | None = None


class WorkerPermissionResponse(BaseModel):
    allowed: bool
    reason: str | None = None


class WorkerRegisterRequest(BaseModel):
    event_name: str
    pipeline_edition: str
    host: str | None = None
    # How the worker describes the InfluxDB it reads from, for display
    influx_connection_name: str | None = None
    reprocess: bool = False


class WorkerCompleteRequest(BaseModel):
    success: bool
    message: str | None = None


class StageMetric(BaseModel):
    name: str
    total_ms: float
    avg_ms: float
    max_ms: float
    calls: int


class PipelineMetric(BaseModel):
    name: str
    frequency: float | None = None  # Hz
    total_ms: float
    avg_ms: float
    max_ms: float | None = None  # The slowest tick; not reported by workers built before it was
    ticks: int
    late_now_ms: float
    late_max_ms: float
    stages: list[StageMetric] = []


class WriterQueueStats(BaseModel):
    queue_depth: int
    queue_capacity: int
    queue_high_water: int
    frames_enqueued: int
    frames_written: int
    batches_flushed: int
    avg_flush_ms: float
    max_flush_ms: float


class ReplayProgressStats(BaseModel):
    """How far a replay worker has got through its window of simulated time."""
    start: datetime
    end: datetime
    current: datetime
    fraction: float
    processed_s: float
    total_s: float
    elapsed_s: float
    speed: float | None = None  # Times faster than realtime; None until anything is processed
    eta_s: float | None = None


class WorkerMetricsReport(BaseModel):
    idle_pct: float
    busy_pct: float
    writer_ms: float
    pipelines: list[PipelineMetric] = []
    writer_queue: WriterQueueStats | None = None
    replay: ReplayProgressStats | None = None  # Only reported by replay workers


class WorkerMetricsRead(WorkerMetricsReport):
    reported_at: datetime