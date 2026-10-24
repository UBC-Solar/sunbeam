import uuid
from datetime import datetime
from pydantic import BaseModel, ConfigDict
from db.sunbeamdb.models import WorkerKind, WorkerStatus


class LaunchWorkerRequest(BaseModel):
    event_id: int
    pipeline_edition: str
    # Which context.toml profile the worker loads; None means the server's own profile
    configuration_profile: str | None = None
    # Wipe the event's earlier samples first. Always applied for replays (past events), whatever this says
    reprocess: bool = False


class ConfigurationProfilesRead(BaseModel):
    profiles: list[str]
    default: str


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

    current_stage: str | None
    status_message: str | None
    stop_requested: bool
    configuration_profile: str | None
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

    start: datetime
    end: datetime
    count: int
    truncated: bool

    # Columnar, ascending; timestamps are epoch milliseconds.
    timestamps: list[int]
    values: list[float | None]


class WorkerHeartbeatRequest(BaseModel):
    status: WorkerStatus = WorkerStatus.RUNNING
    current_stage: str | None = None
    status_message: str | None = None
    host: str | None = None


class WorkerPermissionResponse(BaseModel):
    allowed: bool
    reason: str | None = None
    stop_requested: bool


class WorkerRegisterRequest(BaseModel):
    event_name: str
    pipeline_edition: str
    host: str | None = None
    configuration_profile: str | None = None
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
    total_ms: float
    avg_ms: float
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