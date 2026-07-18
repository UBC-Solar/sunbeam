import uuid
from datetime import datetime
from pydantic import BaseModel, ConfigDict
from db.sunbeamdb.models import WorkerStatus


class LaunchWorkerRequest(BaseModel):
    event_id: int
    pipeline_edition: str


class WorkerRunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_id: int
    pipeline_edition: str
    image_tag: str
    status: WorkerStatus

    host: str | None
    container_id: str | None
    container_name: str | None

    current_stage: str | None
    status_message: str | None
    stop_requested: bool

    created_at: datetime
    started_at: datetime | None
    last_heartbeat_at: datetime | None
    stopped_at: datetime | None
    failure_reason: str | None


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
    event_id: int
    pipeline_edition: str
    host: str | None = None
    container_id: str | None = None


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


class WorkerMetricsReport(BaseModel):
    idle_pct: float
    busy_pct: float
    writer_ms: float
    pipelines: list[PipelineMetric] = []


class WorkerMetricsRead(WorkerMetricsReport):
    reported_at: datetime