import enum
from datetime import datetime
from typing import Optional
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    func,
    UniqueConstraint,
    Enum
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid

class Base(DeclarativeBase):
    pass


class Vehicle(Base):
    __tablename__ = "vehicle"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    events: Mapped[list["Event"]] = relationship(back_populates="vehicle")


class EventStatus(enum.Enum):
    UNPROCESSED = "unprocessed"
    ONGOING = "ongoing"
    PROCESSED = "processed"

class Event(Base):
    __tablename__ = "event"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    vehicle_id: Mapped[int] = mapped_column(ForeignKey("vehicle.id"), nullable=False)

    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    pipeline_edition: Mapped[str] = mapped_column(String(32), nullable=False)

    status: Mapped[EventStatus] = mapped_column(Enum(EventStatus), nullable=False)  # planned, active, complete
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    vehicle: Mapped["Vehicle"] = relationship(back_populates="events")
    signals: Mapped[list["Signal"]] = relationship(back_populates="event")


class Signal(Base):
    __tablename__ = "signal"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    unit: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)  # raw, derived
    frequency: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("event.id"), nullable=False)

    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    event: Mapped["Event"] = relationship(back_populates="signals")

    __table_args__ = (
        UniqueConstraint("event_id", "name", name="uq_signal_event_name"),
    )


class RawSample(Base):
    __tablename__ = "raw_sample"

    event_id: Mapped[int] = mapped_column(ForeignKey("event.id"), nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    signal_id: Mapped[int] = mapped_column(ForeignKey("signal.id"), nullable=False)

    value_f64: Mapped[float] = mapped_column(Float, nullable=False)

    ingest_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    source_message_type: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    source_sequence: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint("event_id", "signal_id", "ts", name="pk_raw_sample"),
    )


class AlignedSample(Base):
    __tablename__ = "aligned_sample"

    event_id: Mapped[int] = mapped_column(ForeignKey("event.id"), nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    signal_id: Mapped[int] = mapped_column(ForeignKey("signal.id"), nullable=False)

    value_f64: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint(
            "event_id",
            "signal_id",
            "ts",
            name="pk_aligned_sample",
        ),
    )

class WorkerStatus(enum.Enum):

    REQUESTED = "requested"

    STARTING = "starting"

    RUNNING = "running"

    STOP_REQUESTED = "stop_requested"

    STOPPING = "stopping"

    COMPLETED = "completed"

    FAILED = "failed"

    LOST = "lost"

    CANCELLED = "cancelled"


TERMINAL_WORKER_STATUSES = {
    WorkerStatus.COMPLETED,
    WorkerStatus.FAILED,
    WorkerStatus.CANCELLED,
    WorkerStatus.LOST,
}

class WorkerRun(Base):

    __tablename__ = "worker_run"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    event_id: Mapped[int] = mapped_column(
        ForeignKey("event.id"),
        nullable=False,
    )

    pipeline_edition: Mapped[str] = mapped_column(String(32), nullable=False)
    image_tag: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[WorkerStatus] = mapped_column(
        Enum(WorkerStatus),
        nullable=False,
        default=WorkerStatus.REQUESTED,
    )

    host: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    container_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    container_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    current_stage: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    status_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stop_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    stop_requested_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_heartbeat_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    stopped_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    failure_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    event: Mapped["Event"] = relationship()
