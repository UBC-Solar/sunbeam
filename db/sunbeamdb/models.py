from __future__ import annotations

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