from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column as col

from eventhub.db import Base, now, uid


class Project(Base):
    __tablename__ = "projects"
    id: Mapped[UUID] = col(PGUUID, primary_key=True, default=uid)
    name: Mapped[str] = col(String(120))
    active: Mapped[bool] = col(Boolean, default=True)
    stream_seq: Mapped[int] = col(Integer, default=0)
    created_at: Mapped[datetime] = col(DateTime(timezone=True), default=now)


class APIKey(Base):
    __tablename__ = "api_keys"
    id: Mapped[UUID] = col(PGUUID, primary_key=True, default=uid)
    project_id: Mapped[UUID] = col(ForeignKey("projects.id"), index=True)
    prefix: Mapped[str] = col(String(24), unique=True)
    digest: Mapped[str] = col(String(64))
    scopes: Mapped[list] = col(JSONB)
    sources: Mapped[list] = col(JSONB, default=list)
    role: Mapped[str] = col(String(20), default="source")
    revoked: Mapped[bool] = col(Boolean, default=False)
    expires_at: Mapped[datetime | None] = col(DateTime(timezone=True), nullable=True)


class BrowserSession(Base):
    __tablename__ = "browser_sessions"
    digest: Mapped[str] = col(String(64), primary_key=True)
    key_id: Mapped[UUID] = col(ForeignKey("api_keys.id"))
    csrf: Mapped[str] = col(String(100))
    expires_at: Mapped[datetime] = col(DateTime(timezone=True))


class Destination(Base):
    __tablename__ = "destinations"
    id: Mapped[UUID] = col(PGUUID, primary_key=True, default=uid)
    project_id: Mapped[UUID] = col(ForeignKey("projects.id"), index=True)
    name: Mapped[str] = col(String(120))
    adapter: Mapped[str] = col(String(20))
    secret: Mapped[str] = col(Text)
    active: Mapped[bool] = col(Boolean, default=True)
    event_type: Mapped[str] = col(String(150), default="studio.booking.snapshot.v1")
    __table_args__ = (UniqueConstraint("project_id", "id"),)


class Event(Base):
    __tablename__ = "events"
    id: Mapped[UUID] = col(PGUUID, primary_key=True, default=uid)
    project_id: Mapped[UUID] = col(ForeignKey("projects.id"), index=True)
    source: Mapped[str] = col(String(200))
    external_id: Mapped[str] = col(String(128))
    event_type: Mapped[str] = col(String(150))
    subject: Mapped[str] = col(String(200))
    payload: Mapped[dict | None] = col(JSONB, nullable=True)
    payload_expired_at: Mapped[datetime | None] = col(DateTime(timezone=True), nullable=True)
    payload_hash: Mapped[str] = col(String(64))
    received_at: Mapped[datetime] = col(DateTime(timezone=True), default=now)
    __table_args__ = (
        UniqueConstraint("project_id", "source", "external_id"),
        UniqueConstraint("project_id", "id"),
        Index("event_project_time", "project_id", "received_at", "id"),
    )


class Delivery(Base):
    __tablename__ = "deliveries"
    id: Mapped[UUID] = col(PGUUID, primary_key=True, default=uid)
    project_id: Mapped[UUID] = col(ForeignKey("projects.id"), index=True)
    event_id: Mapped[UUID] = col(PGUUID)
    destination_id: Mapped[UUID] = col(PGUUID)
    current_run_id: Mapped[UUID] = col(PGUUID, default=uid)
    state: Mapped[str] = col(String(20), default="ready", index=True)
    generation: Mapped[int] = col(Integer, default=1)
    attempt_count: Mapped[int] = col(Integer, default=0)
    next_at: Mapped[datetime] = col(DateTime(timezone=True), default=now, index=True)
    expires_at: Mapped[datetime] = col(DateTime(timezone=True))
    lease_token: Mapped[UUID | None] = col(PGUUID, nullable=True)
    lease_until: Mapped[datetime | None] = col(DateTime(timezone=True), nullable=True, index=True)
    last_error: Mapped[str] = col(String(80), default="")
    created_at: Mapped[datetime] = col(DateTime(timezone=True), default=now)
    __table_args__ = (
        UniqueConstraint("event_id", "destination_id"),
        CheckConstraint("attempt_count >= 0 AND generation > 0", name="delivery_positive_counters"),
        CheckConstraint(
            "state IN ('ready','in_flight','retry_wait','accepted','exhausted','cancelled')",
            name="delivery_state",
        ),
        CheckConstraint(
            "state <> 'in_flight' OR (lease_token IS NOT NULL AND lease_until IS NOT NULL)",
            name="delivery_lease",
        ),
        ForeignKeyConstraint(["project_id", "event_id"], ["events.project_id", "events.id"]),
        ForeignKeyConstraint(
            ["project_id", "destination_id"], ["destinations.project_id", "destinations.id"]
        ),
    )


class DeliveryRun(Base):
    __tablename__ = "delivery_runs"
    id: Mapped[UUID] = col(PGUUID, primary_key=True)
    delivery_id: Mapped[UUID] = col(ForeignKey("deliveries.id"), index=True)
    origin: Mapped[str] = col(String(20))
    request_key: Mapped[str] = col(String(128))
    created_at: Mapped[datetime] = col(DateTime(timezone=True), default=now)
    finished_at: Mapped[datetime | None] = col(DateTime(timezone=True), nullable=True)
    outcome: Mapped[str | None] = col(String(30), nullable=True)
    __table_args__ = (UniqueConstraint("delivery_id", "request_key"),)


class Attempt(Base):
    __tablename__ = "attempts"
    id: Mapped[UUID] = col(PGUUID, primary_key=True, default=uid)
    delivery_id: Mapped[UUID] = col(ForeignKey("deliveries.id"), index=True)
    run_id: Mapped[UUID] = col(ForeignKey("delivery_runs.id"))
    number: Mapped[int] = col(Integer)
    started_at: Mapped[datetime] = col(DateTime(timezone=True), default=now)
    finished_at: Mapped[datetime | None] = col(DateTime(timezone=True), nullable=True)
    outcome: Mapped[str] = col(String(40), default="started")
    status_code: Mapped[int | None] = col(Integer, nullable=True)
    elapsed_ms: Mapped[int | None] = col(Integer, nullable=True)
    __table_args__ = (
        UniqueConstraint("run_id", "number"),
        CheckConstraint("number > 0", name="attempt_number"),
    )


class Outbox(Base):
    __tablename__ = "outbox"
    id: Mapped[UUID] = col(PGUUID, primary_key=True, default=uid)
    delivery_id: Mapped[UUID] = col(ForeignKey("deliveries.id"))
    run_id: Mapped[UUID] = col(PGUUID)
    generation: Mapped[int] = col(Integer)
    published: Mapped[bool] = col(Boolean, default=False, index=True)
    lease_token: Mapped[UUID | None] = col(PGUUID, nullable=True)
    lease_until: Mapped[datetime | None] = col(DateTime(timezone=True), nullable=True)
    __table_args__ = (UniqueConstraint("delivery_id", "run_id", "generation"),)


class Change(Base):
    __tablename__ = "changes"
    project_id: Mapped[UUID] = col(ForeignKey("projects.id"), primary_key=True)
    seq: Mapped[int] = col(Integer, primary_key=True)
    data: Mapped[dict] = col(JSONB)
    created_at: Mapped[datetime] = col(DateTime(timezone=True), default=now)


class Audit(Base):
    __tablename__ = "audit"
    id: Mapped[UUID] = col(PGUUID, primary_key=True, default=uid)
    project_id: Mapped[UUID] = col(ForeignKey("projects.id"), index=True)
    action: Mapped[str] = col(String(80))
    details: Mapped[dict] = col(JSONB, default=dict)
    created_at: Mapped[datetime] = col(DateTime(timezone=True), default=now)


class ReplayCase(Base):
    __tablename__ = "replay_cases"
    id: Mapped[UUID] = col(PGUUID, primary_key=True, default=uid)
    project_id: Mapped[UUID] = col(ForeignKey("projects.id"), index=True)
    name: Mapped[str] = col(String(120))
    definition: Mapped[dict] = col(JSONB)
    checksum: Mapped[str] = col(String(64))


class ReplayRun(Base):
    __tablename__ = "replay_runs"
    id: Mapped[UUID] = col(PGUUID, primary_key=True, default=uid)
    project_id: Mapped[UUID] = col(ForeignKey("projects.id"), index=True)
    case_id: Mapped[UUID] = col(ForeignKey("replay_cases.id"))
    request_key: Mapped[str] = col(String(128))
    state: Mapped[str] = col(String(20), default="pending")
    cancel_requested: Mapped[bool] = col(Boolean, default=False)
    report: Mapped[dict | None] = col(JSONB, nullable=True)
    checkpoint: Mapped[dict] = col(JSONB, default=dict)
    lease_token: Mapped[UUID | None] = col(PGUUID, nullable=True)
    lease_until: Mapped[datetime | None] = col(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = col(DateTime(timezone=True), default=now)
    finished_at: Mapped[datetime | None] = col(DateTime(timezone=True), nullable=True)
    __table_args__ = (UniqueConstraint("project_id", "request_key"),)


class RequestLimit(Base):
    __tablename__ = "request_limits"
    key: Mapped[str] = col(String(64), primary_key=True)
    window: Mapped[datetime] = col(DateTime(timezone=True))
    count: Mapped[int] = col(Integer, default=0)
