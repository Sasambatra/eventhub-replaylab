from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column as col

from eventhub.db import now, uid


class LabBase(DeclarativeBase):
    pass


class ReceiverState(LabBase):
    __tablename__ = "receiver_state"
    run_id: Mapped[str] = col(String(128), primary_key=True)
    source: Mapped[str] = col(String(200), primary_key=True)
    subject: Mapped[str] = col(String(200), primary_key=True)
    revision: Mapped[int] = col(Integer)
    status: Mapped[str] = col(String(20))


class Receipt(LabBase):
    __tablename__ = "receipts"
    run_id: Mapped[str] = col(String(128), primary_key=True)
    source: Mapped[str] = col(String(200), primary_key=True)
    event_id: Mapped[str] = col(String(128), primary_key=True)
    payload_hash: Mapped[str] = col(String(64))
    outcome: Mapped[str] = col(String(30))


class Effect(LabBase):
    __tablename__ = "effects"
    id: Mapped[UUID] = col(PGUUID, primary_key=True, default=uid)
    run_id: Mapped[str] = col(String(128), index=True)
    event_id: Mapped[str] = col(String(128))
    revision: Mapped[int] = col(Integer)
    status: Mapped[str] = col(String(20))


class Invocation(LabBase):
    __tablename__ = "invocations"
    id: Mapped[str] = col(String(128), primary_key=True)
    run_id: Mapped[str] = col(String(128), index=True)
    payload_hash: Mapped[str] = col(String(64))
    result: Mapped[dict] = col(JSONB)
    created_at: Mapped[datetime] = col(DateTime(timezone=True), default=now)
