import hashlib
import uuid
from datetime import timedelta

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from eventhub.contracts import CaseDefinition, Envelope, checksum
from eventhub.db import now, uid
from eventhub.models import (
    Audit,
    Change,
    Delivery,
    DeliveryRun,
    Destination,
    Event,
    Outbox,
    Project,
    ReplayCase,
    ReplayRun,
    RequestLimit,
)
from eventhub.security import encrypt, target_secret

TERMINAL = {"accepted", "exhausted", "cancelled"}


async def lock_project(session, project_id):
    project = await session.scalar(select(Project).where(Project.id == project_id).with_for_update())
    if not project or not project.active:
        raise HTTPException(404, "Project unavailable")
    return project


def change(session, project, kind, **values):
    project.stream_seq += 1
    session.add(Change(project_id=project.id, seq=project.stream_seq, data={"kind": kind, **values}))


def audit(session, project_id, action, **details):
    session.add(Audit(project_id=project_id, action=action, details=details))


async def rate_limit(db, identity, limit=2000):
    key, current = hashlib.sha256(identity.encode()).hexdigest(), now()
    async with db.session() as session, session.begin():
        await session.execute(
            insert(RequestLimit).values(key=key, window=current, count=0).on_conflict_do_nothing()
        )
        row = await session.get(RequestLimit, key, with_for_update=True)
        if (current - row.window).total_seconds() >= 60:
            row.window, row.count = current, 0
        if row.count >= limit:
            raise HTTPException(429, "Request limit reached", headers={"Retry-After": "60"})
        row.count += 1


async def create_destination(db, config, key, name, adapter):
    if not isinstance(adapter, str) or adapter not in {"lab-a", "lab-b"}:
        raise HTTPException(422, "Only registered prepared receivers are allowed")
    identifier = uid()
    token = getattr(config, f"lab_{adapter[-1]}_token").get_secret_value()
    secret = encrypt(config, target_secret(token, key.project_id, identifier))
    async with db.session() as s, s.begin():
        project = await lock_project(s, key.project_id)
        row = Destination(id=identifier, project_id=project.id, name=name, adapter=adapter, secret=secret)
        s.add(row)
        audit(s, project.id, "destination.created", destination_id=str(identifier), adapter=adapter)
        change(s, project, "destination", id=str(identifier))
    return row


async def ingest(db, config, key, envelope: Envelope):
    if "*" not in key.sources and envelope.source not in key.sources:
        raise HTTPException(403, "Source is not allowed for this key")
    payload, identifier, current = envelope.payload(), uid(), now()
    digest = checksum(payload)
    async with db.session() as s, s.begin():
        project = await lock_project(s, key.project_id)
        existing = await s.scalar(
            select(Event).where(
                Event.project_id == project.id,
                Event.source == envelope.source,
                Event.external_id == envelope.id,
            )
        )
        if existing:
            if existing.payload_hash != digest:
                raise HTTPException(409, "Event identity already has different content")
            return existing, True
        count = await s.scalar(select(func.count()).select_from(Event).where(Event.project_id == project.id))
        if count >= 100000:
            raise HTTPException(429, "Project event capacity reached", headers={"Retry-After": "3600"})
        event = Event(
            id=identifier,
            project_id=project.id,
            source=envelope.source,
            external_id=envelope.id,
            event_type=envelope.type,
            subject=envelope.subject,
            payload=payload,
            payload_hash=digest,
            received_at=current,
        )
        s.add(event)
        await s.flush()
        targets = (
            await s.scalars(
                select(Destination).where(
                    Destination.project_id == project.id,
                    Destination.active.is_(True),
                    Destination.event_type == envelope.type,
                )
            )
        ).all()
        for target in targets:
            delivery_id, run_id = uid(), uid()
            s.add(
                Delivery(
                    id=delivery_id,
                    project_id=project.id,
                    event_id=identifier,
                    destination_id=target.id,
                    current_run_id=run_id,
                    state="ready",
                    generation=1,
                    attempt_count=0,
                    next_at=current,
                    expires_at=current + timedelta(seconds=config.delivery_ttl_seconds),
                )
            )
            await s.flush()
            s.add(DeliveryRun(id=run_id, delivery_id=delivery_id, origin="automatic", request_key="initial"))
            s.add(Outbox(delivery_id=delivery_id, run_id=run_id, generation=1))
        change(s, project, "event", id=str(identifier))
    return event, False


async def retry_delivery(db, config, key, identifier, request_key):
    async with db.session() as s, s.begin():
        project = await lock_project(s, key.project_id)
        delivery = await s.scalar(
            select(Delivery)
            .where(Delivery.id == identifier, Delivery.project_id == project.id)
            .with_for_update()
        )
        if not delivery:
            raise HTTPException(404, "Delivery unavailable")
        previous = await s.scalar(
            select(DeliveryRun).where(
                DeliveryRun.delivery_id == identifier, DeliveryRun.request_key == request_key
            )
        )
        if previous:
            return previous.id
        if delivery.state not in TERMINAL:
            raise HTTPException(409, "An active delivery cannot be restarted")
        event = await s.get(Event, delivery.event_id)
        if event.payload is None:
            raise HTTPException(410, "Event payload retention has expired")
        target = await s.get(Destination, delivery.destination_id)
        if not target.active:
            raise HTTPException(409, "Destination is disabled")
        run_id = uid()
        delivery.current_run_id, delivery.state = run_id, "ready"
        delivery.generation += 1
        delivery.attempt_count, delivery.last_error = 0, ""
        delivery.next_at = now()
        delivery.expires_at = now() + timedelta(seconds=config.delivery_ttl_seconds)
        delivery.lease_token = delivery.lease_until = None
        s.add(DeliveryRun(id=run_id, delivery_id=identifier, origin="manual", request_key=request_key))
        s.add(Outbox(delivery_id=identifier, run_id=run_id, generation=delivery.generation))
        audit(s, project.id, "delivery.retried", delivery_id=str(identifier), run_id=str(run_id))
        change(s, project, "delivery", id=str(identifier))
        return run_id


async def save_case(db, key, definition):
    case = CaseDefinition.model_validate(definition)
    payload = case.model_dump(mode="json")
    async with db.session() as s, s.begin():
        project = await lock_project(s, key.project_id)
        existing = await s.scalar(
            select(ReplayCase).where(
                ReplayCase.project_id == project.id, ReplayCase.checksum == checksum(payload)
            )
        )
        if existing:
            return existing
        row = ReplayCase(
            id=uid(), project_id=project.id, name=case.name, definition=payload, checksum=checksum(payload)
        )
        s.add(row)
        audit(s, project.id, "case.imported", case_id=str(row.id), checksum=row.checksum)
        change(s, project, "case", id=str(row.id))
    return row


async def start_replay(db, key, case_id, request_key):
    async with db.session() as s, s.begin():
        project = await lock_project(s, key.project_id)
        previous = await s.scalar(
            select(ReplayRun).where(ReplayRun.project_id == project.id, ReplayRun.request_key == request_key)
        )
        if previous:
            if previous.case_id != case_id:
                raise HTTPException(409, "Request key was used for another case")
            return previous
        case = await s.scalar(
            select(ReplayCase).where(ReplayCase.id == case_id, ReplayCase.project_id == project.id)
        )
        if not case:
            raise HTTPException(404, "Case unavailable")
        active = await s.scalar(
            select(func.count())
            .select_from(ReplayRun)
            .where(
                ReplayRun.project_id == project.id, ReplayRun.state.in_(["pending", "running", "comparing"])
            )
        )
        if active >= 8:
            raise HTTPException(429, "Replay capacity reached", headers={"Retry-After": "10"})
        row = ReplayRun(id=uid(), project_id=project.id, case_id=case_id, request_key=request_key)
        s.add(row)
        audit(s, project.id, "replay.started", run_id=str(row.id))
        change(s, project, "replay", id=str(row.id))
        return row


async def set_cancel(db, key, run_id):
    async with db.session() as s, s.begin():
        project = await lock_project(s, key.project_id)
        row = await s.scalar(
            select(ReplayRun).where(ReplayRun.id == run_id, ReplayRun.project_id == project.id)
        )
        if not row:
            raise HTTPException(404, "Run unavailable")
        if row.state in {"pending", "running", "comparing"}:
            row.cancel_requested = True
            audit(s, project.id, "replay.cancel_requested", run_id=str(run_id))
            change(s, project, "replay", id=str(run_id))
        return row


def idempotency(value):
    if not value or not 1 <= len(value) <= 128 or any(ord(c) < 33 or ord(c) > 126 for c in value):
        raise HTTPException(400, "A bounded Idempotency-Key is required")
    return value


def identifier(value):
    try:
        return uuid.UUID(str(value))
    except ValueError, AttributeError:
        raise HTTPException(404, "Object unavailable") from None
