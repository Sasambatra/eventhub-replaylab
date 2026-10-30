import asyncio
from contextlib import asynccontextmanager
import hashlib
import hmac
import os
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import Field, ValidationError
from sqlalchemy import URL, delete, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from eventhub.contracts import Envelope, StrictModel, checksum, strict_json
from eventhub.security import target_secret, verify_signature
from eventhub.services import identifier
from lab.models import Effect, Invocation, Receipt, ReceiverState


class Invoke(StrictModel):
    run_id: str = Field(min_length=1, max_length=128)
    invocation_id: str = Field(min_length=1, max_length=128)
    event: Envelope
    fault: Literal["none", "delay", "return_500", "return_429", "commit_then_delay_response"] = "none"
    virtual_time: str = Field(max_length=80)


def create_lab_app(variant=None, database_url=None, token=None):
    variant = variant or os.environ["LAB_VARIANT"]
    if variant not in {"A", "B"}:
        raise ValueError("A prepared variant is required")
    token = token or os.environ["LAB_TOKEN"]
    database_url = database_url or URL.create(
        "postgresql+psycopg",
        username=os.environ["LAB_DB_USER"],
        password=os.environ["LAB_DB_PASSWORD"],
        host=os.environ.get("LAB_DB_HOST", "127.0.0.1"),
        port=int(os.environ.get("LAB_DB_PORT", "55432")),
        database=os.environ["LAB_DB_NAME"],
    )
    engine = create_async_engine(database_url, pool_pre_ping=True, hide_parameters=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    version = "receiver-" + variant + "-" + hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:16]

    @asynccontextmanager
    async def lifespan(app):
        yield
        await engine.dispose()

    app = FastAPI(title=f"Prepared receiver {variant}", lifespan=lifespan, docs_url=None, redoc_url=None)

    @app.exception_handler(ValidationError)
    async def invalid_packet(request, exc):
        return JSONResponse({"detail": "Invalid receiver packet"}, status_code=422)

    @app.exception_handler(SQLAlchemyError)
    async def unavailable(request, exc):
        return JSONResponse({"detail": "Receiver temporarily unavailable"}, status_code=503)

    async def read_body(request, limit):
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > limit:
                raise HTTPException(413, "Receiver packet too large")
        return bytes(body)

    def parse(body):
        try:
            return strict_json(body)
        except ValueError, UnicodeError, RecursionError:
            raise HTTPException(400, "Invalid receiver JSON") from None

    @app.delete("/control/state/{run_id}")
    async def erase_experiment(run_id: str, request: Request):
        authenticate(request)
        identifier(run_id)  # Only isolated replay UUIDs, never a live namespace.
        async with sessions() as s, s.begin():
            lock = int.from_bytes(hashlib.sha256(run_id.encode()).digest()[:8], "big", signed=True)
            await s.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock})
            for model in (Invocation, Effect, Receipt, ReceiverState):
                await s.execute(delete(model).where(model.run_id == run_id))
        return {"erased": True}

    def authenticate(request):
        if not hmac.compare_digest(request.headers.get("authorization", ""), "Bearer " + token):
            raise HTTPException(401, "Lab access required")

    async def apply(session, run_id, envelope):
        # A transaction lock serializes state and receipts for the same controlled run.
        lock = int.from_bytes(hashlib.sha256(run_id.encode()).digest()[:8], "big", signed=True)
        await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock})
        data, source, subject = envelope.data, envelope.source, envelope.subject
        digest = checksum(envelope.payload())
        receipt_key = (run_id, source, envelope.id)
        if variant == "B":
            receipt = await session.get(Receipt, receipt_key)
            if receipt:
                if receipt.payload_hash != digest:
                    raise HTTPException(409, "Repeated event has conflicting content")
                return "duplicate"
        state = await session.get(ReceiverState, (run_id, source, subject))
        outcome = "applied"
        if variant == "B" and state and data.revision <= state.revision:
            if data.revision == state.revision and data.status != state.status:
                raise HTTPException(409, "Snapshot revision has conflicting state")
            outcome = "stale"
        else:
            if state:
                state.revision, state.status = data.revision, data.status
            else:
                session.add(
                    ReceiverState(
                        run_id=run_id,
                        source=source,
                        subject=subject,
                        revision=data.revision,
                        status=data.status,
                    )
                )
            session.add(
                Effect(run_id=run_id, event_id=envelope.id, revision=data.revision, status=data.status)
            )
        if variant == "B":
            session.add(
                Receipt(
                    run_id=run_id, source=source, event_id=envelope.id, payload_hash=digest, outcome=outcome
                )
            )
        return outcome

    async def observation(session, run_id):
        states = (await session.scalars(select(ReceiverState).where(ReceiverState.run_id == run_id))).all()
        effects = (
            await session.scalars(
                select(Effect).where(Effect.run_id == run_id).order_by(Effect.revision, Effect.event_id)
            )
        ).all()
        return {
            "states": sorted(
                [
                    {"source": s.source, "subject": s.subject, "revision": s.revision, "status": s.status}
                    for s in states
                ],
                key=lambda s: (s["source"], s["subject"]),
            ),
            "effects": [
                {"event_id": e.event_id, "revision": e.revision, "status": e.status} for e in effects
            ],
        }

    @app.get("/health")
    async def health():
        async with sessions() as s:
            await s.execute(text("SELECT 1"))
        return {"status": "ready", "variant": variant, "version": version}

    @app.post("/invoke")
    async def invoke(request: Request):
        authenticate(request)
        body = await read_body(request, 131072)
        packet = Invoke.model_validate(parse(body))
        identifier(packet.run_id)
        digest = checksum(packet.model_dump(mode="json"))
        lock = int.from_bytes(hashlib.sha256(packet.run_id.encode()).digest()[:8], "big", signed=True)
        async with sessions() as s, s.begin():
            await s.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock})
            previous = await s.get(Invocation, packet.invocation_id)
            if previous:
                if previous.run_id != packet.run_id or previous.payload_hash != digest:
                    raise HTTPException(409, "Invocation conflict")
                result = previous.result
            else:
                status = 500 if packet.fault == "return_500" else 429 if packet.fault == "return_429" else 200
                outcome = await apply(s, packet.run_id, packet.event) if status == 200 else "not_applied"
                await s.flush()
                result = {
                    "outcome": outcome,
                    "status_code": status,
                    "fault": packet.fault,
                    "observation": await observation(s, packet.run_id),
                    "version": version,
                }
                s.add(
                    Invocation(
                        id=packet.invocation_id, run_id=packet.run_id, payload_hash=digest, result=result
                    )
                )
        if packet.fault == "commit_then_delay_response":
            await asyncio.sleep(0.4)
        elif packet.fault == "delay":
            await asyncio.sleep(0.04)
        headers = {"Retry-After": "1"} if result["status_code"] == 429 else {}
        return JSONResponse(result, status_code=result["status_code"], headers=headers)

    @app.get("/control/invocations/{invocation_id}")
    async def get_invocation(invocation_id: str, request: Request):
        authenticate(request)
        async with sessions() as s:
            row = await s.get(Invocation, invocation_id)
            if not row:
                raise HTTPException(404, "No committed invocation")
            return {"run_id": row.run_id, **row.result}

    @app.get("/control/state/{run_id}")
    async def get_state(run_id: str, request: Request):
        authenticate(request)
        if len(run_id) > 128:
            raise HTTPException(400, "Invalid namespace")
        async with sessions() as s:
            return await observation(s, run_id)

    @app.post("/webhook")
    async def webhook(request: Request):
        body = await read_body(request, 65536)
        project_id = identifier(request.headers.get("x-eventhub-project", ""))
        destination_id = identifier(request.headers.get("x-eventhub-destination", ""))
        secret = target_secret(token, project_id, destination_id)
        if not verify_signature(secret, request.headers, body):
            raise HTTPException(401, "Signature verification failed")
        envelope = Envelope.model_validate(parse(body))
        namespace = f"live:{project_id}:{destination_id}"
        async with sessions() as s, s.begin():
            outcome = await apply(s, namespace, envelope)
        return {"accepted": True, "outcome": outcome}

    return app
