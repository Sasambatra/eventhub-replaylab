import asyncio
import base64
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import hmac
import json
import secrets

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    StreamingResponse,
    PlainTextResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError
from sqlalchemy import func, or_, select
from sqlalchemy.exc import SQLAlchemyError
from starlette.middleware.trustedhost import TrustedHostMiddleware

from eventhub.contracts import CaseDefinition, Envelope, canonical, checksum, strict_json
from eventhub.db import Database, now
from eventhub.models import (
    APIKey,
    Attempt,
    BrowserSession,
    Change,
    Delivery,
    Destination,
    Event,
    Project,
    Outbox,
    ReplayCase,
    ReplayRun,
)
from eventhub.security import SCOPES, authorize, check_origin, digest, find_key, make_session, new_key
from eventhub.services import (
    audit,
    change,
    create_destination,
    idempotency,
    identifier,
    ingest,
    lock_project,
    rate_limit,
    retry_delivery,
    save_case,
    set_cancel,
    start_replay,
)
from eventhub.settings import ROOT, Settings
from eventhub.telemetry import span


async def read_json(request, limit):
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > limit:
            raise HTTPException(413, "Body exceeds the limit")
    try:
        return strict_json(body)
    except ValueError, RecursionError, UnicodeError:
        raise HTTPException(400, "Invalid bounded JSON object") from None


def event_data(row):
    return {
        "id": str(row.id),
        "external_id": row.external_id,
        "source": row.source,
        "type": row.event_type,
        "subject": row.subject,
        "received_at": row.received_at.isoformat(),
        "payload": row.payload,
        "payload_expired_at": row.payload_expired_at.isoformat() if row.payload_expired_at else None,
        "checksum": row.payload_hash,
    }


def run_data(row):
    return {
        "id": str(row.id),
        "case_id": str(row.case_id),
        "state": row.state,
        "created_at": row.created_at.isoformat(),
        "report": row.report,
    }


def safe_export(definition):
    case = CaseDefinition.model_validate(definition)
    values, ids = [], {}
    for index, event in enumerate(case.events, 1):
        new_id = f"event-{index:03}"
        ids[event.id] = new_id
        values.append(
            {
                "specversion": "1.0",
                "id": new_id,
                "source": "urn:demo:export",
                "type": event.type,
                "subject": "booking/00000000-0000-4000-8000-000000000001",
                "time": case.start_time.isoformat(),
                "datacontenttype": "application/json",
                "data": {
                    "booking_id": "00000000-0000-4000-8000-000000000001",
                    "revision": event.data.revision,
                    "status": event.data.status,
                },
            }
        )
    result = {
        "case_version": 1,
        "name": "Exported snapshot incident",
        "description": "Sanitized prepared replay",
        "seed": case.seed,
        "start_time": case.start_time.isoformat(),
        "events": values,
        "steps": [{**step.model_dump(), "event_id": ids[step.event_id]} for step in case.steps],
        "expected": case.expected.model_dump(),
        "expected_variants": case.expected_variants,
    }
    return {"checksum": checksum(result), "case": result}


def create_app(config=None):
    config = config or Settings()
    db = Database(config)

    @asynccontextmanager
    async def lifespan(app):
        yield
        await db.close()

    app = FastAPI(title="EventHub ReplayLab", version="0.1.0", lifespan=lifespan)
    app.state.config, app.state.db = config, db
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=["127.0.0.1", "localhost", "testserver", "api"])
    templates = Jinja2Templates(directory=str(ROOT / "eventhub/ui/templates"))
    app.mount("/static", StaticFiles(directory=ROOT / "eventhub/ui/static"), name="static")

    @app.middleware("http")
    async def headers(request, call_next):
        if (
            request.headers.get("content-length", "").isdigit()
            and int(request.headers["content-length"]) > config.max_case_bytes
        ):
            return JSONResponse({"detail": "Body exceeds the limit"}, status_code=413)
        response = await call_next(request)
        response.headers.update(
            {
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
                "X-Frame-Options": "DENY",
                "Referrer-Policy": "no-referrer",
                "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
            }
        )
        return response

    async def validation_error(request, error):
        return JSONResponse(
            {"detail": [{"location": list(item["loc"]), "message": item["type"]} for item in error.errors()]},
            status_code=422,
        )

    app.add_exception_handler(ValidationError, validation_error)
    app.add_exception_handler(RequestValidationError, validation_error)

    @app.exception_handler(SQLAlchemyError)
    async def database_error(request, error):
        return JSONResponse({"detail": "Database temporarily unavailable"}, status_code=503)

    @app.get("/health/live")
    async def live():
        return {"status": "alive"}

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon():
        return RedirectResponse("/static/icon.svg")

    @app.get("/health/ready")
    async def ready():
        async with db.session() as s:
            await s.execute(select(Project.id).limit(1))
        return {"status": "ready", "mode": "local-demo", "delivery": "asynchronous"}

    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request):
        csrf = secrets.token_urlsafe(32)
        response = templates.TemplateResponse(request=request, name="login.html", context={"csrf": csrf})
        response.set_cookie(
            "eh_login", csrf, httponly=True, samesite="strict", secure=config.secure_cookie, max_age=600
        )
        return response

    @app.post("/session")
    async def login(request: Request):
        check_origin(request)
        if not hmac.compare_digest(
            request.headers.get("x-csrf-token", ""), request.cookies.get("eh_login", "!")
        ):
            raise HTTPException(403, "Login CSRF verification failed")
        await rate_limit(db, "login:" + (request.client.host if request.client else "unknown"), 10)
        body = await read_json(request, 4096)
        raw = body.get("key", "") if isinstance(body, dict) else ""
        if not isinstance(raw, str) or len(raw) > 200:
            raise HTTPException(401, "Invalid access key")
        async with db.session() as s, s.begin():
            key = await find_key(s, config, raw)
            if not key or key.role == "source":
                raise HTTPException(401, "A dashboard access key is required")
            session, csrf = await make_session(s, config, key)
        response = JSONResponse({"ok": True})
        response.set_cookie(
            "eh_session",
            session,
            httponly=True,
            samesite="strict",
            secure=config.secure_cookie,
            max_age=config.session_seconds,
        )
        response.delete_cookie("eh_login")
        return response

    @app.post("/session/logout")
    async def logout(request: Request):
        await authorize(request)
        async with db.session() as s, s.begin():
            row = await s.get(BrowserSession, digest(config, request.cookies.get("eh_session", "")))
            if row:
                await s.delete(row)
        response = JSONResponse({"ok": True})
        response.delete_cookie("eh_session")
        return response

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        try:
            key = await authorize(request)
        except HTTPException:
            return RedirectResponse("/login", status_code=303)
        async with db.session() as s:
            project = await s.get(Project, key.project_id)
            browser = await s.get(BrowserSession, digest(config, request.cookies.get("eh_session", "")))
        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "project": project,
                "csrf": browser.csrf if browser else "",
                "scopes": key.scopes,
                "role": key.role,
            },
        )

    @app.get("/v1/overview")
    async def overview(request: Request):
        key = await authorize(request)
        async with db.session() as s:
            project = await s.get(Project, key.project_id)
            counts = dict(
                (
                    await s.execute(
                        select(Delivery.state, func.count())
                        .where(Delivery.project_id == key.project_id)
                        .group_by(Delivery.state)
                    )
                ).all()
            )
            events = await s.scalar(
                select(func.count()).select_from(Event).where(Event.project_id == key.project_id)
            )
            replays = await s.scalar(
                select(func.count()).select_from(ReplayRun).where(ReplayRun.project_id == key.project_id)
            )
            attempts = await s.scalar(
                select(func.count())
                .select_from(Attempt)
                .join(Delivery)
                .where(Delivery.project_id == key.project_id)
            )
            return {
                "project": project.name,
                "events": events,
                "deliveries": counts,
                "attempts": attempts,
                "replays": replays,
                "cursor": project.stream_seq,
                "mode": "demo",
                "transport_guarantee": "bounded at-least-once",
            }

    @app.get("/v1/metrics")
    async def metrics(request: Request):
        key = await authorize(request)
        async with db.session() as s:
            pending = await s.scalar(
                select(func.count())
                .select_from(Outbox)
                .join(Delivery)
                .where(Delivery.project_id == key.project_id, Outbox.published.is_(False))
            )
            counts = (
                await s.execute(
                    select(Delivery.state, func.count())
                    .where(Delivery.project_id == key.project_id)
                    .group_by(Delivery.state)
                )
            ).all()
            oldest = await s.scalar(
                select(func.min(Delivery.created_at)).where(
                    Delivery.project_id == key.project_id,
                    Delivery.state.in_(["ready", "retry_wait", "in_flight"]),
                )
            )
            ages = max(0, (now() - oldest).total_seconds()) if oldest else 0
        lines = [
            "# TYPE eventhub_outbox_pending gauge",
            f"eventhub_outbox_pending {pending}",
            "# TYPE eventhub_oldest_pending_seconds gauge",
            f"eventhub_oldest_pending_seconds {ages:.3f}",
            "# TYPE eventhub_deliveries gauge",
        ]
        lines += [f'eventhub_deliveries{{state="{state}"}} {count}' for state, count in counts]
        return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

    @app.post("/v1/cases/capture")
    async def capture_case(request: Request):
        key = await authorize(request, "cases:manage")
        if "events:read" not in key.scopes:
            raise HTTPException(403, "Event read scope required")
        body = await read_json(request, 2048)
        if not isinstance(body, dict) or set(body) != {"event_id", "pattern"}:
            raise HTTPException(422, "An event ID and prepared pattern are required")
        from eventhub.capture import capture

        definition = await capture(db, key.project_id, identifier(body["event_id"]), body["pattern"])
        row = await save_case(db, key, definition.model_dump(mode="json"))
        return {"id": str(row.id), "checksum": row.checksum}

    @app.post("/v1/events")
    async def post_event(request: Request):
        key = await authorize(request, "events:write")
        await rate_limit(db, "ingest:" + str(key.id), 60000)
        event = Envelope.model_validate(await read_json(request, config.max_event_bytes))
        with span("event.ingest", project_id=key.project_id):
            row, duplicate = await ingest(db, config, key, event)
        return JSONResponse(
            {"event_id": str(row.id), "duplicate": duplicate, "received_at": row.received_at.isoformat()},
            status_code=200 if duplicate else 202,
        )

    @app.get("/v1/events")
    async def events(request: Request, search: str = "", cursor: str = "", limit: int = 25):
        key = await authorize(request)
        if len(search) > 100 or not 1 <= limit <= 100 or len(cursor) > 2048:
            raise HTTPException(400, "Invalid query bounds")
        async with db.session() as s:
            query = select(Event).where(Event.project_id == key.project_id)
            if search:
                query = query.where(
                    or_(
                        Event.external_id.ilike(f"%{search}%"),
                        Event.subject.ilike(f"%{search}%"),
                        Event.source.ilike(f"%{search}%"),
                    )
                )
            if cursor:
                try:
                    raw, mac = cursor.rsplit(".", 1)
                    if not hmac.compare_digest(digest(config, raw), mac):
                        raise ValueError
                    values = json.loads(base64.urlsafe_b64decode(raw))
                    if values["project"] != str(key.project_id) or values["search"] != search:
                        raise ValueError
                    at, last = datetime.fromisoformat(values["at"]), identifier(values["id"])
                    query = query.where(
                        or_(Event.received_at < at, (Event.received_at == at) & (Event.id < last))
                    )
                except ValueError, KeyError, TypeError:
                    raise HTTPException(400, "Invalid cursor") from None
            rows = (
                await s.scalars(query.order_by(Event.received_at.desc(), Event.id.desc()).limit(limit + 1))
            ).all()
            next_cursor = None
            if len(rows) > limit:
                last = rows[limit - 1]
                raw = base64.urlsafe_b64encode(
                    canonical(
                        {
                            "project": str(key.project_id),
                            "search": search,
                            "at": last.received_at.isoformat(),
                            "id": str(last.id),
                        }
                    )
                ).decode()
                next_cursor = raw + "." + digest(config, raw)
            return {"items": [event_data(row) for row in rows[:limit]], "next_cursor": next_cursor}

    @app.get("/v1/events/{event_id}")
    async def get_event(event_id: str, request: Request):
        key = await authorize(request)
        async with db.session() as s:
            row = await s.scalar(
                select(Event).where(Event.id == identifier(event_id), Event.project_id == key.project_id)
            )
            if not row:
                raise HTTPException(404, "Event unavailable")
            return event_data(row)

    @app.get("/v1/events/{event_id}/deliveries")
    async def event_deliveries(event_id: str, request: Request):
        key = await authorize(request)
        async with db.session() as s:
            event = await s.scalar(
                select(Event).where(Event.id == identifier(event_id), Event.project_id == key.project_id)
            )
            if not event:
                raise HTTPException(404, "Event unavailable")
            rows = (await s.scalars(select(Delivery).where(Delivery.event_id == event.id))).all()
            result = []
            for row in rows:
                target = await s.get(Destination, row.destination_id)
                attempts = (
                    await s.scalars(
                        select(Attempt).where(Attempt.delivery_id == row.id).order_by(Attempt.started_at)
                    )
                ).all()
                result.append(
                    {
                        "id": str(row.id),
                        "destination": target.name,
                        "state": row.state,
                        "business_result": "not_verified",
                        "last_error": row.last_error,
                        "attempts": [
                            {
                                "number": a.number,
                                "run_id": str(a.run_id),
                                "outcome": a.outcome,
                                "status_code": a.status_code,
                                "elapsed_ms": a.elapsed_ms,
                            }
                            for a in attempts
                        ],
                    }
                )
            return {"items": result}

    @app.post("/v1/deliveries/{delivery_id}/retry")
    async def retry(delivery_id: str, request: Request):
        key = await authorize(request, "deliveries:retry")
        run = await retry_delivery(
            db, config, key, identifier(delivery_id), idempotency(request.headers.get("idempotency-key"))
        )
        return {"run_id": str(run)}

    @app.get("/v1/destinations")
    async def destinations(request: Request):
        key = await authorize(request)
        async with db.session() as s:
            rows = (
                await s.scalars(select(Destination).where(Destination.project_id == key.project_id))
            ).all()
            return {
                "items": [
                    {"id": str(r.id), "name": r.name, "adapter": r.adapter, "active": r.active} for r in rows
                ]
            }

    @app.post("/v1/destinations")
    async def post_destination(request: Request):
        key = await authorize(request, "destinations:manage")
        body = await read_json(request, 2048)
        if (
            not isinstance(body, dict)
            or set(body) != {"name", "adapter"}
            or not isinstance(body["name"], str)
            or not 1 <= len(body["name"]) <= 120
        ):
            raise HTTPException(422, "A name and registered adapter are required")
        row = await create_destination(db, config, key, body["name"], body["adapter"])
        return {"id": str(row.id)}

    @app.patch("/v1/destinations/{destination_id}")
    async def update_destination(destination_id: str, request: Request):
        key = await authorize(request, "destinations:manage")
        body = await read_json(request, 2048)
        if not isinstance(body, dict) or set(body) != {"active"} or type(body["active"]) is not bool:
            raise HTTPException(422, "A Boolean active flag is required")
        async with db.session() as s, s.begin():
            project = await lock_project(s, key.project_id)
            row = await s.scalar(
                select(Destination).where(
                    Destination.id == identifier(destination_id), Destination.project_id == project.id
                )
            )
            if not row:
                raise HTTPException(404, "Destination unavailable")
            row.active = body["active"]
            audit(s, project.id, "destination.updated", id=str(row.id), active=row.active)
            change(s, project, "destination", id=str(row.id))
        return {"ok": True}

    @app.get("/v1/cases")
    async def cases(request: Request):
        key = await authorize(request, "replays:read")
        async with db.session() as s:
            rows = (await s.scalars(select(ReplayCase).where(ReplayCase.project_id == key.project_id))).all()
            return {
                "items": [
                    {
                        "id": str(r.id),
                        "name": r.name,
                        "description": r.definition.get("description", ""),
                        "steps": len(r.definition["steps"]),
                        "checksum": r.checksum,
                    }
                    for r in rows
                ]
            }

    @app.post("/v1/cases/import")
    async def import_case(request: Request):
        key = await authorize(request, "cases:manage")
        value = await read_json(request, config.max_case_bytes)
        if isinstance(value, dict) and "case" in value:
            if set(value) != {"case", "checksum"} or checksum(value["case"]) != value["checksum"]:
                raise HTTPException(422, "Case checksum mismatch")
            value = value["case"]
        row = await save_case(db, key, value)
        return {"id": str(row.id), "checksum": row.checksum}

    @app.get("/v1/cases/{case_id}/export")
    async def export_case(case_id: str, request: Request):
        key = await authorize(request, "cases:export")
        async with db.session() as s, s.begin():
            row = await s.scalar(
                select(ReplayCase).where(
                    ReplayCase.id == identifier(case_id), ReplayCase.project_id == key.project_id
                )
            )
            if not row:
                raise HTTPException(404, "Case unavailable")
            audit(s, key.project_id, "case.exported", case_id=str(row.id))
            result = safe_export(row.definition)
        return JSONResponse(result, headers={"Content-Disposition": 'attachment; filename="incident.json"'})

    @app.post("/v1/replays")
    async def post_replay(request: Request):
        key = await authorize(request, "replays:run")
        body = await read_json(request, 2048)
        if not isinstance(body, dict) or set(body) != {"case_id"}:
            raise HTTPException(422, "case_id is required")
        row = await start_replay(
            db, key, identifier(body["case_id"]), idempotency(request.headers.get("idempotency-key"))
        )
        return JSONResponse({"id": str(row.id), "state": row.state}, status_code=202)

    @app.get("/v1/replays")
    async def replays(request: Request):
        key = await authorize(request, "replays:read")
        async with db.session() as s:
            rows = (
                await s.scalars(
                    select(ReplayRun)
                    .where(ReplayRun.project_id == key.project_id)
                    .order_by(ReplayRun.created_at.desc())
                    .limit(25)
                )
            ).all()
            return {"items": [run_data(r) for r in rows]}

    @app.get("/v1/replays/{run_id}/report")
    async def report(run_id: str, request: Request):
        key = await authorize(request, "replays:read")
        async with db.session() as s:
            row = await s.scalar(
                select(ReplayRun).where(
                    ReplayRun.id == identifier(run_id), ReplayRun.project_id == key.project_id
                )
            )
            if not row:
                raise HTTPException(404, "Run unavailable")
            return run_data(row)

    @app.post("/v1/replays/{run_id}/cancel")
    async def cancel(run_id: str, request: Request):
        key = await authorize(request, "replays:run")
        return run_data(await set_cancel(db, key, identifier(run_id)))

    @app.get("/v1/stream")
    async def stream(request: Request):
        key = await authorize(request)
        try:
            cursor = max(0, int(request.headers.get("last-event-id", "0")))
        except ValueError:
            raise HTTPException(400, "Invalid stream cursor") from None

        async def updates():
            nonlocal cursor
            yield ": connected\n\n"
            while not await request.is_disconnected():
                try:
                    await authorize(request)
                except HTTPException:
                    break
                async with db.session() as s:
                    project = await s.get(Project, key.project_id)
                    oldest = await s.scalar(
                        select(func.min(Change.seq)).where(Change.project_id == key.project_id)
                    )
                    if (
                        cursor > project.stream_seq
                        or (oldest is not None and cursor < oldest - 1)
                        or (oldest is None and cursor != project.stream_seq)
                    ):
                        cursor = project.stream_seq
                        yield f"id: {cursor}\nevent: reset\ndata: {{}}\n\n"
                    rows = (
                        await s.scalars(
                            select(Change)
                            .where(Change.project_id == key.project_id, Change.seq > cursor)
                            .order_by(Change.seq)
                            .limit(50)
                        )
                    ).all()
                for row in rows:
                    cursor = row.seq
                    yield f"id: {cursor}\ndata: {json.dumps(row.data)}\n\n"
                if not rows:
                    yield ": heartbeat\n\n"
                await asyncio.sleep(1)

        return StreamingResponse(
            updates(), media_type="text/event-stream", headers={"X-Accel-Buffering": "no"}
        )

    @app.get("/v1/keys")
    async def list_keys(request: Request):
        key = await authorize(request, "keys:manage")
        async with db.session() as s:
            rows = (await s.scalars(select(APIKey).where(APIKey.project_id == key.project_id))).all()
            return {
                "items": [
                    {
                        "id": str(r.id),
                        "prefix": r.prefix,
                        "role": r.role,
                        "scopes": r.scopes,
                        "revoked": r.revoked,
                    }
                    for r in rows
                ]
            }

    @app.post("/v1/keys")
    async def create_key(request: Request):
        owner = await authorize(request, "keys:manage")
        body = await read_json(request, 4096)
        if (
            owner.role != "owner"
            or not isinstance(body, dict)
            or set(body) - {"scopes", "sources", "role", "expires_in_days"}
            or not {"scopes", "sources", "role"} <= set(body)
        ):
            raise HTTPException(422, "Explicit key scopes, sources and role required")
        role, scopes, sources = body["role"], body["scopes"], body["sources"]
        if (
            not isinstance(role, str)
            or role not in {"source", "viewer", "operator"}
            or not isinstance(scopes, list)
            or not all(isinstance(v, str) and v in SCOPES - {"keys:manage"} for v in scopes)
            or not isinstance(sources, list)
            or len(sources) > 20
            or not all(isinstance(v, str) and 1 <= len(v) <= 200 for v in sources)
        ):
            raise HTTPException(422, "Invalid bounded key permissions")
        role_scopes = {
            "source": {"events:write"},
            "viewer": {"events:read", "replays:read", "cases:export"},
            "operator": SCOPES - {"keys:manage", "events:write"},
        }
        days = body.get("expires_in_days", 90)
        if not scopes or not set(scopes) <= role_scopes[role] or (role == "source" and not sources):
            raise HTTPException(422, "Permissions exceed the selected role")
        if type(days) is not int or not 1 <= days <= 365:
            raise HTTPException(422, "Key lifetime must be 1–365 days")
        raw, row = new_key(config, owner.project_id, scopes, role, sources)
        row.expires_at = now() + timedelta(days=days)
        async with db.session() as s, s.begin():
            await lock_project(s, owner.project_id)
            s.add(row)
            audit(s, owner.project_id, "key.created", role=role)
        return {"key": raw, "notice": "Shown once. Store privately."}

    @app.post("/v1/keys/{key_id}/revoke")
    async def revoke_key(key_id: str, request: Request):
        owner = await authorize(request, "keys:manage")
        async with db.session() as s, s.begin():
            row = await s.scalar(
                select(APIKey).where(APIKey.id == identifier(key_id), APIKey.project_id == owner.project_id)
            )
            if not row:
                raise HTTPException(404, "Key unavailable")
            row.revoked = True
            audit(s, owner.project_id, "key.revoked", key_id=str(row.id))
        return {"ok": True}

    return app
