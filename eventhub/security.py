import base64
import hashlib
import hmac
import secrets
import time
from datetime import timedelta

from cryptography.fernet import Fernet
from fastapi import HTTPException, Request
from sqlalchemy import select

from eventhub.db import now
from eventhub.models import APIKey, BrowserSession, Project

SCOPES = {
    "events:write",
    "events:read",
    "destinations:manage",
    "deliveries:retry",
    "replays:run",
    "replays:read",
    "cases:manage",
    "cases:export",
    "keys:manage",
}


def digest(config, value):
    return hmac.new(
        config.key_hash_pepper.get_secret_value().encode(), value.encode(), hashlib.sha256
    ).hexdigest()


def new_key(config, project_id, scopes, role="source", sources=None):
    raw = "eh_" + secrets.token_urlsafe(40)
    return raw, APIKey(
        project_id=project_id,
        prefix=raw[:20],
        digest=digest(config, raw),
        scopes=sorted(scopes),
        role=role,
        sources=sources or [],
    )


async def find_key(session, config, raw):
    key = await session.scalar(select(APIKey).where(APIKey.prefix == raw[:20]))
    if key is None or key.revoked or (key.expires_at and key.expires_at <= now()):
        return None
    if not hmac.compare_digest(key.digest, digest(config, raw)):
        return None
    project = await session.get(Project, key.project_id)
    return key if project and project.active else None


def check_origin(request):
    origin = request.headers.get("origin")
    expected = str(request.base_url).rstrip("/")
    if origin != expected:
        raise HTTPException(403, "Same-origin request required")


async def authorize(request: Request, scope="events:read"):
    config, db = request.app.state.config, request.app.state.db
    async with db.session() as session:
        authorization = request.headers.get("authorization", "")
        if authorization.startswith("Bearer "):
            key = await find_key(session, config, authorization[7:])
        else:
            raw = request.cookies.get("eh_session", "")
            browser = await session.get(BrowserSession, digest(config, raw)) if raw else None
            key = (
                await session.get(APIKey, browser.key_id) if browser and browser.expires_at > now() else None
            )
            if key and (key.revoked or (key.expires_at and key.expires_at <= now())):
                key = None
            if key and request.method not in {"GET", "HEAD", "OPTIONS"}:
                check_origin(request)
                if not hmac.compare_digest(request.headers.get("x-csrf-token", ""), browser.csrf):
                    raise HTTPException(403, "CSRF verification failed")
            if key:
                project = await session.get(Project, key.project_id)
                if not project or not project.active:
                    key = None
        if not key:
            raise HTTPException(401, "Valid access required")
        if scope not in key.scopes:
            raise HTTPException(403, "Insufficient scope")
        return key


async def make_session(session, config, key):
    raw, csrf = secrets.token_urlsafe(40), secrets.token_urlsafe(32)
    session.add(
        BrowserSession(
            digest=digest(config, raw),
            key_id=key.id,
            csrf=csrf,
            expires_at=now() + timedelta(seconds=config.session_seconds),
        )
    )
    return raw, csrf


def encrypt(config, value):
    return Fernet(config.secret_encryption_key.get_secret_value().encode()).encrypt(value).decode()


def decrypt(config, value):
    return Fernet(config.secret_encryption_key.get_secret_value().encode()).decrypt(value.encode())


def lab_secret(config, variant):
    value = getattr(config, f"lab_{variant.lower()}_token").get_secret_value()
    return hashlib.sha256(value.encode()).digest()


def target_secret(token, project_id, destination_id):
    return hmac.new(
        token.encode(), f"destination:{project_id}:{destination_id}".encode(), hashlib.sha256
    ).digest()


def signature(secret, identifier, body, timestamp=None):
    timestamp = int(time.time()) if timestamp is None else timestamp
    signed = f"{identifier}.{timestamp}.".encode() + body
    encoded = base64.b64encode(hmac.new(secret, signed, hashlib.sha256).digest()).decode()
    return {
        "webhook-id": identifier,
        "webhook-timestamp": str(timestamp),
        "webhook-signature": "v1," + encoded,
    }


def verify_signature(secret, headers, body, current_time=None):
    try:
        timestamp = int(headers.get("webhook-timestamp", ""))
        identifier = headers.get("webhook-id", "")
        value = headers.get("webhook-signature", "")
        if not identifier or len(identifier) > 128 or len(value) > 512:
            return False
        if abs((time.time() if current_time is None else current_time) - timestamp) > 300:
            return False
        expected = signature(secret, identifier, body, timestamp)["webhook-signature"]
        return any(hmac.compare_digest(part, expected) for part in value.split(" "))
    except ValueError, TypeError:
        return False
