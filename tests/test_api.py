import asyncio

import httpx
from sqlalchemy import func, select

from eventhub.app import create_app
from eventhub.models import APIKey, Event


async def test_ingest_retry_conflict_and_concurrency(client, case, actor):
    event = case["events"][0]
    results = await asyncio.gather(*[client.post("/v1/events", json=event) for _ in range(5)])
    assert sorted(r.status_code for r in results) == [200, 200, 200, 200, 202]
    assert len({r.json()["event_id"] for r in results}) == 1
    event["data"]["status"] = "cancelled"
    assert (await client.post("/v1/events", json=event)).status_code == 409
    async with actor.db.session() as s:
        assert (
            await s.scalar(
                select(func.count()).select_from(Event).where(Event.project_id == actor.key.project_id)
            )
            == 1
        )


async def test_foreign_event_hidden(client, case, actor):
    from eventhub.db import uid
    from eventhub.models import Project
    from eventhub.security import SCOPES, new_key

    first = (await client.post("/v1/events", json=case["events"][0])).json()["event_id"]
    other_id = uid()
    raw, key = new_key(actor.config, other_id, SCOPES, "owner", ["*"])
    async with actor.db.session() as s, s.begin():
        s.add(Project(id=other_id, name="Other test workspace"))
        await s.flush()
        s.add(key)
    r = await client.get("/v1/events/" + first, headers={"Authorization": "Bearer " + raw})
    assert r.status_code == 404
    from sqlalchemy import delete

    async with actor.db.session() as s, s.begin():
        await s.execute(delete(APIKey).where(APIKey.project_id == other_id))
        await s.execute(delete(Project).where(Project.id == other_id))


async def test_unregistered_destination_and_payload_limits(client):
    assert (
        await client.post("/v1/destinations", json={"name": "Bad", "adapter": "http://169.254.169.254/"})
    ).status_code == 422
    assert (
        await client.post(
            "/v1/destinations", json={"name": "Bad", "adapter": "lab-b", "url": "http://localhost/"}
        )
    ).status_code == 422
    assert (await client.post("/v1/events", content=b"x" * 70000)).status_code == 413
    assert (await client.post("/v1/events", content=b'{"a":1,"a":2}')).status_code == 400


async def test_export_strips_free_text_and_import_verifies_hash(client, case):
    case["description"] = "sensitive-marker"
    case["events"][0]["correlationid"] = "sensitive-marker"
    case_id = (await client.post("/v1/cases/import", json=case)).json()["id"]
    result = await client.get("/v1/cases/" + case_id + "/export")
    assert result.status_code == 200 and "sensitive-marker" not in result.text
    assert (await client.post("/v1/cases/import", json=result.json())).status_code == 200
    data = result.json()
    data["case"]["seed"] += 1
    assert (await client.post("/v1/cases/import", json=data)).status_code == 422


async def test_replay_request_key_conflict(client, case):
    case_id = (await client.post("/v1/cases/import", json=case)).json()["id"]
    headers = {"Idempotency-Key": "controlled-key"}
    a = await client.post("/v1/replays", json={"case_id": case_id}, headers=headers)
    b = await client.post("/v1/replays", json={"case_id": case_id}, headers=headers)
    assert a.status_code == 202 and a.json()["id"] == b.json()["id"]
    case["seed"] += 1
    other = (await client.post("/v1/cases/import", json=case)).json()["id"]
    assert (await client.post("/v1/replays", json={"case_id": other}, headers=headers)).status_code == 409
    assert (await client.post("/v1/replays", json={"case_id": case_id})).status_code == 400


async def test_cookie_login_csrf_revocation(actor):
    app = create_app(actor.config)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as browser:
        page = await browser.get("/login")
        import re

        csrf = re.search(r'name="csrf-token" content="([^"]+)"', page.text)[1]
        assert (await browser.post("/session", json={"key": actor.raw})).status_code == 403
        result = await browser.post(
            "/session", json={"key": actor.raw}, headers={"Origin": "http://testserver", "X-CSRF-Token": csrf}
        )
        assert result.status_code == 200 and "HttpOnly" in result.headers["set-cookie"]
        assert (await browser.get("/v1/overview")).status_code == 200
        assert (await browser.post("/v1/cases/import", json={})).status_code == 403
        async with actor.db.session() as s, s.begin():
            key = await s.get(APIKey, actor.key.id)
            key.revoked = True
        assert (await browser.get("/v1/overview")).status_code == 401
    await app.state.db.close()


async def test_no_auth_and_security_headers(actor):
    app = create_app(actor.config)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        r = await client.get("/v1/overview")
        assert r.status_code == 401 and r.headers["cache-control"] == "no-store"
        assert "frame-ancestors" in r.headers["content-security-policy"]
    await app.state.db.close()


async def test_roles_cap_scopes_and_key_expiration(client, actor, case):
    assert (
        await client.post("/v1/keys", json={"role": "viewer", "scopes": ["events:write"], "sources": ["*"]})
    ).status_code == 422
    result = await client.post(
        "/v1/keys",
        json={
            "role": "source",
            "scopes": ["events:write"],
            "sources": [case["events"][0]["source"]],
            "expires_in_days": 1,
        },
    )
    assert result.status_code == 200
    raw = result.json()["key"]
    headers = {"Authorization": "Bearer " + raw}
    assert (await client.post("/v1/events", json=case["events"][0], headers=headers)).status_code == 202
    assert (await client.get("/v1/overview", headers=headers)).status_code == 403
    from datetime import timedelta
    from eventhub.db import now

    async with actor.db.session() as s, s.begin():
        row = await s.scalar(select(APIKey).where(APIKey.prefix == raw[:20]))
        row.expires_at = now() - timedelta(seconds=1)
    assert (await client.post("/v1/events", json=case["events"][0], headers=headers)).status_code == 401


async def test_capture_retained_events_and_scoped_metrics(client, case):
    ids = []
    for event in case["events"]:
        ids.append((await client.post("/v1/events", json=event)).json()["event_id"])
    result = await client.post("/v1/cases/capture", json={"event_id": ids[0], "pattern": "reorder_duplicate"})
    assert result.status_code == 200
    exported = await client.get("/v1/cases/" + result.json()["id"] + "/export")
    definition = exported.json()["case"]
    assert definition["expected_variants"] == {"A": "fail", "B": "pass"}
    assert definition["expected"] == {"revision": 3, "status": "cancelled"}
    assert len(definition["steps"]) == 4
    assert "urn:demo:haircut-studio" not in exported.text
    response = await client.get("/v1/metrics")
    assert response.status_code == 200
    assert "eventhub_outbox_pending" in response.text and "Bearer" not in response.text


async def test_wrong_json_types_are_rejected_without_server_errors(client, case):
    assert (
        await client.post("/v1/keys", json={"role": [], "scopes": ["events:write"], "sources": ["*"]})
    ).status_code == 422
    assert (
        await client.post("/v1/destinations", json={"name": "wrong type", "adapter": {}})
    ).status_code == 422
    event_id = (await client.post("/v1/events", json=case["events"][0])).json()["event_id"]
    assert (
        await client.post("/v1/cases/capture", json={"event_id": event_id, "pattern": []})
    ).status_code == 422
