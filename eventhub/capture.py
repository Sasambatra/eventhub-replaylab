"""Build a sanitized, executable incident from one retained aggregate."""

from fastapi import HTTPException
from sqlalchemy import select

from eventhub.contracts import CaseDefinition
from eventhub.models import Event


async def capture(db, project_id, event_id, pattern):
    if not isinstance(pattern, str) or pattern not in {"arrival", "reorder_duplicate"}:
        raise HTTPException(422, "Unknown prepared incident pattern")
    async with db.session() as s:
        anchor = await s.scalar(select(Event).where(Event.id == event_id, Event.project_id == project_id))
        if not anchor:
            raise HTTPException(404, "Event unavailable")
        rows = (
            await s.scalars(
                select(Event)
                .where(
                    Event.project_id == project_id,
                    Event.source == anchor.source,
                    Event.subject == anchor.subject,
                )
                .order_by(Event.received_at, Event.id)
                .limit(501)
            )
        ).all()
    if len(rows) > 500:
        raise HTTPException(413, "Aggregate exceeds the first case profile")
    if any(row.payload is None for row in rows):
        raise HTTPException(410, "An aggregate payload has expired")
    events, revisions = [], {}
    for index, row in enumerate(rows, 1):
        value = row.payload["data"]
        if value["revision"] in revisions and revisions[value["revision"]] != value["status"]:
            raise HTTPException(409, "Conflicting snapshot revisions need investigation")
        revisions[value["revision"]] = value["status"]
        events.append(
            {
                "specversion": "1.0",
                "id": f"event-{index:03}",
                "source": "urn:demo:capture",
                "type": row.event_type,
                "subject": "booking/00000000-0000-4000-8000-000000000001",
                "time": rows[0].payload["time"],
                "data": {
                    "booking_id": "00000000-0000-4000-8000-000000000001",
                    "revision": value["revision"],
                    "status": value["status"],
                },
            }
        )
    order = list(events)
    if pattern == "reorder_duplicate":
        order = sorted(events, key=lambda e: e["data"]["revision"], reverse=True)
        order.append(order[-1])
    latest = max(revisions)
    a_pass = (
        order[-1]["data"]["revision"] == latest
        and order[-1]["data"]["status"] == revisions[latest]
        and len(order) == len({e["id"] for e in order})
    )
    return CaseDefinition.model_validate(
        {
            "case_version": 1,
            "name": "Captured aggregate: " + pattern,
            "description": "Sanitized snapshots; prepared " + pattern + " schedule.",
            "seed": 42,
            "start_time": events[0]["time"],
            "events": events,
            "steps": [{"event_id": e["id"]} for e in order],
            "expected": {"status": revisions[latest], "revision": latest},
            "expected_variants": {"A": "pass" if a_pass else "fail", "B": "pass"},
        }
    )
