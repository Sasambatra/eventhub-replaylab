from datetime import timedelta
from uuid import UUID

from sqlalchemy import select

from eventhub.db import now
from eventhub.models import Delivery, Event
from eventhub.retention import cleanup


async def test_payload_retention_preserves_dedup_and_prevents_retry(client, actor, case):
    event = case["events"][0]
    await client.post("/v1/destinations", json={"name": "Retention target", "adapter": "lab-b"})
    result = await client.post("/v1/events", json=event)
    event_id = UUID(result.json()["event_id"])
    async with actor.db.session() as s, s.begin():
        row = await s.get(Event, event_id)
        row.received_at = now() - timedelta(days=8)
        delivery = await s.scalar(select(Delivery).where(Delivery.event_id == event_id))
        delivery_id = str(delivery.id)
    await cleanup(actor.db, actor.config)
    async with actor.db.session() as s:
        assert (await s.get(Event, event_id)).payload is not None  # active delivery protected
    async with actor.db.session() as s, s.begin():
        delivery = await s.get(Delivery, UUID(delivery_id))
        delivery.state = "accepted"
    counts = await cleanup(actor.db, actor.config)
    assert counts["payloads"] == 1
    assert (await client.post("/v1/events", json=event)).status_code == 200
    event["data"]["status"] = "expired"
    assert (await client.post("/v1/events", json=event)).status_code == 409
    assert (
        await client.post(
            "/v1/deliveries/" + delivery_id + "/retry", headers={"Idempotency-Key": "expired-payload"}
        )
    ).status_code == 410
