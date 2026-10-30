import asyncio
from datetime import timedelta
import json

import httpx
import pytest
from sqlalchemy import func, select

from eventhub.contracts import Envelope
from eventhub.db import now
from eventhub.models import Attempt, Delivery, Event, Outbox
from eventhub.services import create_destination, ingest
from eventhub.worker import claim, complete, connect_broker, deliver_job, publish_batch, sweep, topology


async def prepared(actor, case):
    await create_destination(actor.db, actor.config, actor.key, "Test B", "lab-b")
    event, _ = await ingest(actor.db, actor.config, actor.key, Envelope.model_validate(case["events"][0]))
    async with actor.db.session() as s:
        row = await s.scalar(select(Delivery).where(Delivery.event_id == event.id))
    return {
        "version": 1,
        "delivery_id": str(row.id),
        "run_id": str(row.current_run_id),
        "generation": row.generation,
    }


async def test_claim_concurrency_and_fencing(actor, case):
    job = await prepared(actor, case)
    works = await asyncio.gather(claim(actor.db, actor.config, job), claim(actor.db, actor.config, job))
    assert sum(w is not None for w in works) == 1
    work = next(w for w in works if w is not None)
    async with actor.db.session() as s, s.begin():
        row = await s.get(Delivery, work["id"])
        row.lease_until = now() - timedelta(seconds=1)
    assert await complete(actor.db, actor.config, work, 200, "", 1) is False
    await sweep(actor.db, actor.config)
    async with actor.db.session() as s:
        row = await s.get(Delivery, work["id"])
        new_job = {
            "version": 1,
            "delivery_id": str(row.id),
            "run_id": str(row.current_run_id),
            "generation": row.generation,
        }
    new_work = await claim(actor.db, actor.config, new_job)
    assert new_work is not None
    assert await complete(actor.db, actor.config, new_work, 200, "", 1)
    assert await complete(actor.db, actor.config, work, 500, "network", 1) is False


async def test_commit_then_lost_worker_result_does_not_duplicate_b_effect(actor, case):
    job = await prepared(actor, case)
    async with httpx.AsyncClient(trust_env=False, timeout=3) as client:
        with pytest.raises(RuntimeError):
            await deliver_job(actor.db, actor.config, job, client, fault="after_http")
        async with actor.db.session() as s, s.begin():
            row = await s.get(Delivery, __import__("uuid").UUID(job["delivery_id"]))
            namespace = f"live:{row.project_id}:{row.destination_id}"
            row.lease_until = now() - timedelta(seconds=1)
        await sweep(actor.db, actor.config)
        async with actor.db.session() as s:
            row = await s.get(Delivery, __import__("uuid").UUID(job["delivery_id"]))
            job["generation"] = row.generation
        await deliver_job(actor.db, actor.config, job, client)
        response = await client.get(
            actor.config.lab_b_url + "/control/state/" + namespace,
            headers={"Authorization": "Bearer " + actor.config.lab_b_token.get_secret_value()},
        )
        assert response.status_code == 200 and len(response.json()["effects"]) == 1
        async with actor.db.session() as s:
            row = await s.get(Delivery, __import__("uuid").UUID(job["delivery_id"]))
            assert row.state == "accepted"


async def test_real_broker_confirm_interruption_and_redelivery(actor, case):
    job = await prepared(actor, case)
    async with await connect_broker(actor.config) as connection:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        exchange, queue = await topology(channel)
        await queue.purge()
        with pytest.raises(RuntimeError):
            await publish_batch(actor.db, actor.config, exchange, fault="after_confirm")
        async with actor.db.session() as s, s.begin():
            box = await s.scalar(
                select(Outbox).where(Outbox.delivery_id == __import__("uuid").UUID(job["delivery_id"]))
            )
            box.lease_until = now() - timedelta(seconds=1)
        assert await publish_batch(actor.db, actor.config, exchange) == 1
        messages = []
        for _ in range(2):
            message = await queue.get(timeout=3)
            messages.append(json.loads(message.body))
            await message.ack()
        assert all(m["delivery_id"] == job["delivery_id"] for m in messages)
        async with httpx.AsyncClient(trust_env=False, timeout=3) as client:
            for message in messages:
                await deliver_job(actor.db, actor.config, message, client)
        async with actor.db.session() as s:
            count = await s.scalar(
                select(func.count())
                .select_from(Attempt)
                .where(Attempt.delivery_id == __import__("uuid").UUID(job["delivery_id"]))
            )
            assert count == 1
        await queue.purge()


async def test_outbox_and_event_roll_back_together(actor, case, monkeypatch):
    await create_destination(actor.db, actor.config, actor.key, "Test B", "lab-b")

    def broken(**values):
        raise RuntimeError("Injected outbox storage failure")

    monkeypatch.setattr("eventhub.services.Outbox", broken)
    with pytest.raises(RuntimeError):
        await ingest(actor.db, actor.config, actor.key, Envelope.model_validate(case["events"][0]))
    async with actor.db.session() as s:
        assert (
            await s.scalar(
                select(func.count()).select_from(Event).where(Event.project_id == actor.key.project_id)
            )
            == 0
        )


async def test_quotas_defer_without_consuming_attempt(actor, case):
    job = await prepared(actor, case)
    first = await claim(actor.db, actor.config, job)
    assert first is not None
    another = case["events"][1]
    event, _ = await ingest(actor.db, actor.config, actor.key, Envelope.model_validate(another))
    async with actor.db.session() as s:
        row = await s.scalar(select(Delivery).where(Delivery.event_id == event.id))
    config = actor.config.model_copy(update={"global_concurrency": 1})
    value = await claim(
        actor.db,
        config,
        {"delivery_id": str(row.id), "run_id": str(row.current_run_id), "generation": row.generation},
    )
    assert value is None
    async with actor.db.session() as s:
        deferred = await s.get(Delivery, row.id)
        assert deferred.state == "retry_wait" and deferred.attempt_count == 0


async def test_manual_run_preserves_event_identity_and_prior_history(actor, case):
    from eventhub.services import retry_delivery
    from eventhub.models import Attempt, DeliveryRun
    from uuid import UUID

    job = await prepared(actor, case)
    async with httpx.AsyncClient(trust_env=False, timeout=3) as client:
        await deliver_job(actor.db, actor.config, job, client)
        new_run = await retry_delivery(
            actor.db, actor.config, actor.key, UUID(job["delivery_id"]), "manual-one"
        )
        assert (
            await retry_delivery(actor.db, actor.config, actor.key, UUID(job["delivery_id"]), "manual-one")
            == new_run
        )
        async with actor.db.session() as s:
            row = await s.get(Delivery, UUID(job["delivery_id"]))
            event_id = row.event_id
            second = {
                "version": 1,
                "delivery_id": str(row.id),
                "run_id": str(new_run),
                "generation": row.generation,
            }
        await deliver_job(actor.db, actor.config, second, client)
    async with actor.db.session() as s:
        runs = (
            await s.scalars(select(DeliveryRun).where(DeliveryRun.delivery_id == UUID(job["delivery_id"])))
        ).all()
        attempts = (
            await s.scalars(select(Attempt).where(Attempt.delivery_id == UUID(job["delivery_id"])))
        ).all()
        assert len(runs) == 2 and len(attempts) == 2
        assert {a.run_id for a in attempts} == {UUID(job["run_id"]), new_run}
        assert (await s.get(Delivery, UUID(job["delivery_id"]))).event_id == event_id


async def test_broker_redelivery_after_committed_result_before_ack(actor, case):
    from uuid import UUID

    job = await prepared(actor, case)
    async with await connect_broker(actor.config) as connection:
        first = await connection.channel(publisher_confirms=True)
        exchange, queue = await topology(first)
        await queue.purge()
        await publish_batch(actor.db, actor.config, exchange)
        message = await queue.get(timeout=3)
        async with httpx.AsyncClient(trust_env=False, timeout=3) as client:
            await deliver_job(actor.db, actor.config, json.loads(message.body), client)
            # Real AMQP channel loss returns the unacknowledged message to the broker.
            await first.close()
            second = await connection.channel()
            _, queue2 = await topology(second)
            redelivered = await queue2.get(timeout=3)
            assert redelivered.redelivered
            await deliver_job(actor.db, actor.config, json.loads(redelivered.body), client)
            await redelivered.ack()
        async with actor.db.session() as s:
            assert (
                await s.scalar(
                    select(func.count())
                    .select_from(Attempt)
                    .where(Attempt.delivery_id == UUID(job["delivery_id"]))
                )
                == 1
            )
        await queue2.purge()
