import asyncio
from datetime import UTC, timedelta
from email.utils import parsedate_to_datetime
import json
import random
import time
import uuid

import aio_pika
import httpx
from opentelemetry import propagate, trace
from sqlalchemy import func, or_, select, text

from eventhub.contracts import canonical
from eventhub.db import now, uid
from eventhub.models import Attempt, Delivery, DeliveryRun, Destination, Event, Outbox
from eventhub.security import decrypt, signature
from eventhub.services import TERMINAL, change, lock_project
from eventhub.telemetry import log, span


def target_url(config, adapter):
    if adapter not in {"lab-a", "lab-b"}:
        raise ValueError("Unregistered destination")
    return getattr(config, f"lab_{adapter[-1]}_url").rstrip("/")


async def connect_broker(config):
    return await aio_pika.connect_robust(
        host=config.rabbitmq_host,
        port=config.rabbitmq_port,
        login=config.rabbitmq_user,
        password=config.rabbitmq_password.get_secret_value(),
        virtualhost=config.rabbitmq_vhost,
        timeout=5,
        reconnect_interval=2,
    )


async def topology(channel):
    exchange = await channel.declare_exchange("eventhub.dispatch", aio_pika.ExchangeType.DIRECT, durable=True)
    queue = await channel.declare_queue(
        "eventhub.delivery", durable=True, arguments={"x-queue-type": "quorum"}
    )
    await queue.bind(exchange, "delivery")
    await channel.declare_queue("eventhub.invalid", durable=True)
    return exchange, queue


async def publish_batch(db, config, exchange, limit=32, fault=None):
    sent = 0
    async with db.session() as s:
        candidates = (
            await s.scalars(
                select(Outbox.id)
                .where(
                    Outbox.published.is_(False), or_(Outbox.lease_until.is_(None), Outbox.lease_until < now())
                )
                .limit(limit * 4)
            )
        ).all()
    # Interleave projects before publishing, so one project's backlog does not monopolize the batch.
    async with db.session() as s:
        rows = (
            await s.execute(
                select(Outbox.id, Delivery.project_id)
                .join(Delivery, Outbox.delivery_id == Delivery.id)
                .where(Outbox.id.in_(candidates))
            )
        ).all()
    groups = {}
    for identifier, project_id in rows:
        groups.setdefault(project_id, []).append(identifier)
    ordered = []
    while groups and len(ordered) < limit:
        for project_id in list(groups):
            ordered.append(groups[project_id].pop(0))
            if not groups[project_id]:
                del groups[project_id]
            if len(ordered) == limit:
                break
    for identifier in ordered:
        lease = uid()
        async with db.session() as s, s.begin():
            row = await s.scalar(
                select(Outbox).where(Outbox.id == identifier).with_for_update(skip_locked=True)
            )
            if not row or row.published or (row.lease_until and row.lease_until > now()):
                continue
            row.lease_token, row.lease_until = lease, now() + timedelta(seconds=15)
            job = {
                "version": 1,
                "delivery_id": str(row.delivery_id),
                "run_id": str(row.run_id),
                "generation": row.generation,
            }
        with span("dispatch.publish", delivery_id=job["delivery_id"]):
            carrier = {}
            propagate.inject(carrier)
            job["trace"] = carrier
            await exchange.publish(
                aio_pika.Message(
                    canonical(job),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json",
                ),
                routing_key="delivery",
                mandatory=True,
            )
            if fault == "after_confirm":
                raise RuntimeError("Injected publisher interruption")
        async with db.session() as s, s.begin():
            row = await s.get(Outbox, identifier, with_for_update=True)
            if row.lease_token == lease:
                row.published, row.lease_token, row.lease_until = True, None, None
        sent += 1
    return sent


def retry_after(value, current=None):
    current = current or now()
    if not value:
        return 0
    try:
        seconds = int(value)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            seconds = int((parsed - current).total_seconds())
        except ValueError, TypeError, OverflowError:
            return 0
    return max(0, seconds)


def decide(status, error, attempts, config, retry_value=None, current=None):
    current = current or now()
    if status is not None and 200 <= status < 300:
        return "accepted", None, ""
    transient = (
        error in {"timeout", "network", "lease_expired"} or status in {408, 429} or (status and status >= 500)
    )
    if not transient:
        return "exhausted", None, error or f"http_{status}"
    if attempts >= config.max_attempts:
        return "exhausted", None, "attempt_budget"
    delay = max(
        retry_after(retry_value, current), min(300, 2**attempts) + random.SystemRandom().uniform(0, 0.5)
    )
    return "retry_wait", current + timedelta(seconds=delay), error or f"http_{status}"


async def claim(db, config, job):
    identifier = uuid.UUID(job["delivery_id"])
    async with db.session() as s, s.begin():
        # Short global allocation lock makes concurrency limits apply across worker processes.
        await s.execute(text("SELECT pg_advisory_xact_lock(807314)"))
        initial = await s.get(Delivery, identifier)
        if not initial:
            return None
        project = await lock_project(s, initial.project_id)
        row = await s.get(Delivery, identifier, with_for_update=True)
        if (
            row.state in TERMINAL
            or str(row.current_run_id) != job["run_id"]
            or row.generation != job["generation"]
        ):
            return None
        if row.state == "in_flight" or row.state != "ready":
            return None
        target = await s.get(Destination, row.destination_id)
        if not target.active or row.expires_at <= now():
            row.state, row.last_error = (
                ("cancelled", "destination_disabled") if not target.active else ("exhausted", "ttl")
            )
            await finish_run(s, row)
            change(s, project, "delivery", id=str(row.id))
            return None
        active = select(Delivery).where(Delivery.state == "in_flight", Delivery.lease_until > now())
        total = await s.scalar(select(func.count()).select_from(active.subquery()))
        project_count = await s.scalar(
            select(func.count()).select_from(active.where(Delivery.project_id == row.project_id).subquery())
        )
        destination_count = await s.scalar(
            select(func.count()).select_from(active.where(Delivery.destination_id == target.id).subquery())
        )
        if (
            total >= config.global_concurrency
            or project_count >= config.project_concurrency
            or destination_count >= config.destination_concurrency
        ):
            row.state, row.next_at = "retry_wait", now() + timedelta(seconds=1)
            return None
        row.state, row.lease_token = "in_flight", uid()
        row.lease_until = now() + timedelta(seconds=config.lease_seconds)
        row.attempt_count += 1
        attempt_id = uid()
        s.add(Attempt(id=attempt_id, delivery_id=row.id, run_id=row.current_run_id, number=row.attempt_count))
        event = await s.get(Event, row.event_id)
        change(s, project, "delivery", id=str(row.id))
        return {
            "id": row.id,
            "run_id": row.current_run_id,
            "token": row.lease_token,
            "attempt_id": attempt_id,
            "body": canonical(event.payload),
            "project_id": row.project_id,
            "destination_id": target.id,
            "url": target_url(config, target.adapter) + "/webhook",
            "secret": decrypt(config, target.secret),
        }


async def finish_run(s, row):
    run = await s.get(DeliveryRun, row.current_run_id)
    if run:
        run.finished_at, run.outcome = now(), row.state


async def complete(db, config, work, status, error, elapsed_ms, retry_value=None):
    async with db.session() as s, s.begin():
        project = await lock_project(s, work["project_id"])
        row = await s.get(Delivery, work["id"], with_for_update=True)
        if (
            row.current_run_id != work["run_id"]
            or row.lease_token != work["token"]
            or row.state != "in_flight"
        ):
            return False
        if row.lease_until <= now():
            return False
        attempt = await s.get(Attempt, work["attempt_id"])
        if attempt.outcome != "started":
            return False
        state, next_at, reason = decide(status, error, row.attempt_count, config, retry_value)
        if next_at and next_at >= row.expires_at:
            state, next_at, reason = "exhausted", None, "ttl"
        attempt.finished_at, attempt.status_code, attempt.elapsed_ms = now(), status, elapsed_ms
        attempt.outcome = "accepted" if state == "accepted" else (error or f"http_{status}")
        row.state, row.last_error = state, reason
        row.lease_token = row.lease_until = None
        if next_at:
            row.next_at = next_at
        if state in TERMINAL:
            await finish_run(s, row)
        change(s, project, "delivery", id=str(row.id))
        return True


async def deliver_job(db, config, job, client, fault=None):
    work = await claim(db, config, job)
    if not work:
        return
    if fault == "before_http":
        raise RuntimeError("Injected interruption")
    headers = signature(work["secret"], str(work["id"]), work["body"])
    headers.update(
        {
            "Content-Type": "application/cloudevents+json",
            "x-eventhub-project": str(work["project_id"]),
            "x-eventhub-destination": str(work["destination_id"]),
        }
    )
    status, error, retry_value = None, "", None
    started = time.monotonic()
    context = propagate.extract(job.get("trace", {}))
    with trace.get_tracer("eventhub").start_as_current_span("delivery.http", context=context):
        propagate.inject(headers)
        try:
            async with asyncio.timeout(config.http_timeout_seconds):
                async with client.stream(
                    "POST", work["url"], content=work["body"], headers=headers
                ) as response:
                    status, retry_value = response.status_code, response.headers.get("retry-after")
                    read = 0
                    async for chunk in response.aiter_bytes():
                        read += len(chunk)
                        if read > 65536:
                            break
        except httpx.TimeoutException, TimeoutError:
            status, error = None, "timeout"
        except httpx.HTTPError:
            status, error = None, "network"
    if fault == "after_http":
        raise RuntimeError("Injected interruption")
    await complete(db, config, work, status, error, round((time.monotonic() - started) * 1000), retry_value)


async def sweep(db, config, limit=100):
    current, count = now(), 0
    async with db.session() as s:
        ids = (
            await s.scalars(
                select(Delivery.id)
                .where(
                    or_(
                        (Delivery.state == "retry_wait") & (Delivery.next_at <= current),
                        (Delivery.state == "in_flight") & (Delivery.lease_until <= current),
                        (Delivery.state == "ready")
                        & (
                            (Delivery.expires_at <= current)
                            | (Delivery.next_at <= current - timedelta(seconds=30))
                        ),
                    )
                )
                .limit(limit)
            )
        ).all()
    for identifier in ids:
        async with db.session() as s, s.begin():
            initial = await s.get(Delivery, identifier)
            if not initial:
                continue
            project = await lock_project(s, initial.project_id)
            row = await s.get(Delivery, identifier, with_for_update=True)
            if row.state == "in_flight" and row.lease_until <= current:
                attempt = await s.scalar(
                    select(Attempt).where(
                        Attempt.run_id == row.current_run_id, Attempt.number == row.attempt_count
                    )
                )
                if attempt and attempt.outcome == "started":
                    attempt.outcome, attempt.finished_at = "unknown", current
                row.lease_token = row.lease_until = None
                row.state, row.next_at = "retry_wait", current
                row.last_error = "lease_expired"
            if row.state not in {"ready", "retry_wait"} or row.next_at > current:
                continue
            if row.expires_at <= current or row.attempt_count >= config.max_attempts:
                row.state, row.last_error = "exhausted", "budget_expired"
                await finish_run(s, row)
            else:
                row.state = "ready"
                row.next_at = current
                row.generation += 1
                s.add(Outbox(delivery_id=row.id, run_id=row.current_run_id, generation=row.generation))
            change(s, project, "delivery", id=str(row.id))
            count += 1
    return count


async def serve_publisher(db, config):
    while True:
        try:
            async with await connect_broker(config) as connection:
                channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
                exchange, _ = await topology(channel)
                while True:
                    await publish_batch(db, config, exchange)
                    await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            raise
        except Exception:
            log("publisher.unavailable")
            await asyncio.sleep(2)


async def serve_worker(db, config):
    semaphore = asyncio.Semaphore(config.global_concurrency)
    while True:
        try:
            async with (
                await connect_broker(config) as connection,
                httpx.AsyncClient(
                    timeout=config.http_timeout_seconds, follow_redirects=False, trust_env=False
                ) as client,
            ):
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=config.global_concurrency)
                _, queue = await topology(channel)
                invalid = await channel.get_queue("eventhub.invalid")

                async def handle(message):
                    async with semaphore:
                        try:
                            if len(message.body) > 4096:
                                raise ValueError
                            job = json.loads(message.body)
                            if job.get("version") != 1 or type(job.get("generation")) is not int:
                                raise ValueError
                            uuid.UUID(job["delivery_id"])
                            uuid.UUID(job["run_id"])
                        except ValueError, KeyError, TypeError, AttributeError:
                            await channel.default_exchange.publish(
                                aio_pika.Message(b"invalid job schema"), invalid.name
                            )
                            await message.ack()
                            return
                        try:
                            await deliver_job(db, config, job, client)
                            await message.ack()
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            log("worker.job_deferred")
                            await asyncio.sleep(2)
                            await message.reject(requeue=True)

                await queue.consume(handle)
                await asyncio.Future()
        except asyncio.CancelledError:
            raise
        except Exception:
            log("worker.unavailable")
            await asyncio.sleep(2)
