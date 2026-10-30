"""Bound stored payload/history without losing event identity or active work."""

from datetime import timedelta

import httpx
from sqlalchemy import delete, select

from eventhub.db import now
from eventhub.models import (
    Attempt,
    Audit,
    BrowserSession,
    Change,
    Delivery,
    Event,
    Outbox,
    Project,
    ReplayRun,
    RequestLimit,
)
from eventhub.services import TERMINAL, audit, lock_project


async def cleanup(db, config, *, current=None):
    current = current or now()
    payload_cutoff = current - timedelta(days=config.event_retention_days)
    history_cutoff = current - timedelta(days=config.history_retention_days)
    counts = {"payloads": 0, "attempts": 0, "changes": 0, "lab_runs": 0}
    async with db.session() as s:
        projects = (await s.scalars(select(Project.id))).all()
    for project_id in projects:
        async with db.session() as s, s.begin():
            await lock_project(s, project_id)
            active = select(Delivery.event_id).where(
                Delivery.project_id == project_id, Delivery.state.not_in(TERMINAL)
            )
            rows = (
                await s.scalars(
                    select(Event).where(
                        Event.project_id == project_id,
                        Event.received_at < payload_cutoff,
                        Event.payload_expired_at.is_(None),
                        Event.id.not_in(active),
                    )
                )
            ).all()
            for row in rows:
                row.payload, row.payload_expired_at = None, current
            counts["payloads"] += len(rows)
            finished = select(Delivery.id).where(
                Delivery.project_id == project_id,
                Delivery.state.in_(TERMINAL),
                Delivery.created_at < history_cutoff,
            )
            result = await s.execute(delete(Attempt).where(Attempt.delivery_id.in_(finished)))
            counts["attempts"] += result.rowcount
            await s.execute(
                delete(Outbox).where(Outbox.delivery_id.in_(finished), Outbox.published.is_(True))
            )
            result = await s.execute(
                delete(Change).where(Change.project_id == project_id, Change.created_at < history_cutoff)
            )
            counts["changes"] += result.rowcount
            await s.execute(
                delete(Audit).where(Audit.project_id == project_id, Audit.created_at < history_cutoff)
            )
            audit(s, project_id, "retention.applied", payloads=len(rows))
    async with db.session() as s:
        runs = (
            await s.scalars(
                select(ReplayRun).where(
                    ReplayRun.finished_at < history_cutoff,
                    ReplayRun.state.in_(["completed", "failed", "interrupted", "cancelled"]),
                    ReplayRun.report.is_not(None),
                )
            )
        ).all()
    async with httpx.AsyncClient(trust_env=False, timeout=5) as client:
        for run in runs:
            if run.report.get("retention_expired"):
                continue
            for variant in ("a", "b"):
                response = await client.delete(
                    getattr(config, f"lab_{variant}_url") + "/control/state/" + str(run.id),
                    headers={
                        "Authorization": "Bearer "
                        + getattr(config, f"lab_{variant}_token").get_secret_value()
                    },
                )
                response.raise_for_status()
            async with db.session() as s, s.begin():
                row = await s.get(ReplayRun, run.id, with_for_update=True)
                row.report = {"retention_expired": True}
                row.checkpoint = {}
            counts["lab_runs"] += 1
    async with db.session() as s, s.begin():
        await s.execute(delete(BrowserSession).where(BrowserSession.expires_at < current))
        await s.execute(delete(RequestLimit).where(RequestLimit.window < current - timedelta(days=1)))
    return counts
