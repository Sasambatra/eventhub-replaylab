import asyncio
from collections import Counter
from datetime import timedelta
import random
import uuid

import httpx
from sqlalchemy import or_, select

from eventhub.contracts import CaseDefinition, checksum
from eventhub.db import now, uid
from eventhub.models import ReplayCase, ReplayRun
from eventhub.services import change, lock_project
from eventhub.telemetry import log, span
from eventhub.worker import target_url


async def claim_run(db):
    async with db.session() as s:
        candidate = await s.scalar(
            select(ReplayRun)
            .where(
                or_(
                    ReplayRun.state == "pending",
                    (ReplayRun.state == "running") & (ReplayRun.lease_until < now()),
                )
            )
            .order_by(ReplayRun.created_at)
            .limit(1)
        )
        if not candidate:
            return None
    async with db.session() as s, s.begin():
        project = await lock_project(s, candidate.project_id)
        row = await s.get(ReplayRun, candidate.id, with_for_update=True)
        if row.state != "pending" and not (row.state == "running" and row.lease_until < now()):
            return None
        row.state, row.lease_token = "running", uid()
        row.lease_until = now() + timedelta(seconds=60)
        case = await s.get(ReplayCase, row.case_id)
        change(s, project, "replay", id=str(row.id))
        return {
            "id": row.id,
            "project_id": row.project_id,
            "token": row.lease_token,
            "case": case.definition,
            "checksum": case.checksum,
            "checkpoint": row.checkpoint or {},
        }


async def checkpoint(db, work, data, *, state=None, report=None):
    async with db.session() as s, s.begin():
        project = await lock_project(s, work["project_id"])
        row = await s.get(ReplayRun, work["id"], with_for_update=True)
        if row.state != "running" or row.lease_token != work["token"]:
            raise RuntimeError("Replay ownership lost")
        if row.cancel_requested:
            row.state, row.finished_at = "cancelled", now()
            row.lease_token = row.lease_until = None
            change(s, project, "replay", id=str(row.id))
            return False
        row.checkpoint = data
        row.lease_until = now() + timedelta(seconds=60)
        if state:
            row.state, row.report, row.finished_at = state, report, now()
            row.lease_token = row.lease_until = None
        change(s, project, "replay", id=str(row.id))
        return True


def checks(observation, expected):
    states, effects = observation.get("states", []), observation.get("effects", [])
    counts = Counter(e["event_id"] for e in effects)
    duplicates = sum(max(0, n - 1) for n in counts.values())
    state = states[0] if len(states) == 1 else {}
    rules = [
        {
            "name": "Final status",
            "expected": expected.status,
            "actual": state.get("status"),
            "passed": len(states) == 1 and state.get("status") == expected.status,
        },
        {
            "name": "Latest revision",
            "expected": expected.revision,
            "actual": state.get("revision"),
            "passed": len(states) == 1 and state.get("revision") == expected.revision,
        },
        {"name": "Duplicate effects", "expected": 0, "actual": duplicates, "passed": duplicates == 0},
    ]
    return {
        "result": "pass" if all(rule["passed"] for rule in rules) else "fail",
        "rules": rules,
        "applied_effects": len(effects),
    }


async def run_claimed(db, config, work, client=None, interrupt_after=None):
    own = client is None
    client = client or httpx.AsyncClient(timeout=3, follow_redirects=False, trust_env=False)
    case = CaseDefinition.model_validate(work["case"])
    data = {**work["checkpoint"]}
    data.setdefault("steps", {"A": [], "B": []})
    try:
        versions = {}
        for variant in ("A", "B"):
            response = await client.get(target_url(config, "lab-" + variant.lower()) + "/health")
            response.raise_for_status()
            health = response.json()
            if health["variant"] != variant:
                raise RuntimeError("Receiver variant mismatch")
            versions[variant] = health["version"]
        if data.get("versions") and data["versions"] != versions:
            await checkpoint(
                db, work, data, state="interrupted", report={"error": "receiver_version_changed"}
            )
            return
        data["versions"] = versions
        if not await checkpoint(db, work, data):
            return
        events = {event.id: event for event in case.events}
        observations = {}
        for variant in ("A", "B"):
            base = target_url(config, "lab-" + variant.lower())
            headers = {
                "Authorization": "Bearer "
                + getattr(config, f"lab_{variant.lower()}_token").get_secret_value()
            }
            for index, step in enumerate(case.steps):
                if not await checkpoint(db, work, data):
                    return
                for attempt in range(1, 4):
                    invocation_id = str(uuid.uuid5(work["id"], f"{variant}:{index}:{attempt}"))
                    existing = next(
                        (
                            entry
                            for entry in data["steps"][variant]
                            if entry["invocation_id"] == invocation_id
                        ),
                        None,
                    )
                    if existing:
                        if existing["final"]:
                            break
                        continue
                    fault = step.fault if attempt <= step.fault_attempts else "none"
                    tick = index * 100 + attempt + random.Random(case.seed + index).randrange(10)
                    packet = {
                        "run_id": str(work["id"]),
                        "invocation_id": invocation_id,
                        "event": events[step.event_id].payload(),
                        "fault": fault,
                        "virtual_time": (case.start_time + timedelta(seconds=tick)).isoformat(),
                    }
                    stored = await client.get(base + "/control/invocations/" + invocation_id, headers=headers)
                    transport = "recovered"
                    if stored.status_code == 404:
                        try:
                            with span("replay.invoke", run_id=work["id"], variant=variant):
                                reply = await client.post(
                                    base + "/invoke",
                                    json=packet,
                                    headers=headers,
                                    timeout=0.15 if fault == "commit_then_delay_response" else 3,
                                )
                            transport = str(reply.status_code)
                        except httpx.TimeoutException:
                            transport = "timeout"
                        stored = await client.get(
                            base + "/control/invocations/" + invocation_id, headers=headers
                        )
                    stored.raise_for_status()
                    result = stored.json()
                    if result["run_id"] != str(work["id"]):
                        raise RuntimeError("Invocation namespace mismatch")
                    retry = result["status_code"] in {429, 500} or fault == "commit_then_delay_response"
                    final = not retry or attempt == 3
                    data["steps"][variant].append(
                        {
                            "invocation_id": invocation_id,
                            "index": index,
                            "attempt": attempt,
                            "event_id": step.event_id,
                            "fault": fault,
                            "outcome": result["outcome"],
                            "transport": transport,
                            "final": final,
                        }
                    )
                    if interrupt_after and len(data["steps"][variant]) == interrupt_after:
                        raise InterruptedError("Controlled runner interruption")
                    if not await checkpoint(db, work, data):
                        return
                    if final:
                        if retry:
                            raise RuntimeError("Replay attempt budget exhausted")
                        break
            observed = await client.get(base + "/control/state/" + str(work["id"]), headers=headers)
            observed.raise_for_status()
            observations[variant] = observed.json()
        variants = {variant: checks(observations[variant], case.expected) for variant in ("A", "B")}
        normalized = {"report_version": 1, "expected": case.expected.model_dump(), "variants": variants}
        report = {
            "report_version": 1,
            "case_name": case.name,
            "case_checksum": work["checksum"],
            "receiver_versions": versions,
            "seed": case.seed,
            "variants": variants,
            "observations": observations,
            "steps": data["steps"],
            "normalized": normalized,
            "normalized_checksum": checksum(normalized),
            "expectations_met": all(variants[v]["result"] == case.expected_variants[v] for v in ("A", "B")),
            "transport_note": "HTTP acceptance and verified application state are separate results.",
        }
        await checkpoint(db, work, data, state="completed", report=report)
    except InterruptedError:
        raise
    except asyncio.CancelledError:
        raise
    except Exception:
        await checkpoint(
            db, work, data, state="failed", report={"error": "replay_dependency_or_contract_failure"}
        )
    finally:
        if own:
            await client.aclose()


async def serve_replays(db, config):
    async with httpx.AsyncClient(timeout=3, follow_redirects=False, trust_env=False) as client:
        while True:
            try:
                work = await claim_run(db)
                if work:
                    await run_claimed(db, config, work, client)
                else:
                    await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                raise
            except Exception:
                log("replay.deferred")
                await asyncio.sleep(2)
