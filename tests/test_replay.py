from datetime import timedelta
import json
import uuid

from hypothesis import given, settings as hypothesis_settings, strategies as st
import httpx
import pytest

from eventhub.db import now
from eventhub.models import ReplayRun
from eventhub.replay import claim_run, run_claimed
from eventhub.services import save_case, start_replay
from eventhub.settings import ROOT, Settings


async def execute(actor, definition):
    row = await save_case(actor.db, actor.key, definition)
    run = await start_replay(actor.db, actor.key, row.id, str(uuid.uuid4()))
    work = await claim_run(actor.db)
    assert work is not None
    await run_claimed(actor.db, actor.config, work)
    async with actor.db.session() as s:
        return await s.get(ReplayRun, run.id)


@pytest.mark.parametrize("filename", ["cancelled_booking_reordered.json", "timeout_after_commit.json"])
async def test_real_http_replay_matches_declared_expectations(actor, filename):
    definition = json.loads((ROOT / "lab/cases" / filename).read_text())
    row = await execute(actor, definition)
    assert row.state == "completed"
    assert row.report["expectations_met"] is True
    assert row.report["variants"]["A"]["result"] == "fail"
    assert row.report["variants"]["B"]["result"] == "pass"
    if filename.startswith("timeout"):
        assert any(step["transport"] == "timeout" for step in row.report["steps"]["A"])


async def test_runner_recovery_after_receiver_commit(actor, case):
    original = await save_case(actor.db, actor.key, case)
    run = await start_replay(actor.db, actor.key, original.id, str(uuid.uuid4()))
    work = await claim_run(actor.db)
    with pytest.raises(InterruptedError):
        await run_claimed(actor.db, actor.config, work, interrupt_after=1)
    async with actor.db.session() as s, s.begin():
        row = await s.get(ReplayRun, run.id)
        row.lease_until = now() - timedelta(seconds=1)
    resumed = await claim_run(actor.db)
    await run_claimed(actor.db, actor.config, resumed)
    async with actor.db.session() as s:
        result = await s.get(ReplayRun, run.id)
    fresh = await execute(actor, case)
    assert result.state == "completed"
    assert result.report["normalized_checksum"] == fresh.report["normalized_checksum"]
    assert result.report["variants"]["A"]["applied_effects"] == 4


async def test_cancellation_prevents_further_steps(actor, case):
    row = await save_case(actor.db, actor.key, case)
    run = await start_replay(actor.db, actor.key, row.id, str(uuid.uuid4()))
    work = await claim_run(actor.db)
    async with actor.db.session() as s, s.begin():
        current = await s.get(ReplayRun, run.id)
        current.cancel_requested = True
    await run_claimed(actor.db, actor.config, work)
    async with actor.db.session() as s:
        result = await s.get(ReplayRun, run.id)
        assert result.state == "cancelled" and not result.report


@hypothesis_settings(max_examples=25, deadline=None)
@given(order=st.lists(st.sampled_from([1, 2, 3]), min_size=1, max_size=10))
def test_receiver_b_invariant_under_generated_reordering(lab_servers, order):
    config = Settings()
    source = json.loads((ROOT / "lab/cases/cancelled_booking_reordered.json").read_text())
    mapping = {e["data"]["revision"]: e for e in source["events"]}
    run_id = str(uuid.uuid4())
    headers = {"Authorization": "Bearer " + config.lab_b_token.get_secret_value()}
    with httpx.Client(trust_env=False, timeout=5) as client:
        for index, revision in enumerate(order):
            reply = client.post(
                "http://127.0.0.1:8212/invoke",
                headers=headers,
                json={
                    "run_id": run_id,
                    "invocation_id": str(uuid.uuid4()),
                    "event": mapping[revision],
                    "fault": "none",
                    "virtual_time": "2026-10-29T12:00:00Z",
                },
            )
            assert reply.status_code == 200
        result = client.get("http://127.0.0.1:8212/control/state/" + run_id, headers=headers).json()
    assert result["states"][0]["revision"] == max(order)
    assert result["states"][0]["status"] == mapping[max(order)]["data"]["status"]
    effects = [e["event_id"] for e in result["effects"]]
    assert len(effects) == len(set(effects))
