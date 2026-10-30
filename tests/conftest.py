from dataclasses import dataclass
import os
import subprocess
import sys
import time

from dotenv import dotenv_values
import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, select

from eventhub.app import create_app
from eventhub.db import Database, uid
from eventhub.models import (
    APIKey,
    Attempt,
    Audit,
    BrowserSession,
    Change,
    Delivery,
    DeliveryRun,
    Destination,
    Event,
    Outbox,
    Project,
    ReplayCase,
    ReplayRun,
)
from eventhub.security import SCOPES, new_key
from eventhub.settings import ROOT, Settings


@pytest.fixture(scope="session")
def lab_servers():
    config = Settings()
    values = {**dotenv_values(ROOT / ".env"), **os.environ}
    processes, handles = [], []
    try:
        for variant, port in [("A", 8211), ("B", 8212)]:
            env = {
                **os.environ,
                "LAB_VARIANT": variant,
                "LAB_TOKEN": getattr(config, f"lab_{variant.lower()}_token").get_secret_value(),
                "LAB_DB_HOST": config.db_host,
                "LAB_DB_PORT": str(config.db_port),
                "LAB_DB_NAME": "eventhub_lab_" + variant.lower() + "_test",
                "LAB_DB_USER": "eventhub_lab_" + variant.lower(),
                "LAB_DB_PASSWORD": values["LAB_" + variant + "_DB_PASSWORD"],
            }
            path = ROOT / ".local" / ("test-receiver-" + variant + ".log")
            handle = path.open("w")
            handles.append(handle)
            processes.append(
                subprocess.Popen(
                    [sys.executable, "-m", "lab", "--port", str(port)],
                    cwd=ROOT,
                    env=env,
                    stdout=handle,
                    stderr=subprocess.STDOUT,
                )
            )
        with httpx.Client(trust_env=False, timeout=1) as client:
            for _ in range(100):
                try:
                    if all(
                        client.get(f"http://127.0.0.1:{p}/health").status_code == 200 for p in [8211, 8212]
                    ):
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(0.1)
            else:
                raise RuntimeError("Prepared test receivers did not become ready")
        yield
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for handle in handles:
            handle.close()


@pytest.fixture
def config(lab_servers):
    return (
        Settings()
        .for_tests()
        .model_copy(update={"lab_a_url": "http://127.0.0.1:8211", "lab_b_url": "http://127.0.0.1:8212"})
    )


@dataclass(repr=False)
class Actor:
    db: object
    key: object
    raw: str
    config: object


@pytest_asyncio.fixture
async def actor(config):
    db = Database(config)
    project_id = uid()
    raw, key = new_key(config, project_id, SCOPES, "owner", ["*"])
    async with db.session() as s, s.begin():
        s.add(Project(id=project_id, name="Isolated test project"))
        await s.flush()
        s.add(key)
    yield Actor(db, key, raw, config)
    async with db.session() as s, s.begin():
        ids = select(Delivery.id).where(Delivery.project_id == project_id)
        await s.execute(delete(Attempt).where(Attempt.delivery_id.in_(ids)))
        await s.execute(delete(Outbox).where(Outbox.delivery_id.in_(ids)))
        await s.execute(delete(DeliveryRun).where(DeliveryRun.delivery_id.in_(ids)))
        await s.execute(delete(Delivery).where(Delivery.project_id == project_id))
        await s.execute(delete(Event).where(Event.project_id == project_id))
        await s.execute(delete(ReplayRun).where(ReplayRun.project_id == project_id))
        await s.execute(delete(ReplayCase).where(ReplayCase.project_id == project_id))
        await s.execute(delete(Change).where(Change.project_id == project_id))
        await s.execute(delete(Audit).where(Audit.project_id == project_id))
        await s.execute(
            delete(BrowserSession).where(
                BrowserSession.key_id.in_(select(APIKey.id).where(APIKey.project_id == project_id))
            )
        )
        await s.execute(delete(APIKey).where(APIKey.project_id == project_id))
        await s.execute(delete(Destination).where(Destination.project_id == project_id))
        await s.execute(delete(Project).where(Project.id == project_id))
    await db.close()


@pytest_asyncio.fixture
async def client(actor):
    app = create_app(actor.config)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
        headers={"Authorization": "Bearer " + actor.raw},
    ) as client:
        yield client
    await app.state.db.close()


@pytest.fixture
def case():
    import json

    return json.loads((ROOT / "lab/cases/cancelled_booking_reordered.json").read_text())
