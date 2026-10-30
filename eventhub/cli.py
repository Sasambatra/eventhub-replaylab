import argparse
import asyncio
import json
import logging
import subprocess
import sys
import uuid

import httpx
from sqlalchemy import text
import uvicorn

from eventhub.bootstrap import bootstrap
from eventhub.contracts import CaseDefinition
from eventhub.db import Database
from eventhub.replay import serve_replays
from eventhub.settings import ROOT, Settings
from eventhub.telemetry import setup
from eventhub.worker import serve_publisher, serve_worker, sweep


def migrate(config, test=False):
    databases = ["eventhub_test"] if test else ["eventhub"]
    labs = ["eventhub_lab_a_test", "eventhub_lab_b_test"] if test else ["eventhub_lab_a", "eventhub_lab_b"]
    for database in databases + labs:
        args = [sys.executable, "-m", "alembic"]
        if "_lab_" in database:
            args.extend(["-n", "lab"])
        args.extend(["-x", "database=" + database, "upgrade", "head"])
        subprocess.run(args, cwd=ROOT, check=True)


async def run_role(args, config):
    db = Database(config)
    try:
        if args.command == "bootstrap-demo":
            path = await bootstrap(db, config)
            print("Demo data ready. Private owner access: " + str(path))
        elif args.command == "cleanup":
            from eventhub.retention import cleanup

            print(json.dumps(await cleanup(db, config)))
        elif args.command == "doctor":
            async with db.session() as session:
                await session.execute(text("SELECT 1"))
            print("Database reachable; isolated demo profile; credentials suppressed.")
        elif args.command == "publisher":
            await serve_publisher(db, config)
        elif args.command == "worker":
            await serve_worker(db, config)
        elif args.command == "scheduler":
            while True:
                try:
                    await sweep(db, config)
                except Exception:
                    logging.warning("Scheduler dependency unavailable; retrying")
                await asyncio.sleep(0.5)
        elif args.command == "replay-worker":
            await serve_replays(db, config)
        elif args.command in {"demo", "verify-case"}:
            access = json.loads((ROOT / args.access_file).read_text())
            definition = CaseDefinition.model_validate_json((ROOT / args.case).read_text())
            async with httpx.AsyncClient(
                base_url=args.url,
                headers={"Authorization": "Bearer " + access["key"]},
                timeout=10,
                trust_env=False,
            ) as client:
                response = await client.post("/v1/cases/import", json=definition.model_dump(mode="json"))
                response.raise_for_status()
                case_id = response.json()["id"]
                if args.command == "demo":
                    for event in definition.events:
                        delivered = await client.post("/v1/events", json=event.payload())
                        delivered.raise_for_status()
                response = await client.post(
                    "/v1/replays", json={"case_id": case_id}, headers={"Idempotency-Key": str(uuid.uuid4())}
                )
                response.raise_for_status()
                run_id = response.json()["id"]
                for _ in range(120):
                    result = await client.get("/v1/replays/" + run_id + "/report")
                    result.raise_for_status()
                    value = result.json()
                    if value["state"] not in {"pending", "running", "comparing"}:
                        break
                    await asyncio.sleep(0.5)
                else:
                    raise RuntimeError("Replay deadline exceeded")
                if args.output:
                    path = ROOT / args.output
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
                    path.chmod(0o600)
                report = value.get("report") or {}
                print(
                    json.dumps(
                        {
                            "run_id": run_id,
                            "state": value["state"],
                            "expectations_met": report.get("expectations_met", False),
                        }
                    )
                )
                if value["state"] != "completed" or not report.get("expectations_met"):
                    raise RuntimeError("Replay did not match its declared expectations")
    finally:
        await db.close()


def main():
    parser = argparse.ArgumentParser(prog="eventhub")
    parser.add_argument(
        "command",
        choices=[
            "api",
            "publisher",
            "worker",
            "scheduler",
            "replay-worker",
            "doctor",
            "cleanup",
            "migrate",
            "bootstrap-demo",
            "demo",
            "verify-case",
        ],
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8100)
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--case", default="lab/cases/cancelled_booking_reordered.json")
    parser.add_argument("--url", default="http://127.0.0.1:8100")
    parser.add_argument("--output")
    parser.add_argument("--access-file", default=".local/owner-access.txt")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    config = Settings()
    if args.test:
        config = config.for_tests()
    if args.command == "migrate":
        migrate(config, args.test)
        return
    provider = setup(args.command, config)
    try:
        if args.command == "api":
            from eventhub.app import create_app

            uvicorn.run(
                create_app(config),
                host=args.host,
                port=args.port,
                access_log=False,
                proxy_headers=False,
                limit_concurrency=128,
            )
        else:
            asyncio.run(run_role(args, config))
    except KeyboardInterrupt:
        pass
    except Exception as error:
        print(
            "Operation stopped: " + type(error).__name__ + ". Check sanitized diagnostics.", file=sys.stderr
        )
        raise SystemExit(1)
    finally:
        provider.shutdown()
