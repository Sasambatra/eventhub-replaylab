"""Stop this project's broker, accept a fake snapshot, then verify recovered delivery."""

from datetime import UTC, datetime
import json
import subprocess
import time
import uuid

import httpx
from docker_command import ROOT, docker_command


def main():
    docker = docker_command()
    access = json.loads((ROOT / ".local/owner-access.txt").read_text())
    booking = str(uuid.uuid4())
    packet = {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "source": "urn:demo:broker-outage",
        "type": "studio.booking.snapshot.v1",
        "subject": "booking/" + booking,
        "time": datetime.now(UTC).isoformat(),
        "data": {"booking_id": booking, "revision": 1, "status": "confirmed"},
    }

    def run(*args):
        return subprocess.run(docker + ["compose", *args], cwd=ROOT, check=True, capture_output=True)

    with httpx.Client(
        base_url="http://127.0.0.1:8100",
        headers={"Authorization": "Bearer " + access["key"]},
        timeout=10,
        trust_env=False,
    ) as client:
        for _ in range(120):
            try:
                if client.get("/health/ready").status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.5)
        else:
            raise RuntimeError("API readiness deadline exceeded")
        try:
            run("stop", "rabbitmq")
            accepted = client.post("/v1/events", json=packet)
            accepted.raise_for_status()
            assert accepted.status_code == 202
            event_id = accepted.json()["event_id"]
            before = client.get("/v1/events/" + event_id + "/deliveries").json()["items"]
            assert before and all(r["state"] == "ready" for r in before)
        finally:
            run("start", "rabbitmq")
        for _ in range(180):
            rows = client.get("/v1/events/" + event_id + "/deliveries").json()["items"]
            if rows and all(r["state"] == "accepted" for r in rows):
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("Delivery recovery deadline exceeded")
    report = {
        "broker_unavailable_acceptance": 202,
        "recovered_deliveries": len(rows),
        "states": [r["state"] for r in rows],
    }
    path = ROOT / ".local/reports/broker-recovery.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    path.chmod(0o600)
    print(json.dumps(report))


if __name__ == "__main__":
    main()
