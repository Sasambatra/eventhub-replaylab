"""Small explicit local API experiment, not a production capacity claim."""

import asyncio
from datetime import UTC, datetime
import json
import statistics
import time
import uuid

import httpx

from docker_command import ROOT


async def main():
    access = json.loads((ROOT / ".local/owner-access.txt").read_text())
    semaphore = asyncio.Semaphore(5)
    results = []
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8100",
        headers={"Authorization": "Bearer " + access["key"]},
        timeout=15,
        trust_env=False,
    ) as client:

        async def emit(index):
            booking = str(uuid.uuid4())
            packet = {
                "specversion": "1.0",
                "id": str(uuid.uuid4()),
                "source": "urn:demo:load",
                "type": "studio.booking.snapshot.v1",
                "subject": "booking/" + booking,
                "time": datetime.now(UTC).isoformat(),
                "datacontenttype": "application/json",
                "data": {"booking_id": booking, "revision": 1, "status": "confirmed"},
            }
            async with semaphore:
                start = time.perf_counter()
                result = await client.post("/v1/events", json=packet)
                results.append(
                    {"status": result.status_code, "elapsed_ms": (time.perf_counter() - start) * 1000}
                )

        start = time.perf_counter()
        await asyncio.gather(*(emit(i) for i in range(50)))
        elapsed = time.perf_counter() - start
    values = sorted(r["elapsed_ms"] for r in results)
    report = {
        "events": 50,
        "concurrency": 5,
        "seconds": round(elapsed, 3),
        "accepted": sum(r["status"] == 202 for r in results),
        "p50_ms": round(statistics.median(values), 2),
        "p95_ms": round(values[int(len(values) * 0.95) - 1], 2),
        "scope": "local WSL Docker; HTTP acceptance only, not end-to-end effect latency",
    }
    path = ROOT / ".local/reports/load-probe.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    path.chmod(0o600)
    print(json.dumps(report))
    if report["accepted"] != 50:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
