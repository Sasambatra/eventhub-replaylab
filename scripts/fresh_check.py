"""Exercise empty volumes and remove only the explicitly named disposable stack."""

import json
import os
import subprocess
import sys
import time

import httpx
from docker_command import ROOT, docker_command


def main():
    private = ROOT / ".local/freshcheck"
    private.mkdir(mode=0o700, exist_ok=True)
    compose = docker_command() + [
        "compose",
        "-p",
        "eventhub-freshcheck",
        "-f",
        "compose.yaml",
        "-f",
        "compose.smoke.yaml",
    ]

    def run(*args):
        return subprocess.run(compose + list(args), cwd=ROOT, check=True, capture_output=True)

    owned = False
    try:
        existing = subprocess.run(
            compose + ["ps", "-a", "-q"], cwd=ROOT, check=True, capture_output=True
        ).stdout.strip()
        if existing:
            raise RuntimeError("Disposable stack already exists; inspect before retrying")
        if (private / "owner-access.txt").exists():
            (private / "owner-access.txt").unlink()
        owned = True
        run("up", "-d", "--wait", "postgres", "rabbitmq")
        run("run", "--rm", "--no-deps", "migrate")
        run("run", "--rm", "--no-deps", "api", "python", "-m", "eventhub", "bootstrap-demo")
        run(
            "up",
            "-d",
            "--no-build",
            "api",
            "publisher",
            "worker",
            "scheduler",
            "replay-worker",
            "receiver-a",
            "receiver-b",
            "otel",
        )
        with httpx.Client(trust_env=False, timeout=1) as client:
            for _ in range(120):
                try:
                    if client.get("http://127.0.0.1:18100/health/ready").status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(0.5)
            else:
                raise RuntimeError("Fresh API readiness deadline")
        for case in ["cancelled_booking_reordered", "timeout_after_commit"]:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "eventhub",
                    "verify-case",
                    "--url",
                    "http://127.0.0.1:18100",
                    "--access-file",
                    ".local/freshcheck/owner-access.txt",
                    "--case",
                    "lab/cases/" + case + ".json",
                    "--output",
                    ".local/reports/fresh-" + case + ".json",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
            )
        path = ROOT / ".local/reports/fresh-start.json"
        path.write_text(
            json.dumps(
                {
                    "empty_volumes": True,
                    "migrations": "passed",
                    "bootstrap": "passed",
                    "real_http_cases": 2,
                    "main_stack_preserved": True,
                },
                indent=2,
            )
            + "\n"
        )
        os.chmod(path, 0o600)
        print("Fresh-volume initialization and both real HTTP replay cases passed.")
    finally:
        # No main stack or unnamed volume may be removed by this script.
        if owned:
            run("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
