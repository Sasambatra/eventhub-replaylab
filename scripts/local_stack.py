"""Run the native development processes against the isolated Docker infrastructure."""

import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from eventhub.settings import Settings  # noqa: E402


def main():
    config = Settings()
    values = {**dotenv_values(ROOT / ".env"), **os.environ}
    for port in [8100, 8111, 8112]:
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                raise SystemExit(f"Port {port} is occupied; existing process was preserved")
    logs = ROOT / ".local/logs"
    logs.mkdir(parents=True, exist_ok=True)
    processes, handles = [], []
    try:
        for variant, port in [("A", 8111), ("B", 8112)]:
            env = {
                **os.environ,
                "LAB_VARIANT": variant,
                "LAB_TOKEN": getattr(config, f"lab_{variant.lower()}_token").get_secret_value(),
                "LAB_DB_HOST": config.db_host,
                "LAB_DB_PORT": str(config.db_port),
                "LAB_DB_NAME": "eventhub_lab_" + variant.lower(),
                "LAB_DB_USER": "eventhub_lab_" + variant.lower(),
                "LAB_DB_PASSWORD": values["LAB_" + variant + "_DB_PASSWORD"],
            }
            handle = (logs / ("receiver-" + variant + ".log")).open("a")
            handles.append(handle)
            processes.append(
                subprocess.Popen(
                    [sys.executable, "-m", "lab", "--port", str(port)],
                    cwd=ROOT,
                    env=env,
                    stdout=handle,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            )
        for role in ["api", "publisher", "worker", "scheduler", "replay-worker"]:
            handle = (logs / (role + ".log")).open("a")
            handles.append(handle)
            processes.append(
                subprocess.Popen(
                    [sys.executable, "-m", "eventhub", role],
                    cwd=ROOT,
                    stdout=handle,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            )
        print("EventHub local stack starting: http://127.0.0.1:8100/", flush=True)
        while all(process.poll() is None for process in processes):
            time.sleep(1)
        raise RuntimeError("A process stopped; inspect sanitized .local/logs")
    except KeyboardInterrupt:
        pass
    finally:
        for process in processes:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
        for process in processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
        for handle in handles:
            handle.close()


if __name__ == "__main__":
    main()
