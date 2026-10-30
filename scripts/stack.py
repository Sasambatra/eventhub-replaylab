"""Start/stop the local product; never install host dependencies or remove volumes."""

import argparse
import subprocess
import sys

from docker_command import ROOT, docker_command


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["start", "stop", "status", "logs"])
    args = parser.parse_args()
    docker = docker_command()

    def run(*words):
        subprocess.run(docker + ["compose", *words], cwd=ROOT, check=True)

    if args.command == "start":
        if not (ROOT / ".env").exists():
            subprocess.run([sys.executable, "scripts/init_env.py"], cwd=ROOT, check=True)
        run("build", "api")
        run("up", "-d", "--wait", "postgres", "rabbitmq")
        run("run", "--rm", "--no-deps", "migrate")
        run("run", "--rm", "--no-deps", "api", "python", "-m", "eventhub", "bootstrap-demo")
        run(
            "up",
            "-d",
            "--no-build",
            "--wait",
            "api",
            "publisher",
            "worker",
            "scheduler",
            "replay-worker",
            "receiver-a",
            "receiver-b",
            "otel",
        )
        print("EventHub: http://127.0.0.1:8100/ — owner key is private in .local/owner-access.txt")
    elif args.command == "stop":
        run("stop")
    elif args.command == "status":
        run("ps")
    else:
        run("logs", "--tail", "80", "api", "publisher", "worker", "scheduler", "replay-worker")


if __name__ == "__main__":
    main()
