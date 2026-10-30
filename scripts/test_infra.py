"""Prepare only explicitly named test databases/vhost; preserve product data."""

import subprocess
import sys

from docker_command import ROOT, docker_command


def main():
    docker = docker_command()

    def run(*args, **kwargs):
        return subprocess.run(docker + list(args), cwd=ROOT, check=True, **kwargs)

    run(
        "compose",
        "-f",
        "compose.yaml",
        "-f",
        "compose.dev.yaml",
        "up",
        "-d",
        "--wait",
        "postgres",
        "rabbitmq",
    )
    listed = (
        run(
            "exec",
            "eventhub-replaylab-rabbitmq-1",
            "rabbitmqctl",
            "list_vhosts",
            "--silent",
            capture_output=True,
        )
        .stdout.decode()
        .splitlines()
    )
    if "eventhub_test" not in listed:
        run("exec", "eventhub-replaylab-rabbitmq-1", "rabbitmqctl", "add_vhost", "eventhub_test")
    run(
        "exec",
        "eventhub-replaylab-rabbitmq-1",
        "rabbitmqctl",
        "set_permissions",
        "-p",
        "eventhub_test",
        "eventhub",
        "^",
        "^",
        "^",
    )
    subprocess.run([sys.executable, "-m", "eventhub", "migrate", "--test"], cwd=ROOT, check=True)


if __name__ == "__main__":
    main()
