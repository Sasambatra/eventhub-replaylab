"""Use the installed Docker engine without modifying group permissions."""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def docker_command():
    if os.access("/var/run/docker.sock", os.R_OK | os.W_OK):
        return ["docker"]
    wsl = Path("/mnt/c/Windows/System32/wsl.exe")
    if wsl.exists():
        return [str(wsl), "--distribution", "Ubuntu", "--user", "root", "--exec", "docker"]
    return ["sudo", "docker"]
