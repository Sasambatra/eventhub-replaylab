"""Private coordinated backups and non-destructive restore drills using existing Docker."""

import argparse
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess

from docker_command import ROOT, docker_command

DBS = ("eventhub", "eventhub_lab_a", "eventhub_lab_b")
ROLES = ("api", "publisher", "worker", "scheduler", "replay-worker", "receiver-a", "receiver-b")
DOCKER = docker_command()


def call(args, **kwargs):
    if not kwargs.get("capture_output"):
        kwargs.setdefault("stderr", subprocess.PIPE)
    return subprocess.run(DOCKER + args, cwd=ROOT, check=True, **kwargs)


def pg(args, **kwargs):
    return call(["exec", "-i", "eventhub-replaylab-postgres-1"] + args, **kwargs)


def counts(database):
    names = (
        pg(
            [
                "psql",
                "-U",
                "postgres",
                "-d",
                database,
                "-Atc",
                "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename",
            ],
            capture_output=True,
        )
        .stdout.decode()
        .splitlines()
    )
    result = {}
    for name in names:
        if not name.replace("_", "").isalnum():
            raise RuntimeError("Unexpected table identifier")
        result[name] = int(
            pg(
                ["psql", "-U", "postgres", "-d", database, "-Atc", f'SELECT count(*) FROM "{name}"'],
                capture_output=True,
            ).stdout
        )
    return result


def backup():
    folder = ROOT / ".local/backups" / (datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ-") + secrets.token_hex(3))
    folder.mkdir(parents=True, mode=0o700)
    running = (
        call(["compose", "ps", "--status", "running", "--services"], capture_output=True)
        .stdout.decode()
        .split()
    )
    active = [r for r in ROLES if r in running]
    try:
        if active:
            call(["compose", "stop", "--timeout", "15"] + active, stdout=subprocess.DEVNULL)
        manifest = {"format": 1, "databases": {}, "created_at": datetime.now(UTC).isoformat()}
        for database in DBS:
            path = folder / (database + ".dump")
            with path.open("xb") as handle:
                os.chmod(path, 0o600)
                pg(
                    [
                        "pg_dump",
                        "-U",
                        "postgres",
                        "-d",
                        database,
                        "--format=custom",
                        "--no-owner",
                        "--no-acl",
                    ],
                    stdout=handle,
                )
            manifest["databases"][database] = {
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "tables": counts(database),
            }
        # The encrypted destination secrets/API hashes depend on these private settings.
        shutil.copyfile(ROOT / ".env", folder / "private.env")
        os.chmod(folder / "private.env", 0o600)
        (folder / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        os.chmod(folder / "manifest.json", 0o600)
    except BaseException:
        shutil.rmtree(folder)
        raise
    finally:
        if active:
            call(["compose", "start"] + active, stdout=subprocess.DEVNULL)
    print("Private backup: " + str(folder))
    return folder


def verify(folder):
    folder = folder.resolve()
    manifest = json.loads((folder / "manifest.json").read_text())
    if manifest.get("format") != 1 or set(manifest.get("databases", {})) != set(DBS):
        raise ValueError("Unsupported backup manifest")
    # Check every archive before creating any database.
    for database in DBS:
        actual = hashlib.sha256((folder / (database + ".dump")).read_bytes()).hexdigest()
        if actual != manifest["databases"][database]["sha256"]:
            raise ValueError("Backup checksum mismatch")
    verified = {}
    for database in DBS:
        temporary = "eventhub_restorecheck_" + secrets.token_hex(8)
        created = False
        try:
            pg(["createdb", "-U", "postgres", temporary], stdout=subprocess.DEVNULL)
            created = True
            with (folder / (database + ".dump")).open("rb") as handle:
                pg(
                    [
                        "pg_restore",
                        "-U",
                        "postgres",
                        "-d",
                        temporary,
                        "--exit-on-error",
                        "--no-owner",
                        "--no-acl",
                    ],
                    stdin=handle,
                    stdout=subprocess.DEVNULL,
                )
            observed = counts(temporary)
            if observed != manifest["databases"][database]["tables"]:
                raise RuntimeError("Restored table counts differ")
            verified[database] = {"tables": len(observed), "rows": sum(observed.values())}
        finally:
            if created:
                pg(["dropdb", "-U", "postgres", "--force", temporary], stdout=subprocess.DEVNULL)
    path = ROOT / ".local/reports/restore-drill.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"restored_and_verified": verified, "original_databases_untouched": True}, indent=2) + "\n"
    )
    os.chmod(path, 0o600)
    print("Restore drill passed for all three databases; temporary databases removed.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["create", "verify", "drill"])
    parser.add_argument("--path", type=Path)
    args = parser.parse_args()
    if args.command == "create":
        backup()
    elif args.command == "drill":
        verify(backup())
    elif args.path:
        verify(args.path)
    else:
        parser.error("verify requires --path")


if __name__ == "__main__":
    try:
        main()
    except OSError, ValueError, RuntimeError, subprocess.CalledProcessError:
        raise SystemExit(
            "Backup operation failed; inspect service health and private archive. Secrets suppressed."
        ) from None
