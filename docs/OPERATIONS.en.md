# Local operations

[Русский](OPERATIONS.md) · **English**

Run commands from the cloned `eventhub-replaylab` root. The demo needs Python
3.10+ and Docker; `.venv` is needed only for native development/checks.
The booking website is a separate application.

## Start, inspect, and stop

```bash
python3 scripts/stack.py start
python3 scripts/stack.py status
python3 scripts/stack.py logs
python3 scripts/stack.py stop
```

Start builds the image, starts PostgreSQL/RabbitMQ, migrates, performs repeat-safe
bootstrap, starts the app, and waits for readiness. Stop preserves volumes.
Status/logs read state and recent sanitized logs. Do not remove the main volumes.

[Dashboard](http://127.0.0.1:8100/) · [API](http://127.0.0.1:8100/docs) ·
[Readiness](http://127.0.0.1:8100/health/ready).

Owner access is the `key` field of private `.local/owner-access.txt`. Copy it
locally into the login form. Bootstrap does not reveal a lost existing key;
recover owner access separately rather than deleting the database. Expired or
revoked keys also invalidate their browser sessions.

## Development and checks

`test_infra.py` enables compose.dev.yaml, exposes loopback 55432/5673, creates a
test vhost, and migrates only `_test` databases. Pytest owns receiver processes
8211/8212 and stops them afterward. Occupied ports/unavailable dependencies fail
the run; product tables are never cleared. `scripts/local_stack.py` is an
additional native developer mode; do not run it beside containers on the same ports.

```bash
.venv/bin/python scripts/test_infra.py
bash scripts/check.sh
python3 scripts/stack.py start
```

The last command restores the standard topology without exposed database/broker
ports. It rebuilds Python changes while preserving volume data.

## Diagnostics

- `/health/live` checks API response; `/health/ready` checks the core database,
  independently of broker availability.
- Authenticated `/v1/metrics` returns current project-scoped gauges: unpublished
  outbox, waiting-delivery age, and state counts.
- Delivery/attempt history separates network errors, budget exhaustion, and
  disabled destinations. Receiver HTTP 200 is not business verification.
- After broker failure, publication reconnects and persisted events should become
  accepted or reach an explained terminal state. Do not delete outbox rows as repair.
- `docker compose logs otel` shows Collector spans. No long-term trace store is
  installed. HTTP access logs are disabled; keys and bodies are not logged.

## Retention

Cleanup is explicit, without an installed periodic schedule:

```bash
# Uses internal database/laboratory addresses inside the API container.
sudo docker compose exec -T api python -m eventhub cleanup
```

The earlier WSL setup also supported its existing Docker engine through root
Ubuntu. Local sudo may ask for a local password; never publish that password.

Payloads default to seven days and are cleared only after active deliveries end.
ID/source/hash remain for deduplication; replaying a removed body returns `410`.
After 30 days, old attempts, terminal-delivery published outbox, SSE changes,
and audit are removed. Expired sessions/counters are also cleaned. Completed
replay data is deleted from both labs and the report becomes retention_expired.
IDs, delivery/run metadata, deduplication keys, and case definitions remain.
This is not an unlimited archive of payloads.

## Backups and restore drills

```bash
.venv/bin/python scripts/backup.py create
.venv/bin/python scripts/backup.py verify --path .local/backups/BACKUP_NAME
.venv/bin/python scripts/backup.py drill
```

Backup briefly stops only this project's running processes, saves three
custom-format dumps, SHA-256, row counts, and private settings, and restores
processes even on error. The common write pause keeps core/receiver snapshots
consistent. Directories are owner-only, but copies are unencrypted: use protected
off-machine storage and test restoration regularly.

Verify checks every hash, restores into random `eventhub_restorecheck_` databases
with pg_restore error checks, compares all table counts, and removes only those
temporary databases. Working databases are not overwritten. The private report
is `.local/reports/restore-drill.json`.

Restoring the main stack is a separate operation: stop writes, preserve existing
volumes, check PostgreSQL/migration versions, and restore `private.env` to `.env`.
Verify in isolation and run both replays before returning the API. Runtime keys,
pepper, and Fernet key must match the backup; changed PostgreSQL passwords also
require role updates. No unchecked overwrite command is provided.

## Platform boundary

The app stops when the computer/Docker/WSL stops. Restarting WSL needs a running
Docker daemon; containers have restart policies. 127.0.0.1 is local to the host.
The [public repository](https://github.com/Sasambatra/eventhub-replaylab) contains
source, not a hosted application. Public hosting is not configured.
