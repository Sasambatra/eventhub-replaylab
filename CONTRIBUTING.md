# Development

EventHub is a local event-delivery and incident-replay demonstration. The supported
test environment is Linux x86_64, Python 3.14 and Docker Compose v2.24.4 or newer.
Prepared receivers use PostgreSQL, not a mock or an SQLite replacement.

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install --require-hashes --only-binary=:all: -r requirements-dev.lock
.venv/bin/python scripts/init_env.py
.venv/bin/python scripts/test_infra.py
bash scripts/check.sh
```

The test databases are `eventhub_test`, `eventhub_lab_a_test` and
`eventhub_lab_b_test`, with a separate `eventhub_test` broker vhost. The fixture
owns receiver processes on 8211/8212 and shuts down only those processes. Tests
must fail rather than fall back to a product database.

`test_infra.py` exposes database/broker ports on loopback for native tests.
Return to the standard local topology with `python3 scripts/stack.py start`.
For a separate empty-volume run, build the application image and run
`.venv/bin/python scripts/fresh_check.py`; port 18100 must be free.

Include regression checks for transaction boundaries, duplicate/conflicting
requests, lease fencing or project authorization when changing those behaviours.
Do not add tests that merely repeat a low-impact implementation detail. New
packages or dependency upgrades require an explicit scope decision.

Keep transport acceptance separate from business correctness. Receiver A contains
an intentional defect: do not "fix" it without preserving a control comparison.
Do not add arbitrary code execution or external destination URLs to this demo.

The project was developed with AI assistance. Issue reports and changes should
be assessed against reproducible code and verification evidence, not assumptions
about commercial deployment or individual experience.

Report bugs with expected/observed behaviour, a sanitized JSON case and platform
versions. Never attach `.env`, access keys, raw database dumps or customer data.
For security issues, follow [SECURITY.md](SECURITY.md).
