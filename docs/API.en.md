# Version 1 API

[Русский](API.md) · **English**

Authenticate with `Authorization: Bearer <key>` or a same-origin browser session.
Never place keys in URLs. Cookie-authenticated writes require Origin and
X-CSRF-Token. Local schemas: `/docs` and `/openapi.json`; operations still require authorization.

| Method / path | Purpose | Scope |
| --- | --- | --- |
| POST /v1/events | Accept: 202 new, 200 retry, 409 conflict | events:write + source |
| GET /v1/events | Search, limit 1–100, signed cursor | events:read |
| GET /v1/events/{id} | Event, hash, retention state | events:read |
| GET /v1/events/{id}/deliveries | Receivers and individual attempts | events:read |
| POST /v1/deliveries/{id}/retry | New delivery run; Idempotency-Key required | deliveries:retry |
| GET /v1/destinations | Project destinations, no secrets | events:read |
| POST /v1/destinations | name + lab-a/lab-b adapter | destinations:manage |
| PATCH /v1/destinations/{id} | active true/false | destinations:manage |
| GET /v1/cases | Laboratory case definitions | replays:read |
| POST /v1/cases/import | Strict case or {case,checksum} | cases:manage |
| POST /v1/cases/capture | {event_id,pattern}, arrival/reorder_duplicate | cases:manage + events:read |
| GET /v1/cases/{id}/export | Sanitized copy with checksum | cases:export |
| POST /v1/replays | {case_id}; Idempotency-Key required | replays:run |
| GET /v1/replays | Latest 25 runs | replays:read |
| GET /v1/replays/{id}/report | State, observations, rules, outcome | replays:read |
| POST /v1/replays/{id}/cancel | Persist a cancellation request | replays:run |
| GET /v1/overview, /v1/metrics, /v1/stream | Overview, scoped gauges, SSE | events:read |
| GET/POST /v1/keys | List/create a key; plaintext shown once | keys:manage, owner |
| POST /v1/keys/{id}/revoke | Revoke a key and related sessions | keys:manage |

The same operation/key returns its original run. Changed intent under the same
key returns `409`. Retrying an exhausted/accepted/cancelled delivery creates
a separate run with the original event ID; active runs cannot restart.

Other projects appear missing. A source key cannot read/manage data. Delegated
keys expire after 90 days by default, configurable with `expires_in_days` 1–365.
Viewer is read/export only. Operator manages cases/destinations/retries but
cannot issue keys or write events.

The CloudEvents profile is in `lab/cases/*.json`. Only
`studio.booking.snapshot.v1` is accepted: urn source, `subject=booking/<UUID>`,
positive integer revision, held/confirmed/cancelled/expired status, timezone-aware
time. Event bodies are limited to 64 KiB; cases to 1 MiB, 500 definitions and
2,000 steps for one aggregate, with explicit A/B expectations. No arbitrary code/URLs.
The internal RabbitMQ contract is not a public event-ingestion endpoint.

The CLI reads a private key file rather than accepting a key on the command line:

```bash
.venv/bin/python -m eventhub demo --output .local/reports/demo.json
.venv/bin/python -m eventhub verify-case --case lab/cases/timeout_after_commit.json
```

`demo` imports a case, submits ordinary events, and starts replay. `verify-case`
checks the case in the laboratory only. Exit 0 means completed with all declared
expectations met, including the expected failure of control receiver A.
