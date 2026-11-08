# Verification performed on 2026-10-30

[Русский](VERIFICATION.md) · **English**

Ubuntu/WSL2 Linux x86_64; Python 3.14.4 in .venv; container Python 3.14,
PostgreSQL 18.6, RabbitMQ 4, OpenTelemetry Collector 0.164.0, Docker 29.1.3,
Compose 2.40.3. Approximately 7.5 GiB WSL RAM with other local applications.
Images and dependencies are pinned in source.

## Automated evidence

- **42 EventHub tests:** strict JSON/schema/signature contracts, concurrent
  deduplication/conflicts, project/role/source/expiry/revocation, CSRF/sessions,
  export/capture/metrics, retention, distinct manual runs/history, and recovery
  against real PostgreSQL/RabbitMQ/HTTP.
- Hypothesis executes 25 generated sequences against real HTTP receiver B.
  Reordering/duplicates/omissions preserve the maximum received revision and
  avoid repeated effects for an event ID.
- pip check, Ruff lint/format, and private-key source scanning.
- **143 booking-website checks** passed after the adapter migration, with one
  live check normally skipped without its temporary key. All **7 source tests**
  were separately run with a live key, including three snapshots sent over HTTP
  from a test database; the key was revoked. This does not update the separately
  published booking repository or change its 136-test baseline.

## Seven failure windows

| Window | Observed evidence |
| --- | --- |
| Event/outbox commit → publish | Real broker stopped; API returned 202; both deliveries accepted after restart |
| Broker confirm → outbox update | Controlled exception after a real confirm; republished with one original delivery |
| Claim → HTTP | Scheduler recovers an expired lease and work resumes |
| Receiver commit → response | Real database commit, HTTP delay/timeout/retry; B avoids duplicate effects |
| Worker commit → ack | Actual AMQP channel closed with unacked work; redelivery adds no attempt |
| Expired lease → late result | Old token rejected before and after the new owner completes |
| Receiver step → runner checkpoint | Controlled post-commit interruption; invocation-ledger recovery preserves normalized hash |

Controlled exceptions model process interruption, not physical power loss.
Broker outages, HTTP timeouts, channel loss, and databases are real. Arbitrary
kernel/network disasters were not tested.

## Containers, restoration, and browser

- Complete Compose stack ran with API bound only to 127.0.0.1:8100.
- An isolated fresh-volume stack initialized roles/databases, migrated,
  bootstrapped, and met expectations for both HTTP cases; temporary volumes were removed.
- Neither receiver could connect to external 1.1.1.1:443 or core DB with its own
  role. Core secrets/docker.sock were absent from receiver containers.
- Three custom-format backups restored into new random databases. All 25 table
  counts matched; temporary databases were removed.
- The OpenTelemetry Collector actually received API/delivery/replay spans.
- Chrome/Playwright checked four sections at 1440/768/375 px, no JS errors or
  horizontal overflow, case capture/run/report, and Escape dismissal. Published
  screenshots show the running app rather than mockups.

## Small load probe

50 synthetic events, 5 concurrent requests, local Docker/WSL:

| Metric | Observation |
| --- | --- |
| Accepted | 50/50, HTTP 202 |
| Group duration | 2.237 s |
| Median API response | 201.34 ms |
| p95 API response | 316.12 ms |

This measures API acceptance, not final business-effect latency. It used the
main local worker/Collector stack alongside other apps, without dedicated
hardware or repeated production trials. It is not an SLA, capacity limit, or
framework comparison. About 0.8 GiB across ten containers was a one-time snapshot.

## Reproduction and provenance

Commands are in the README and [operations](OPERATIONS.en.md). Full private reports
remain in `.local/reports`; only sanitized cases/screenshots are published.
The workflow uses pinned checkout/setup-python revisions. Check the
[actual workflow](https://github.com/Sasambatra/eventhub-replaylab/actions/workflows/check.yml)
and each run's commit SHA separately: local results do not establish hosted CI
success. No public server is deployed.
