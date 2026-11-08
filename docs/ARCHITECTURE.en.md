# Architecture decisions

[Русский](ARCHITECTURE.md) · **English**

## Acknowledgement boundaries

The API returns `202` after committing Event + Delivery + DeliveryRun + Outbox.
Uniqueness on project/source/external ID and a canonical JSON hash distinguish
retry from conflict. Broker failure cannot undo an accepted event. Receiver
acknowledgement is recorded as `accepted`; business correctness is checked separately.

The publisher leases outbox rows, sends persistent messages with mandatory
routing and publisher confirms, then records publication. An unknown outcome
means retry. Workers accept small, versioned job references and load data from
PostgreSQL. Stale run/generation references are ignored. Invalid envelopes go to
`eventhub.invalid` without payload.

A worker claims work in a short transaction, releases it before HTTP, and saves
the outcome in another transaction. Lease tokens fence late responses. The
scheduler recovers expired leases, schedules due retries, and republishes ready
jobs left unprocessed. Network waits/backoff never hold delivery transactions.

## Concurrency and limits

Project state/SSE sequences use a Project-row lock. A short global advisory lock
serializes worker quota allocation, not long-running cross-project work. Outbox
batches alternate projects. Defaults: 16 concurrent HTTP jobs, 8 per project,
2 per receiver; 10-second HTTP timeout; 60-second lease; 5 attempts within 24 hours.
Quota deferral does not spend an attempt. Receiver B serializes a namespace and
commits its receipt and effect in the same transaction.

## Snapshots rather than commands

Revision describes a complete snapshot of one booking UUID, not arrival time.
B applies only newer revisions; the same event ID/hash is a retry. Equal revision
with another status conflicts. This cannot be mechanically applied to an
increment-balance command with a different contract.

A intentionally applies every new network invocation in arrival order. Its
Invocation ledger recovers an interrupted runner after commit, while preserving
deliberate duplicate event IDs in separate invocations. Otherwise the laboratory
would hide the very defect it is meant to demonstrate.

## Reproducibility

A case stores envelopes, order, allowed faults, seed, virtual starting time,
A/B expectations, and checksum. Each run gets its own UUID namespace. A
deterministic invocation ID identifies already committed calls. Receiver file
versions are checked before resuming; cancellation is checked between steps.

Reports compare status, latest revision, and duplicate effects. The normalized
result excludes random run IDs and network duration. Actual timeouts/durations
remain available separately; bit-for-bit network timing is not promised. Each
case has one source/subject and cannot restore arbitrary external services.

## Local security boundaries

| Boundary | Implementation |
| --- | --- |
| Login | Random owner key, peppered hash, lookup prefix, constant-time comparison |
| Authorization | Project + scopes + source; role-limited delegation, expiry, revocation |
| Browser | HttpOnly/SameSite cookie, Origin/CSRF, CSP, text rendering, no key in localStorage |
| Receivers | Only lab-a/lab-b; no JSON-supplied URLs; HMAC binds project/destination/body/time |
| Replay | Prepared handlers and fault types; no uploaded code |
| Databases | Separate runtime roles/databases; no migration-role credentials in runtime |
| Containers | Owner non-root UID/GID, readonly, tmpfs, dropped capabilities, memory limits, no docker.sock |
| Network | Internal core/lab networks; only 127.0.0.1:8100 is exposed |
| Secrets | Private .env/.local, excluded from builds; keys and bodies are not logged |

Laboratory containers have no internet egress, core secrets, or core-database
access through their own credentials. They share an internal network with the
controller; this is not a separate machine or protection from kernel escape.
The API has a front network for its loopback port. Core processes are trusted.

Demo HTTP disables secure cookies and has no MFA/SSO. Non-demo APP_MODE is rejected.
Public hosting needs a separate TLS, edge limiting, identity, egress, and deployment review.

## Deliberate simplifications

Responsibilities are grouped in app/services/worker/replay/security rather than
many one-file services. Real databases, broker, and HTTP receivers remain.
Protected `/v1/metrics` and a Collector debug exporter replace a full
Prometheus/Grafana installation. Search covers ID/source/subject with a signed
cursor. Prepared cases, capture, and validated JSON import replace a free-form
graph editor. The separate booking outbox adapter is disabled by default.
