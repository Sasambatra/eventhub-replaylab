# Changelog

## 0.1.0 — 2026-10-30

First public release of the local demonstration.

- Durable snapshot ingestion, transactional delivery outbox, RabbitMQ quorum queue
  and bounded HTTP delivery with recovery, lease fencing and manual run history.
- Isolated A/B replay for reordered/duplicated snapshots and lost HTTP responses
  after commit; checkpoint recovery, cancellation, normalized reports and case capture.
- Project-scoped API keys, role/scope/source limits, sessions/CSRF and dashboard SSE.
- Hash-locked dependencies, pinned images, migrations, gauges/Collector traces,
  retention and coordinated three-database backup/restore drills.
- Optional Django booking outbox adapter and reproducible verification tools.

The release is a local demonstration. It does not introduce public hosting,
arbitrary webhook destinations, third-party code execution or production SLAs.
