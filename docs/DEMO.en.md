# Five-minute demonstration

[Русский](DEMO_RU.md) · **English**

Follow [setup](../README.en.md), then open http://127.0.0.1:8100 and sign in with
the `key` from private `.local/owner-access.txt`. Keep the file out of recordings.

1. Open **Лаборатория** (Laboratory), choose reordered/duplicate snapshots, and run
   the comparison. A should fail its business rules; B should pass.
2. Explain snapshots 1 → 3 → 2 → 2: B keeps revision 3 and records one effect per
   event ID; A demonstrates arrival-order regression and duplication.
3. Run the response-lost-after-commit case. Show that a successful commit can
   require an HTTP retry, and compare receiver states rather than status codes alone.
4. Inspect events and delivery attempts. Distinguish transport `accepted` from
   application verification and show the sanitized JSON report.
5. Open [architecture](ARCHITECTURE.en.md) and [verification](VERIFICATION.en.md).
   Explain outbox, lease fencing, bounded retries, and the laboratory's scope.

The controlled A failure is the expected result, not a failed installation.
Real HTTP, timeouts, and separate PostgreSQL receiver databases are used.
No arbitrary code/URL, real payment, or external email is executed.

## Resume entry

EventHub ReplayLab — FastAPI/PostgreSQL/RabbitMQ event delivery and reproducible
A/B incident replay. Transactional outbox, recovery, lease fencing, and
Hypothesis-generated message sequences; 42 tests exercise real database,
broker, and HTTP boundaries. Local testbed, not a hosted external delivery service.

Source: [Sasambatra/eventhub-replaylab](https://github.com/Sasambatra/eventhub-replaylab).
