# Security

Only the latest development version is maintained. There is no commercial
security response SLA. This application is an isolated local demonstration,
not an Internet-facing webhook service.

## Reporting

Please do not include credentials, customer payloads or an exploitable private
instance in a public issue. Use GitHub's **Security → Report a vulnerability**
private reporting form for this repository. For ordinary reproducible bugs,
open an issue with synthetic data.

## Boundaries

- Bind the UI to loopback. Public mode is disabled; HTTPS/SSO/MFA/edge controls
  for an external deployment are outside this release.
- Store `.env`, owner access and backups privately. Backups contain the
  configuration required to decrypt destination secrets and are not encrypted.
- Receiver containers have internal-only networks and separate database roles.
  They share the host kernel; this is not a sandbox for untrusted programs.
- Only the two prepared adapters are accepted. A replay case cannot execute
  uploaded code or contact a user-supplied URL.
- Transport can repeat a request. Receiver B's idempotency guarantee is specific
  to its transaction and full-snapshot contract, not global exactly-once delivery.

See [architecture](docs/ARCHITECTURE.md) and [operations](docs/OPERATIONS.md).
