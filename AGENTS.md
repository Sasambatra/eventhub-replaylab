# Working on EventHub ReplayLab

Read README.md, docs/ARCHITECTURE.md and docs/VERIFICATION.md before changing
reliability or replay semantics. See CONTRIBUTING.md for local checks.

- Keep Event + delivery plan + outbox in one transaction. Broker acknowledgements
  and HTTP acceptance are separate from verified application state.
- Never hold a database transaction during HTTP, broker waits or backoff sleeps.
- Preserve event identity on transport retry; retain separate manual run history.
- Enforce project/scopes/source authorization at every API boundary.
- Preserve lease token/generation checks when changing worker recovery.
- Use only prepared receiver adapters. Do not accept arbitrary URL/code uploads.
- Test against the explicitly named test databases/vhost. Never clean product data.
- Keep secrets, local access files, dumps and personal notes outside Git/artifacts.
- Change frozen database schemas through new Alembic migrations.
- Keep reliability claims limited to what the documented tests actually verify.
- Do not install/upgrade dependencies without authorization from the task owner.
- Do not publish or alter external repositories beyond the user's requested scope.
