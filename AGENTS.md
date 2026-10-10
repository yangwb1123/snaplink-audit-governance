# Agent rules

- Specs: `README.md`, `docs/adr/`, `api/**`; not plans/history.
- Own audit/governance, not identity or business state. Replay has no business side effects; approval is not execution.
- Keep tenant/source isolation, fail-closed auth/self-audit, at-least-once delivery and idempotency.
- Ledger/WORM are truth; Kafka transports; ClickHouse projects.
- After code/contract changes, run `python3 cli.py quality`; no completion on failure. Web changes: also `make web-check`.
- Release: run `python3 cli.py security-scan` if `gosec`/`govulncheck` is available.
- Limits/gate: `engineering.yaml`, `docs/ENGINEERING_GATE.md`.
