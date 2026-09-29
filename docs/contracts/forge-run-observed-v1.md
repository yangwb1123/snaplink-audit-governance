# Forge run observer receiver fixture

Catalyst Forge's receiver-side value images live in their own package,
[`test/contract/forge`](../../test/contract/forge), not in
`internal/domain`: they are contracts for another product's wire values, they
carry no audit-governance behaviour, and keeping them beside the domain model
made the model package 8x its own size in test code. This repository keeps a
local copy of `forge.run.observed.v1` at
[`test/contract/forge/testdata/forge-run-observed-v1.json`](../../test/contract/forge/testdata/forge-run-observed-v1.json).
The contract tests decode it with unknown fields rejected and require the
exact top-level and authority key sets.

The fixture carries bounded run metadata and an opaque owner reference. It
contains no prompt, result, tool, artifact, credential, or raw owner value.
All eight authority flags are explicitly `false`. The tests only validate the
receiver mapping; they do not register a live schema, append an event, grant
permissions, create leases, place work, or dispatch execution.

Three of the checks build a real `domain.Event` and assert that
`ValidateBasic` accepts the receiver mapping, so those files import
`internal/domain`; the other 47 use only the standard library and their own
value types.
