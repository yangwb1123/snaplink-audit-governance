# Forge run observer receiver fixture

This repository keeps a local copy of Catalyst Forge's
`forge.run.observed.v1` value at
[`internal/domain/testdata/forge-run-observed-v1.json`](../../internal/domain/testdata/forge-run-observed-v1.json).
The domain tests decode it with unknown fields rejected and require the exact
top-level and authority key sets.

The fixture carries bounded run metadata and an opaque owner reference. It
contains no prompt, result, tool, artifact, credential, or raw owner value.
All eight authority flags are explicitly `false`. The tests only validate the
receiver mapping; they do not register a live schema, append an event, grant
permissions, create leases, place work, or dispatch execution.
