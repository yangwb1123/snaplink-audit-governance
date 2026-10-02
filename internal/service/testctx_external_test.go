package service_test

import "context"

// testCtx is the shared store-operation context for the external service
// test package. Store operations derive a bounded child from it, so a plain
// background context preserves the pre-deadline behaviour; context-sensitive
// tests pass their own cancellable or expiring context explicitly.
var testCtx = context.Background()
