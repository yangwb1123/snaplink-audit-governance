package store

import "context"

// testCtx is the shared store-operation context for store tests. The store
// only reads it to derive a bounded operation context, so a plain background
// context preserves the pre-deadline behaviour; context-sensitive tests pass
// their own cancellable or expiring context explicitly.
var testCtx = context.Background()
