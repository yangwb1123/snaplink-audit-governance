package service

import "context"

// testCtx is the shared context for service-layer tests. The default HMAC
// signer is deterministic and ignores the context, so threading it in is
// behavior-preserving; context-aware tests pass their own cancellable
// contexts explicitly.
var testCtx = context.Background()
