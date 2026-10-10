package main

import (
	"math/rand/v2"
	"time"
)

// Positive jitter dephases replicas even in healthy rounds. Exponential
// backoff is capped at 30s, resets on a clean round, and never changes the
// durable at-least-once contract. The injectable jitter keeps tests exact.
func ledgeredFlushDelay(failures int, jitter func(int64) int64) time.Duration {
	base := ledgeredOutboxFlushInterval
	for failures > 0 && base < ledgeredOutboxMaxRetryInterval/2 {
		base *= 2
		failures--
	}
	if base > ledgeredOutboxMaxRetryInterval/2 {
		base = ledgeredOutboxMaxRetryInterval / 2
	}
	if jitter == nil {
		jitter = rand.Int64N
	}
	return base + time.Duration(jitter(int64(base)))
}
