package httpapi

import (
	"math"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
)

func TestLatencyIgnoresNegativeSamplesWithoutUnsignedWrap(t *testing.T) {
	server := NewServer(nil, auth.Authenticator{}, nil)
	server.recordLatency(time.Duration(math.MinInt64))
	server.recordLatency(-time.Nanosecond)
	if server.latencyTotal.Load() != 0 || server.latencySumNanos.Load() != 0 {
		t.Fatal("invalid negative sample affected the histogram")
	}
	server.recordLatency(0)
	server.recordLatency(5 * time.Millisecond)
	if server.latencyTotal.Load() != 2 || server.latencySumNanos.Load() != uint64(5*time.Millisecond) {
		t.Fatal("valid durations did not retain histogram semantics")
	}
	if server.latencyCounts[0].Load() != 1 || server.latencyCounts[1].Load() != 2 {
		t.Fatal("valid cumulative bucket counts changed")
	}
}
