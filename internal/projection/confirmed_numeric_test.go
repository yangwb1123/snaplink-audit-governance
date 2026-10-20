package projection

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestConfirmedInsertRejectsNonpositiveSequenceBeforeIO(t *testing.T) {
	for _, sequence := range []int64{math.MinInt64, -1, 0} {
		// Nil DB is a boundary probe: even an unencodable envelope must not
		// reach encoding or a DB call when its signed sequence is invalid.
		store := &Store{}
		event := domain.Event{Sequence: sequence, Payload: map[string]any{"invalid": func() {}}}
		if err := store.insertConfirmed(context.Background(), event); !errors.Is(err, ErrNotLedgered) {
			t.Fatalf("sequence=%d err=%v", sequence, err)
		}
	}
}
