package store

import (
	"context"
	"errors"
	"testing"
)

func TestControlCheckedCancellationAtMutationBoundaryNoSave(t *testing.T) {
	for _, before := range []bool{false, true} {
		backend := &leaseControlBackend{memoryControlDeltaBackend: memoryControlDeltaBackend{data: NewSnapshot()}}
		st := NewWithBackend(backend)
		ctx, cancel := context.WithCancel(testCtx)
		if before {
			cancel()
		}
		calls := 0
		err := st.UpdateControlChecked(ctx, func(data *Snapshot) (bool, error) {
			calls++
			data.ExportLeases["job"] = ExportLease{Owner: "not-committed"}
			cancel()
			return true, nil
		})
		cancel()
		if !errors.Is(err, context.Canceled) || backend.saves != 0 || len(backend.data.ExportLeases) != 0 || (before && calls != 0) {
			t.Fatal("cancellation before Save published control mutation")
		}
	}
}
