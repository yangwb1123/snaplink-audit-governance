package httpapi

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type batchDeadlineBackend struct {
	*scriptedStoreBackend
	calls atomic.Int64
}

func (b *batchDeadlineBackend) Save(ctx context.Context, _ *store.Snapshot) error {
	b.calls.Add(1)
	<-ctx.Done() // the real Save boundary, under the unchanged default budget
	return ctx.Err()
}

func TestHTTPBatchCountCapAndStoreDeadlineAreDifferentBoundaries(t *testing.T) {
	base := &scriptedStoreBackend{data: store.NewSnapshot()}
	server, svc := testHTTPServerWithBackend(t, base)
	defer server.Close()
	svc.Config.Archive = newMemoryArchive()
	blocked := &batchDeadlineBackend{scriptedStoreBackend: base}
	svc.Store = store.NewWithBackend(blocked) // original default timeout, not an extended test SLA
	defer svc.Store.Close()
	events := make([]domain.Event, domain.MaxBatchEvents+1)
	for i := range events {
		events[i] = httpCapEvent("http-save-boundary", i)
	}
	status, response := postHTTPBatch(t, server.URL, events)
	if status != http.StatusBadRequest || response.Error.Code != "invalid_request" || len(response.Receipts) != 0 || blocked.calls.Load() != 0 {
		t.Fatal("over-count request reached the blocked Save instead of count preflight")
	}
	// Count admission is not execution success. An exact-boundary request
	// whose first real Save exhausts its original deadline must remain 503;
	// never make the cap test accept/ignore/retry that production failure.
	status, response = postHTTPBatch(t, server.URL, events[:domain.MaxBatchEvents])
	if status != http.StatusServiceUnavailable || response.Error.Code != "store_timeout" || blocked.calls.Load() != 1 || svc.Store.OperationDeadlineExhaustions() != 1 {
		t.Fatal("a real blocked Save lost Store timeout classification or bypassed persistence")
	}
	for _, receipt := range response.Receipts {
		if receipt.EventID != "" || receipt.Hash != "" {
			t.Fatal("failed first Save produced a successful batch receipt")
		}
	}
	assertHTTPBatchEventsAbsent(t, svc.Store, events)
}
