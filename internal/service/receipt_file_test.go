package service

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type receiptForbiddenArchive struct{ archive.Store }

func (a receiptForbiddenArchive) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("unexpected source Get during receipt metadata read")
}

func TestReceiptReadFileRestartPreservesCommittedMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal("could not open owned receipt file fixture")
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := newServiceOn(t, st)
	svc.Config.Archive = &archive.FileStore{Dir: filepath.Join(filepath.Dir(path), "archive")}
	seedFileTestDomain(t, svc)
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("receipt-file", "receipt-operation", pinnedNow), domain.StatusArchived); err != nil {
		t.Fatal("could not prepare real archived receipt")
	}
	original, err := st.Snapshot(testCtx)
	if err != nil || original.LayoutVersion != 2 || len(original.Events) != 0 {
		t.Fatal("receipt file oracle requires evicted source and committed cold metadata")
	}
	if err := st.Close(); err != nil {
		t.Fatal("could not close owned receipt writer")
	}
	reopened, err := store.Open(path)
	if err != nil {
		t.Fatal("could not reopen original receipt ledger")
	}
	defer reopened.Close()
	next := newServiceOn(t, reopened)
	next.Config.Archive = receiptForbiddenArchive{Store: svc.Config.Archive}
	receipt, err := next.GetReceipt(testCtx, "tenant-a", "reader", "receipt-file")
	if err != nil || !reflect.DeepEqual(receipt, original.Receipts[store.EventKey("tenant-a", "receipt-file")]) || receipt.Status != domain.StatusArchived {
		t.Fatal("file receipt read required source Get or changed original committed metadata")
	}
	current, err := reopened.Snapshot(testCtx)
	if err != nil || !reflect.DeepEqual(current, original) || len(readFacts(t, next)) != 1 {
		t.Fatal("receipt read rewrote persisted control/cold metadata or lost its independent fact")
	}
}
