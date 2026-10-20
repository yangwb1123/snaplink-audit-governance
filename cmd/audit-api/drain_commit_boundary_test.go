package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/store"
)

// Keep the old mutex-holder's actual full-file Save, but in preparation rather
// than between force-stop and the first bounded state read. The measured new
// ingest still performs all its original hot/ledger/receipt writes and fsyncs.
func prepareForcedCommitState(t *testing.T, st *store.Store) {
	t.Helper()
	before, err := st.Snapshot(testCtx)
	if err != nil {
		t.Fatal("could not capture original commit preparation state")
	}
	if err := st.Update(testCtx, func(*store.Snapshot) error { return nil }); err != nil {
		t.Fatal("original fixture Save did not complete; result may be unknown")
	}
	after, err := st.Snapshot(testCtx)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("preparation changed original logical evidence or governance facts")
	}
}

func TestForcedCommitPreparationRetainsRealFileSaveAndOriginalState(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal("could not open original file preparation")
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := seededDrainService(t, st)
	client := startIngestClientOnly(t, svc)
	if receipt, err := client.Write(devAuthContext(), &auditv1.WriteRequest{Event: drainTestEvent("commit-preparation")}); err != nil || receipt.GetDuplicate() {
		t.Fatal("preparation oracle did not make a real original authenticated Write")
	}
	images := map[string]os.FileInfo{}
	for _, file := range []string{path, filepath.Join(root, "tenants", "demo.json"), filepath.Join(root, "ledger", "demo.jsonl")} {
		info, err := os.Stat(file)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatal("preparation oracle lost its real control/hot/cold files")
		}
		images[file] = info
	}
	prepareForcedCommitState(t, st)
	for file, before := range images {
		after, err := os.Stat(file)
		if err != nil || os.SameFile(before, after) {
			t.Fatal("fixture optimization removed an original durable file Save")
		}
	}
	if receipts, events, receiptFound, eventFound := drainSnapshotCounts(t, st, "commit-preparation"); receipts != 1 || events != 1 || !receiptFound || !eventFound {
		t.Fatal("prepared Save changed the original receipt/event sample")
	}
	t.Log("commit_preparation actual_replaced_files=3 logical_state_unchanged=true")
}
