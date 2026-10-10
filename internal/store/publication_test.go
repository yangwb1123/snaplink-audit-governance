package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func publicationTestEvent(tenantID, eventID string) domain.Event {
	return domain.Event{TenantID: tenantID, EventID: eventID, StreamID: tenantID + ":source:crm", Sequence: 1, Hash: "ledger-hash"}
}

func publicationTestReceipt(event domain.Event) domain.EventReceipt {
	return domain.EventReceipt{TenantID: event.TenantID, EventID: event.EventID, StreamID: event.StreamID, Sequence: event.Sequence, Hash: event.Hash}
}

func TestPublicationConfirmationIdentityAndIsolation(t *testing.T) {
	for _, mode := range []string{"memory", "file"} {
		t.Run(mode, func(t *testing.T) {
			path := ""
			if mode == "file" {
				path = filepath.Join(t.TempDir(), "state.json")
			}
			st, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			event := publicationTestEvent("tenant-a", "event-same-id")
			receipt := publicationTestReceipt(event)
			if got, err := st.PublicationConfirmed(context.Background(), receipt); err != nil || got {
				t.Fatalf("missing confirmation=%v err=%v", got, err)
			}
			if err := st.ConfirmPublication(context.Background(), event); err != nil {
				t.Fatal(err)
			}
			if err := st.ConfirmPublication(context.Background(), event); err != nil {
				t.Fatalf("idempotent confirmation: %v", err)
			}
			if got, err := st.PublicationConfirmed(context.Background(), receipt); err != nil || !got {
				t.Fatalf("confirmation=%v err=%v", got, err)
			}
			other := publicationTestEvent("tenant-b", event.EventID)
			if got, err := st.PublicationConfirmed(context.Background(), publicationTestReceipt(other)); err != nil || got {
				t.Fatalf("cross-tenant confirmation=%v err=%v", got, err)
			}
			for _, mutate := range []func(*domain.Event){
				func(e *domain.Event) { e.Hash = "different" },
				func(e *domain.Event) { e.StreamID = "tenant-a:other" },
				func(e *domain.Event) { e.Sequence++ },
			} {
				changed := event
				mutate(&changed)
				if err := st.ConfirmPublication(context.Background(), changed); !errors.Is(err, ErrPublicationConflict) {
					t.Fatalf("changed confirmation err=%v", err)
				}
				if _, err := st.PublicationConfirmed(context.Background(), publicationTestReceipt(changed)); !errors.Is(err, ErrPublicationConflict) {
					t.Fatalf("changed read err=%v", err)
				}
			}
		})
	}
}

func TestPublicationConfirmationSurvivesRestartOutsideControl(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	// Confirming a publication must never write the control document.
	st, err := OpenWithControlWriteFault(path, func() error { return errors.New("control forbidden") })
	if err != nil {
		t.Fatal(err)
	}
	event := publicationTestEvent("tenant-a", "confirmed-before-cleanup")
	if err := st.ConfirmPublication(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	want := publicationFromReceipt(publicationTestReceipt(event))
	first, found, err := st.readPublication(context.Background(), want)
	if err != nil || !found {
		t.Fatalf("confirmation found=%v err=%v", found, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.ConfirmPublication(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	second, found, err := reopened.readPublication(context.Background(), want)
	if err != nil || !found || first != second {
		t.Fatalf("confirmation changed after restart/retry: first=%+v second=%+v err=%v", first, second, err)
	}
	if err := reopened.ReadControl(context.Background(), func(data *Snapshot) error {
		if len(data.Events) != 0 || len(data.Receipts) != 0 || len(data.LedgeredOutbox) != 0 {
			t.Fatalf("confirmation polluted control: %+v", data)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPublicationReaderUsesTenantClosureAndRejectsCorruption(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	event := publicationTestEvent("tenant-a", "tenant-bound-reader")
	if err := st.ConfirmPublication(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := st.ReadTenant(context.Background(), "tenant-a", func(view *TenantView) error {
		got, err := view.PublicationConfirmed(publicationTestReceipt(event))
		if err != nil || !got {
			t.Fatalf("closure confirmation=%v err=%v", got, err)
		}
		other := publicationTestReceipt(event)
		other.TenantID = "tenant-b"
		if _, err := view.PublicationConfirmed(other); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("closure cross-tenant read err=%v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	path := st.publicationPath(publicationFromReceipt(publicationTestReceipt(event)))
	if err := os.WriteFile(path, []byte(`{"tenant_id":"tenant-a"}`), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PublicationConfirmed(context.Background(), publicationTestReceipt(event)); !errors.Is(err, ErrPublicationCorrupt) {
		t.Fatalf("corrupt read err=%v", err)
	}
	if err := st.ConfirmPublication(context.Background(), event); !errors.Is(err, ErrPublicationCorrupt) {
		t.Fatalf("corrupt metadata must not be overwritten, err=%v", err)
	}
}

func TestVisibleConfirmationRequiresAncestorDurability(t *testing.T) {
	root := t.TempDir()
	st, err := Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	failure := errors.New("publication parent sync unavailable")
	armed := true
	seen := map[string]bool{}
	st.publicationSyncDir = func(dir string) error {
		seen[dir] = true
		if armed && dir == filepath.Join(root, "publications") {
			return failure
		}
		return nil
	}
	event := publicationTestEvent("tenant-a", "durable-directory-chain")
	if err := st.ConfirmPublication(context.Background(), event); !errors.Is(err, failure) {
		t.Fatalf("failed ancestor fsync err=%v", err)
	}
	// The renamed file is complete and visible, but visibility is not enough
	// to remove the outbox: a failed directory boundary still needs repair.
	if _, err := os.Stat(st.publicationPath(publicationFromReceipt(publicationTestReceipt(event)))); err != nil {
		t.Fatal("fault must occur after rename", err)
	}
	if got, err := st.PublicationConfirmed(context.Background(), publicationTestReceipt(event)); got || !errors.Is(err, failure) {
		t.Fatalf("visible but unsynced confirmation=%v err=%v", got, err)
	}
	armed = false
	if got, err := st.PublicationConfirmed(context.Background(), publicationTestReceipt(event)); err != nil || !got {
		t.Fatalf("directory repair confirmation=%v err=%v", got, err)
	}
	for _, dir := range []string{root, filepath.Join(root, "publications"), filepath.Join(root, "publications", tenantFileName("tenant-a")), filepath.Dir(st.publicationPath(publicationFromReceipt(publicationTestReceipt(event))))} {
		if !seen[dir] {
			t.Fatalf("confirmation did not fsync ancestor %s", dir)
		}
	}
}

func TestPublicationCancellationAndInvalidCoordinates(t *testing.T) {
	st, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	event := publicationTestEvent("tenant-a", "cancelled-confirmation")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := st.ConfirmPublication(ctx, event); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled confirmation err=%v", err)
	}
	if got, err := st.PublicationConfirmed(context.Background(), publicationTestReceipt(event)); err != nil || got {
		t.Fatalf("cancelled write was persisted=%v err=%v", got, err)
	}
	event.Sequence = 0
	if err := st.ConfirmPublication(context.Background(), event); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("unledgered confirmation err=%v", err)
	}
}

func TestPostgresPublicationConfirmationSharedByReplicas(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "007_publication_confirmation.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(raw)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`TRUNCATE audit_publication_confirmation`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP TABLE IF EXISTS audit_publication_confirmation`) })
	one, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	two, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := one.CheckPublicationStorage(context.Background()); err != nil {
		t.Fatal(err)
	}
	event := publicationTestEvent("tenant-a", "two-replicas")
	if err := one.ConfirmPublication(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if got, err := two.PublicationConfirmed(context.Background(), publicationTestReceipt(event)); err != nil || !got {
		t.Fatalf("shared confirmation=%v err=%v", got, err)
	}
	if err := two.ConfirmPublication(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	event.Hash = "different"
	if err := two.ConfirmPublication(context.Background(), event); !errors.Is(err, ErrPublicationConflict) {
		t.Fatalf("replica conflict err=%v", err)
	}
	if _, err := db.Exec(`DROP TABLE audit_publication_confirmation`); err != nil {
		t.Fatal(err)
	}
	if err := one.CheckPublicationStorage(context.Background()); err == nil {
		t.Fatal("missing publication catalog must fail readiness")
	}
}
