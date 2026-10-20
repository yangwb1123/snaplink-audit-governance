package service

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportPreflightCurrentHoldSetFencesCreation(t *testing.T) {
	for _, surface := range []string{"create", "retry"} {
		for _, change := range []string{"new-Hold", "released-Hold", "foreign-Hold", "unrelated-policy"} {
			t.Run(surface+"/"+change, func(t *testing.T) {
				svc, _, objects, _, query := preflightEvidenceFixture(t)
				parent := seedPreflightParent(t, svc, query)
				hold, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "nonmatching", Reason: "case", Filter: domain.Query{From: query.From, To: query.To, ActorID: "nonmatching-actor"}})
				if err != nil {
					t.Fatal("could not seed preflight current Hold")
				}
				if change == "foreign-Hold" {
					if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-b", Name: "Foreign", Active: true}); err != nil {
						t.Fatal("could not seed isolated tenant")
					}
				}
				paused := &exportPreflightArchive{Store: objects, entered: make(chan struct{}), release: make(chan struct{}), deadlines: make(chan time.Time, 8)}
				svc.Config.Archive = paused
				ctx, cancel := context.WithCancel(testCtx)
				release := sync.OnceFunc(func() { close(paused.release) })
				done, exited := make(chan error, 1), make(chan struct{})
				var id string
				t.Cleanup(func() { cancel(); release(); <-exited })
				go func() {
					defer close(exited)
					var err error
					id, err = callExportPreflight(t, ctx, svc, surface, query)
					done <- err
				}()
				select {
				case <-paused.entered:
				case <-time.After(10 * time.Second):
					t.Fatal("preflight never reached its second, current-Hold Get")
				}
				firstDeadline, holdDeadline := <-paused.deadlines, <-paused.deadlines
				if firstDeadline.IsZero() || !firstDeadline.Equal(holdDeadline) {
					t.Fatal("current-Hold authentication restarted the query deadline")
				}
				controlDone := make(chan error, 1)
				go func() {
					var err error
					switch change {
					case "new-Hold", "foreign-Hold":
						tenant := "tenant-a"
						if change == "foreign-Hold" {
							tenant = "tenant-b"
						}
						_, err = svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: tenant, Name: "matching", Reason: "case"})
					case "released-Hold":
						_, err = svc.ReleaseLegalHold(testCtx, "tenant-a", hold.ID, "case-worker")
					case "unrelated-policy":
						err = svc.SetRetentionPolicy(testCtx, "case-worker", domain.RetentionPolicy{TenantID: "tenant-a", HotDays: 2, WarmDays: 3, ArchiveDays: 4, RetentionClass: "standard"})
					}
					controlDone <- err
				}()
				t.Cleanup(func() { release(); <-controlDone })
				// Leave a reusable completion token for cleanup, including fatal paths.
				select {
				case err := <-controlDone:
					controlDone <- err
					if err != nil {
						t.Fatal("control change failed during actual source Get")
					}
				case <-time.After(10 * time.Second):
					t.Fatal("current-Hold authentication retained Store.mu")
				}
				release()
				err = awaitExportAccess(t, done)
				if change == "new-Hold" || change == "released-Hold" {
					if !errors.Is(err, domain.ErrConflict) || id != "" {
						t.Fatal("changed active Hold set did not fence the new-job CAS")
					}
					assertPreflightNoCreation(t, svc, parent)
					if change == "new-Hold" {
						if id, err := callExportPreflight(t, testCtx, svc, surface, query); !errors.Is(err, domain.ErrForbidden) || id != "" {
							t.Fatal("fresh explicit attempt ignored the now-current matching Hold")
						}
					}
				} else {
					data := leaseSnapshot(t, svc)
					if err != nil || id == "" || len(data.Exports) != 2 || !reflect.DeepEqual(data.Exports[parent.ID], parent) || data.Exports[id].Status != "pending" {
						t.Fatal("unrelated control change prevented or corrupted safe job creation")
					}
					if change == "unrelated-policy" && data.Policies["tenant-a"].HotDays != 2 {
						t.Fatal("preflight saved its stale snapshot over concurrent policy")
					}
				}
			})
		}
	}
}

// A real Save boundary can reject its stale CAS after another writer changes
// Hold. The retried callback must recheck the same original observation, not
// refresh its binding and silently turn that attempt into an authorized one.
// Independent PostgreSQL CAS is additionally covered by the opt-in suite.
type preflightConflictBackend struct {
	*checkpointBackend
	armed bool
}

func (b *preflightConflictBackend) Save(ctx context.Context, data *store.Snapshot) error {
	if b.armed {
		b.mu.Lock()
		for id, job := range data.Exports {
			if _, existed := b.data.Exports[id]; !existed && job.Status == "pending" {
				b.armed = false
				b.data.LegalHolds["concurrent"] = domain.LegalHold{ID: "concurrent", TenantID: "tenant-a", Name: "case", Reason: "case"}
				b.mu.Unlock()
				return store.ErrSnapshotConflict
			}
		}
		b.mu.Unlock()
	}
	return b.checkpointBackend.Save(ctx, data)
}

func TestExportPreflightCASRetryDoesNotRefreshItsHoldObservation(t *testing.T) {
	for _, surface := range []string{"create", "retry"} {
		t.Run(surface, func(t *testing.T) {
			svc, base, _, _, query := preflightEvidenceFixture(t)
			parent := seedPreflightParent(t, svc, query)
			backend := &preflightConflictBackend{checkpointBackend: base, armed: true}
			svc.Store = store.NewWithBackend(backend)
			if id, err := callExportPreflight(t, testCtx, svc, surface, query); !errors.Is(err, domain.ErrConflict) || id != "" || backend.armed {
				t.Fatal("CAS replay ignored Hold committed after preflight or skipped actual Save boundary")
			}
			assertPreflightNoCreation(t, svc, parent)
			if _, exists := leaseSnapshot(t, svc).LegalHolds["concurrent"]; !exists {
				t.Fatal("CAS retry overwrote concurrent Hold with the old snapshot")
			}
		})
	}
}
