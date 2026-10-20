package service

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

var legacyTimelineKinds = []string{"operation", "aggregate", "aggregate-replay"}

type legacyTimelineResult struct {
	events []domain.Event
	replay domain.ReplayResult
}

func readLegacyTimeline(ctx context.Context, svc *Service, kind, tenant, actor string) (legacyTimelineResult, error) {
	var result legacyTimelineResult
	var err error
	switch kind {
	case "operation":
		result.events, err = svc.OperationTimeline(ctx, tenant, actor, "replay-operation")
	case "aggregate":
		result.events, err = svc.AggregateTimeline(ctx, tenant, actor, "invoice", "inv-1")
	case "aggregate-replay":
		result.replay, err = svc.ReplayAggregate(ctx, tenant, actor, "invoice", "inv-1")
	default:
		panic("unknown test read kind")
	}
	return result, err
}

func legacyTimelineCount(result legacyTimelineResult, kind string) int {
	if kind == "aggregate-replay" {
		return result.replay.EventCount
	}
	return len(result.events)
}

func assertLegacyTimelineFact(t *testing.T, fact domain.AdminAction, kind, actor string) {
	t.Helper()
	target, id, detail := "aggregate", "inv-1", "invoice"
	if kind == "operation" {
		target, id, detail = "operation", "replay-operation", "timeline"
	} else if kind == "aggregate-replay" {
		detail = "replay"
	}
	if fact.Action != domain.AdminActionEventRead || fact.TenantID != "tenant-a" || fact.Actor != actor || fact.TargetType != target || fact.TargetID != id || fact.Detail != detail {
		t.Fatal("legacy read changed its original single read fact identity or target")
	}
}

func TestLegacyTimelineArchiveGetReleasesControlLock(t *testing.T) {
	for _, kind := range legacyTimelineKinds {
		t.Run(kind, func(t *testing.T) {
			// Real FileStore Put/fsync/Get remains the source. Unrelated
			// control/trail fsync throughput is not the mutex oracle.
			svc := queryMemoryService(t, true)
			t.Cleanup(func() { _ = svc.Store.Close() })
			var clock atomic.Int64
			clock.Store(pinnedNow.UnixNano())
			svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
			if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("legacy-source", "replay-operation", pinnedNow), domain.StatusArchived); err != nil {
				t.Fatal("could not commit the real legacy timeline source")
			}
			seedLeaseExport(t, svc, "legacy-concurrent-job", "tenant-a")
			claim, won, err := svc.claimPendingExport(testCtx, "legacy-concurrent-job")
			if err != nil || !won {
				t.Fatal("could not prepare the fenced running job")
			}
			paused := &queryPausedArchive{Store: svc.Config.Archive, entered: make(chan struct{}, 1), release: make(chan struct{})}
			svc.Config.Archive = paused
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			release := sync.OnceFunc(func() { close(paused.release) })
			done, exited := make(chan error, 1), make(chan struct{})
			var result legacyTimelineResult
			defer func() { release(); cancel(); <-exited }()
			go func() {
				defer close(exited)
				var err error
				result, err = readLegacyTimeline(ctx, svc, kind, "tenant-a", "reader")
				done <- err
			}()
			select {
			case <-paused.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("legacy timeline missed the actual file Get")
			}
			control, controlExited := make(chan error, 1), make(chan struct{})
			defer func() { cancel(); <-controlExited }()
			go func() {
				defer close(controlExited)
				clock.Store(pinnedNow.Add(ExportHeartbeatInterval).UnixNano())
				if err := svc.renewExportLease(ctx, claim); err != nil {
					control <- err
					return
				}
				if _, err := svc.CancelExport(ctx, "tenant-a", "operator", claim.ID); err != nil {
					control <- err
					return
				}
				control <- svc.Store.UpdateControl(ctx, func(data *store.Snapshot) error {
					data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 365, RetentionClass: "standard"}
					data.LegalHolds["legacy-hold"] = domain.LegalHold{ID: "legacy-hold", TenantID: "tenant-a", Name: "Concurrent Hold", CreatedAt: svc.Now()}
					return nil
				})
			}()
			select {
			case err := <-control:
				if err != nil {
					t.Fatal("control mutation failed during the actual legacy source Get")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("legacy read retained Store.mu across Get and blocked lease renewal/cancellation")
			}
			release()
			if err := <-done; err != nil || legacyTimelineCount(result, kind) != 1 {
				t.Fatal("legacy read did not resume authenticated evidence after Get release")
			}
			if kind == "aggregate-replay" {
				if result.replay.State["status"] != "paid" || result.replay.AggregateID != "inv-1" || result.replay.OperationID != "" || result.replay.TenantID != "tenant-a" {
					t.Fatal("legacy aggregate replay changed its original local fold")
				}
			} else if result.events[0].EventID != "legacy-source" || result.events[0].TenantID != "tenant-a" {
				t.Fatal("legacy timeline returned different or foreign evidence")
			}
			current := leaseSnapshot(t, svc)
			if current.Exports[claim.ID].Status != domain.ExportStatusCancelled || len(current.ExportLeases) != 0 || current.Policies["tenant-a"].ArchiveDays != 365 || current.LegalHolds["legacy-hold"].ID == "" {
				t.Fatal("legacy read overwrote concurrent terminal, lease, Hold or policy control")
			}
			facts := readFacts(t, svc)
			if len(facts) != 1 {
				t.Fatal("legacy read did not leave exactly one original read fact")
			}
			assertLegacyTimelineFact(t, facts[0], kind, "reader")
		})
	}
}

func TestLegacyTimelineAuthenticatesBeforeSelectionAndFold(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"operation", func(e *domain.Event) { e.OperationID = "hidden-operation" }},
		{"aggregate-type", func(e *domain.Event) { e.AggregateType = "hidden-type" }},
		{"aggregate-id", func(e *domain.Event) { e.AggregateID = "hidden-id" }},
		{"aggregate-version", func(e *domain.Event) { e.AggregateVersion++ }},
		{"changed-after", func(e *domain.Event) { e.ChangedFields["status"] = domain.FieldChange{After: "forged"} }},
		{"changed-delete", func(e *domain.Event) { e.ChangedFields["nested"] = domain.FieldChange{After: nil} }},
		{"occurrence", func(e *domain.Event) { e.OccurredAt = e.OccurredAt.Add(time.Hour) }},
		{"actor", func(e *domain.Event) { e.Actor.ID = "changed-actor" }},
		{"protected-content", func(e *domain.Event) { e.Payload["value"] = "invalid-protected-value" }},
		{"schema-version", func(e *domain.Event) { e.SchemaVersion++ }},
		{"source-digest", func(e *domain.Event) { e.SourceDigest = "" }},
	}
	for _, kind := range legacyTimelineKinds {
		for _, archived := range []bool{false, true} {
			mode := "hot"
			if archived {
				mode = "archived"
			}
			for _, mutation := range mutations {
				t.Run(kind+"/"+mode+"/"+mutation.name, func(t *testing.T) {
					svc, backend, _ := operationReplayFixture(t, archived)
					if archived {
						svc.Config.Archive = &timelineMutatedArchive{Store: svc.Config.Archive, mutate: mutation.mutate}
					} else if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
						key := store.EventKey("tenant-a", "replay-a")
						event := data.Events[key]
						mutation.mutate(&event)
						data.Events[key] = event
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					result, err := readLegacyTimeline(testCtx, svc, kind, "tenant-a", "reader")
					if err == nil || errors.Is(err, domain.ErrNotFound) || !reflect.DeepEqual(result, legacyTimelineResult{}) || backend.appendCalls != 0 || len(backend.trail) != 0 {
						t.Fatal("bad committed evidence became a successful/partial legacy read or was hidden as not found")
					}
				})
			}
		}
	}
}
