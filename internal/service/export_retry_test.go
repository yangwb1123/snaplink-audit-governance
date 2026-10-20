package service

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestRetryExportNewLineageFreshCursorAndSingleSuccessor(t *testing.T) {
	svc := testService(t, true)
	other, err := New(svc.Store, svc.Config)
	if err != nil {
		t.Fatal(err)
	}
	pauseExportDispatch(svc)
	pauseExportDispatch(other)
	source := seedActionExport(t, svc, "retry-source", "failed")
	source.Query.Cursor, source.Query.ActorID = "expired-sensitive-cursor", "sensitive-selector"
	source.Error, source.ObjectPath, source.Digest, source.EventCount = "private-error", "private-object", "private-digest", 99
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error { data.Exports[source.ID] = source; return nil }); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan domain.ExportSummary, 4)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(s *Service) {
			defer wg.Done()
			<-start
			result, err := s.RetryExport(testCtx, "tenant-a", "operator", source.ID)
			if err != nil {
				t.Error(err)
				return
			}
			results <- result
		}([]*Service{svc, other}[i%2])
	}
	close(start)
	wg.Wait()
	close(results)
	if len(results) != 4 {
		t.Fatal("retry race lost responses")
	}
	child := <-results
	for result := range results {
		if result.ID != child.ID {
			t.Fatal("same parent acquired more than one successor")
		}
	}
	if child.ID == source.ID || child.RetryOf != source.ID || child.Status != "pending" || child.FailureCode != "" {
		t.Fatal("not a clean new child")
	}
	data := leaseSnapshot(t, svc)
	if len(data.Exports) != 2 || !reflect.DeepEqual(data.Exports[source.ID], source) {
		t.Fatal("retry mutated original or duplicated child")
	}
	job := data.Exports[child.ID]
	if job.Query.Cursor != "" || job.Query.ActorID != source.Query.ActorID || job.ObjectPath != "" || job.Digest != "" || job.Error != "" || job.EventCount != 0 || job.StartedAt != nil || job.FinishedAt != nil {
		t.Fatal("retry copied stale execution or old membership state")
	}
	raw, _ := json.Marshal(child)
	for _, forbidden := range []string{"private", "sensitive", "query", "digest", "event_count", "error", "lease"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal("retry summary leaked", forbidden)
		}
	}
	created, retried := 0, 0
	for _, fact := range data.AdminActions {
		if fact.TargetID != child.ID {
			continue
		}
		if fact.Actor != "operator" || fact.Detail != "retry_of="+source.ID {
			t.Fatal("lineage fact not bound")
		}
		if fact.Action == domain.AdminActionExportCreated {
			created++
		}
		if fact.Action == domain.AdminActionExportRetried {
			retried++
		}
	}
	if created != 1 || retried != 1 {
		t.Fatal("retry CAS duplicated facts")
	}
	// Repeating the original request returns its first successor, even if the
	// successor is terminal. A new attempt names that successor explicitly.
	if _, err := svc.CancelExport(testCtx, "tenant-a", "operator", child.ID); err != nil {
		t.Fatal(err)
	}
	again, err := svc.RetryExport(testCtx, "tenant-a", "operator", source.ID)
	if err != nil || again.ID != child.ID || again.Status != domain.ExportStatusCancelled {
		t.Fatal("repeat requeued/duplicated terminal child", err)
	}
	next, err := svc.RetryExport(testCtx, "tenant-a", "operator", child.ID)
	if err != nil || next.ID == child.ID || next.RetryOf != child.ID {
		t.Fatal("explicit retry chain did not create new attempt", err)
	}
}

func TestRetryExportOwnerAndQueueBudgetDoNotCreateJobs(t *testing.T) {
	for _, global := range []bool{false, true} {
		svc := testService(t, true)
		pauseExportDispatch(svc)
		source := seedActionExport(t, svc, "bounded-retry", "failed")
		for _, tc := range []struct {
			tenant, actor string
			want          error
		}{{"tenant-b", "operator", domain.ErrNotFound}, {"tenant-a", "different", domain.ErrForbidden}, {"tenant-a", "", domain.ErrInvalid}} {
			if result, err := svc.RetryExport(testCtx, tc.tenant, tc.actor, source.ID); !errors.Is(err, tc.want) || result.ID != "" {
				t.Fatal(err)
			}
		}
		count := MaxPendingExportsPerTenant
		if global {
			count = MaxPendingExports
		}
		if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
			for i := 0; i < count; i++ {
				id := newID("queued")
				tenant := "tenant-a"
				if global {
					tenant = id
				}
				data.Exports[id] = domain.ExportJob{ID: id, TenantID: tenant, Status: "pending"}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if result, err := svc.RetryExport(testCtx, "tenant-a", "operator", source.ID); !errors.Is(err, domain.ErrQuotaExceeded) || result.ID != "" {
			t.Fatal("over-budget retry did not fail closed", err)
		}
		data := leaseSnapshot(t, svc)
		if len(data.Exports) != count+1 || !reflect.DeepEqual(data.Exports[source.ID], source) {
			t.Fatal("budget failure changed jobs")
		}
		for _, fact := range data.AdminActions {
			if fact.Action == domain.AdminActionExportRetried {
				t.Fatal("rejected retry wrote governance success")
			}
		}
	}
}

func TestRetryExportHoldAndReadSelfAuditRemainAuthoritative(t *testing.T) {
	svc := testService(t, true)
	pauseExportDispatch(svc)
	at := pinnedNow.Add(10 * time.Second)
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("retry-evidence", "retry-operation", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	source := seedActionExport(t, svc, "held-retry", "failed")
	source.Query = domain.Query{From: at.Add(-time.Second), To: at.Add(time.Second), OperationID: "retry-operation"}
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error { data.Exports[source.ID] = source; return nil }); err != nil {
		t.Fatal(err)
	}
	hold, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "case", Reason: "case", Filter: domain.Query{OperationID: "retry-operation"}})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := svc.RetryExport(testCtx, "tenant-a", "operator", source.ID); !errors.Is(err, domain.ErrForbidden) || result.ID != "" {
		t.Fatal("retry bypassed Hold", err)
	}
	if len(leaseSnapshot(t, svc).Exports) != 1 {
		t.Fatal("Hold denial created child")
	}
	if _, err := svc.ReleaseLegalHold(testCtx, "tenant-a", hold.ID, "case-worker"); err != nil {
		t.Fatal(err)
	}
	child, err := svc.RetryExport(testCtx, "tenant-a", "operator", source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", CreatedBy: "case-worker", Name: "new case", Reason: "case", Filter: source.Query}); err != nil {
		t.Fatal(err)
	}
	// Idempotent reply is just control metadata, while details still fail
	// closed under the new Hold. This cannot serve or create evidence.
	if existing, err := svc.RetryExport(testCtx, "tenant-a", "operator", source.ID); err != nil || existing.ID != child.ID {
		t.Fatal("metadata-only retry repeat was not stable", err)
	}
	if _, err := svc.GetExport(testCtx, "tenant-a", child.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatal("child detail bypassed new Hold")
	}
	facts, err := svc.ListAdminActions(testCtx, "tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	reads := 0
	for _, fact := range facts {
		if fact.Action == domain.AdminActionEventRead && fact.Actor == "operator" {
			reads++
		}
	}
	if reads != 2 {
		t.Fatalf("new/blocked attempt read audit=%d, want 2; repeat must not read evidence", reads)
	}
}

type retryFaultBackend struct {
	*scriptedConflictBackend
	failChild bool
	unknown   bool
	primary   error
}

func (b *retryFaultBackend) Save(ctx context.Context, data *store.Snapshot) error {
	for _, job := range data.Exports {
		if job.RetryOf != "" && b.failChild {
			if b.unknown {
				if err := b.scriptedConflictBackend.Save(ctx, data); err != nil {
					return err
				}
			}
			return b.primary
		}
	}
	return b.scriptedConflictBackend.Save(ctx, data)
}

func TestRetryExportFailedAndUnknownSavesReturnNoPartialResult(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		backend := &retryFaultBackend{scriptedConflictBackend: &scriptedConflictBackend{data: store.NewSnapshot()}, primary: errors.New("injected child Save failure"), unknown: unknown}
		svc := newServiceOn(t, store.NewWithBackend(backend))
		seedTestDomain(t, svc)
		pauseExportDispatch(svc)
		source := seedActionExport(t, svc, "retry-save", "failed")
		backend.failChild = true
		if result, err := svc.RetryExport(testCtx, "tenant-a", "operator", source.ID); !errors.Is(err, backend.primary) || result.ID != "" {
			t.Fatal("unknown/failed Save served success")
		}
		backend.failChild = false
		result, err := svc.RetryExport(testCtx, "tenant-a", "operator", source.ID)
		if err != nil || result.ID == "" {
			t.Fatal(err)
		}
		data := leaseSnapshot(t, svc)
		if len(data.Exports) != 2 || !reflect.DeepEqual(data.Exports[source.ID], source) {
			t.Fatal("unknown Save duplicated attempt or changed source")
		}
		retries := 0
		for _, fact := range data.AdminActions {
			if fact.Action == domain.AdminActionExportRetried {
				retries++
			}
		}
		if retries != 1 {
			t.Fatal("unknown Save repeated governance fact")
		}
	}
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	pauseExportDispatch(svc)
	source := seedActionExport(t, svc, "read-fail-retry", "failed")
	backend.saveErr = errors.New("read self-audit failed")
	if result, err := svc.RetryExport(testCtx, "tenant-a", "operator", source.ID); err == nil || result.ID != "" {
		t.Fatal("failed read self-audit allowed retry")
	}
	backend.saveErr = nil
	if len(leaseSnapshot(t, svc).Exports) != 1 {
		t.Fatal("failed read audit created job")
	}
}
