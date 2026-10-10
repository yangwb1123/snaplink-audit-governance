package service

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func seedWorkLists(t *testing.T, svc *Service) time.Time {
	t.Helper()
	at := time.Unix(1_700_000_000, 0).UTC()
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		for index, id := range []string{"export-a", "export-b", "export-c"} {
			owner := "alice"
			if index == 1 {
				owner = "bob"
			}
			data.Exports[id] = domain.ExportJob{ID: id, TenantID: "tenant-a", RequestedBy: owner, Status: "completed", CreatedAt: at, ObjectPath: "/private/archive", Digest: "private-digest", EventCount: 42, Error: "/private/backend/error", Query: domain.Query{OperationID: "op-1"}}
			data.RestoreRuns["restore-"+id] = domain.RestoreRun{ID: "restore-" + id, TenantID: "tenant-a", CreatedBy: owner, Status: domain.RestoreStatusPendingApproval, CreatedAt: at, OperationID: "op-1", Reason: "review"}
		}
		data.Exports["foreign"] = domain.ExportJob{ID: "foreign", TenantID: "tenant-b", Status: "completed", CreatedAt: at.Add(time.Second)}
		data.RestoreRuns["foreign"] = domain.RestoreRun{ID: "foreign", TenantID: "tenant-b", CreatedBy: "bob", Status: domain.RestoreStatusPendingApproval, CreatedAt: at}
		data.RestoreRuns["decided"] = domain.RestoreRun{ID: "decided", TenantID: "tenant-a", CreatedBy: "bob", Status: domain.RestoreStatusApproved, CreatedAt: at.Add(time.Second)}
		data.RestoreRuns["legacy"] = domain.RestoreRun{ID: "legacy", TenantID: "tenant-a", Status: domain.RestoreStatusPendingApproval, CreatedAt: at}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return at
}

func TestExportWorkListPagingAndScopeBinding(t *testing.T) {
	svc := testService(t, false)
	seedWorkLists(t, svc)
	query := domain.JobQuery{PageSize: 1}
	var ids []string
	for {
		page, err := svc.ListExports(testCtx, "tenant-a", "alice", query)
		if err != nil {
			t.Fatal(err)
		}
		if page.Count != 3 || len(page.Items) != 1 {
			t.Fatalf("page=%+v", page)
		}
		ids = append(ids, page.Items[0].ID)
		if page.NextCursor == "" {
			break
		}
		query.Cursor = page.NextCursor
	}
	if !reflect.DeepEqual(ids, []string{"export-c", "export-b", "export-a"}) {
		t.Fatalf("ids=%v", ids)
	}
	first, err := svc.ListExports(testCtx, "tenant-a", "alice", domain.JobQuery{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		tenant string
		actor  string
		query  domain.JobQuery
	}{
		{"tenant-b", "alice", domain.JobQuery{Cursor: first.NextCursor}},
		{"tenant-a", "bob", domain.JobQuery{Cursor: first.NextCursor}},
		{"tenant-a", "alice", domain.JobQuery{Cursor: first.NextCursor, View: "mine"}},
		{"tenant-a", "alice", domain.JobQuery{Cursor: first.NextCursor, Status: "failed"}},
		{"tenant-a", "alice", domain.JobQuery{Cursor: first.NextCursor + "tampered"}},
		{"tenant-a", "alice", domain.JobQuery{Cursor: strings.Repeat("x", 4097)}},
	} {
		if _, err := svc.ListExports(testCtx, tc.tenant, tc.actor, tc.query); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("query=%+v tenant=%s actor=%s err=%v", tc.query, tc.tenant, tc.actor, err)
		}
	}
	if _, err := svc.ListRestores(testCtx, "tenant-a", "alice", domain.JobQuery{Cursor: first.NextCursor}); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("cross-kind cursor err=%v", err)
	}
}

func TestWorkListFiltersAndActionableQueue(t *testing.T) {
	svc := testService(t, false)
	at := seedWorkLists(t, svc)
	page, err := svc.ListExports(testCtx, "tenant-a", "alice", domain.JobQuery{View: "mine", From: at, To: at.Add(time.Second), OperationID: "op-1", Status: "completed"})
	if err != nil || page.Count != 2 {
		t.Fatalf("mine=%+v err=%v", page, err)
	}
	queue, err := svc.ListRestores(testCtx, "tenant-a", "alice", domain.JobQuery{View: "actionable"})
	if err != nil || queue.Count != 1 || queue.Items[0].CreatedBy != "bob" || queue.Items[0].Status != domain.RestoreStatusPendingApproval {
		t.Fatalf("queue=%+v err=%v", queue, err)
	}
	mine, err := svc.ListRestores(testCtx, "tenant-a", "alice", domain.JobQuery{View: "mine"})
	if err != nil || mine.Count != 2 {
		t.Fatalf("mine=%+v err=%v", mine, err)
	}
	other, err := svc.ListExports(testCtx, "tenant-b", "alice", domain.JobQuery{})
	if err != nil || other.Count != 1 || other.Items[0].ID != "foreign" {
		t.Fatalf("other=%+v err=%v", other, err)
	}
}

func TestWorkListRejectsInvalidFilters(t *testing.T) {
	svc := testService(t, false)
	at := seedWorkLists(t, svc)
	for _, query := range []domain.JobQuery{
		{Status: "pending|running"}, {Status: "approved"}, {View: "actionable"}, {PageSize: -1}, {PageSize: 1001}, {From: at, To: at},
	} {
		if _, err := svc.ListExports(testCtx, "tenant-a", "alice", query); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("query=%+v err=%v", query, err)
		}
	}
	if _, err := svc.ListExports(testCtx, "", "alice", domain.JobQuery{}); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("empty tenant err=%v", err)
	}
	if _, err := svc.ListExports(testCtx, "tenant-a", "", domain.JobQuery{}); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("empty actor err=%v", err)
	}
}
