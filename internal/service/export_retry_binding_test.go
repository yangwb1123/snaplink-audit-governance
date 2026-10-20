package service

import (
	"errors"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestRetryExportRejectsInvalidTerminalAndSuccessorBindings(t *testing.T) {
	for _, poison := range []string{"unfinished", "live-lease", "wrong-key", "wrong-tenant", "wrong-requester", "changed-filter", "self-link", "two-children"} {
		t.Run(poison, func(t *testing.T) {
			svc := testService(t, true)
			pauseExportDispatch(svc)
			source := seedActionExport(t, svc, "poisoned-retry", "failed")
			if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
				child := domain.ExportJob{ID: "child", TenantID: source.TenantID, RequestedBy: source.RequestedBy, RetryOf: source.ID, Query: source.Query, Status: "pending"}
				switch poison {
				case "unfinished":
					source.FinishedAt = nil
					data.Exports[source.ID] = source
					return nil
				case "live-lease":
					data.ExportLeases[source.ID] = store.ExportLease{Owner: "unknown-owner"}
					return nil
				case "wrong-key":
					child.ID = "different-key"
				case "wrong-tenant":
					child.TenantID = "tenant-b"
				case "wrong-requester":
					child.RequestedBy = "other"
				case "changed-filter":
					child.Query.ActorID = "other-filter"
				case "self-link":
					source.RetryOf = source.ID
					data.Exports[source.ID] = source
					return nil
				case "two-children":
					second := child
					second.ID = "second-child"
					data.Exports[second.ID] = second
				}
				data.Exports["child"] = child
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			count := len(leaseSnapshot(t, svc).Exports)
			if result, err := svc.RetryExport(testCtx, "tenant-a", "operator", source.ID); !errors.Is(err, domain.ErrConflict) || result.ID != "" {
				t.Fatal("bad retry state inferred into a successor", err)
			}
			data := leaseSnapshot(t, svc)
			if len(data.Exports) != count {
				t.Fatal("binding conflict changed jobs")
			}
			for _, fact := range data.AdminActions {
				if fact.Action == domain.AdminActionExportRetried {
					t.Fatal("binding conflict wrote governance success")
				}
			}
		})
	}
}
