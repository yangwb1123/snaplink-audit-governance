package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

type preflightHTTPArchive struct {
	archive.Store
	gets   atomic.Int64
	onHold func() error
}

func (a *preflightHTTPArchive) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "events/") && a.gets.Add(1) == 2 && a.onHold != nil {
		if err := a.onHold(); err != nil {
			return nil, err
		}
	}
	return a.Store.Get(ctx, key)
}

func seedHTTPPreflight(t *testing.T, svc *service.Service) domain.Query {
	t.Helper()
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "preflight.http", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}, EncryptedFields: []string{"value"}, SearchableFields: []string{"value"}}); err != nil {
		t.Fatal("could not register exact HTTP preflight Schema")
	}
	event := domain.Event{EventID: "preflight-http-source", SourceSystem: "crm", EventType: "audit.event", SchemaID: "preflight.http", SchemaVersion: 1, OccurredAt: svc.Now().Add(10 * time.Second), Actor: domain.Actor{ID: "source-actor"}, Action: "read", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "preflight-http-idempotency", Payload: map[string]any{"resource": "invoice", "value": 10}}
	if _, err := svc.Ingest(testCtx, "tenant-a", domain.IngestPrincipal{ClientID: "crm"}, event, domain.StatusArchived); err != nil {
		t.Fatal("could not commit original HTTP preflight evidence")
	}
	query := domain.Query{From: svc.Now(), To: svc.Now().Add(time.Minute), PayloadField: "value"}
	var err error
	query.PayloadDigest, err = security.SearchDigestBound(10, svc.Config.EncryptionKey, "tenant-a", "value")
	if err != nil {
		t.Fatal("could not derive HTTP preflight predicate")
	}
	return query
}

func TestHTTPExportPreflightAuthenticationCurrentHoldAndNoStore(t *testing.T) {
	for _, surface := range []string{"create", "retry"} {
		for _, mode := range []string{"sidecar", "source-actor", "Hold-change"} {
			t.Run(surface+"/"+mode, func(t *testing.T) {
				server, svc, st := gateCostServer(t)
				t.Cleanup(func() { server.Close(); _ = st.Close() })
				query := seedHTTPPreflight(t, svc)
				at := svc.Now()
				parent := domain.ExportJob{ID: "preflight-parent", TenantID: "tenant-a", RequestedBy: "tenant-a", Query: query, Status: "failed", CreatedAt: at, FinishedAt: &at}
				if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error { data.Exports[parent.ID] = parent; return nil }); err != nil {
					t.Fatal("could not seed original terminal HTTP parent")
				}
				holdQuery := query
				if mode == "Hold-change" {
					holdQuery.PayloadField, holdQuery.PayloadDigest, holdQuery.ActorID = "", "", "nonmatching-actor"
				}
				hold, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "case", Reason: "case", Filter: holdQuery})
				if err != nil {
					t.Fatal("could not create original HTTP preflight Hold")
				}
				objects := &preflightHTTPArchive{Store: svc.Config.Archive}
				var holdChanged atomic.Bool
				if mode == "Hold-change" {
					objects.onHold = func() error {
						_, err := svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "new matching", Reason: "case"})
						holdChanged.Store(err == nil)
						return err
					}
				} else {
					objects.Store = &exportAccessHTTPArchive{Store: objects.Store, drift: mode}
				}
				svc.Config.Archive = objects
				path, body := "/api/v1/exports", ""
				if surface == "create" {
					raw, err := json.Marshal(query)
					if err != nil {
						t.Fatal("could not encode preflight body")
					}
					body = string(raw) // filter stays only in POST body, never URL/logs
				} else {
					path += "/preflight-parent/retry"
				}
				for _, credential := range []struct {
					token string
					want  int
				}{{"", http.StatusUnauthorized}, {"dev:tenant-a:viewer", http.StatusForbidden}, {"dev:platform:platform-admin", http.StatusBadRequest}} {
					status, _ := exportActionRequest(t, server.URL, path, credential.token, body)
					if status != credential.want || objects.gets.Load() != 0 {
						t.Fatal("HTTP preflight read evidence before permission/tenant selection")
					}
				}
				status, raw := exportActionRequest(t, server.URL, path+"?tenant_id=tenant-b", gateComplianceToken, body)
				wantStatus := http.StatusForbidden
				if mode == "source-actor" {
					wantStatus = http.StatusInternalServerError
				} else if mode == "Hold-change" {
					wantStatus = http.StatusConflict
				}
				var result errorEnvelope
				if status != wantStatus || json.Unmarshal(raw, &result) != nil || bytes.Contains(raw, []byte(`"query"`)) || bytes.Contains(raw, []byte(`"event_id"`)) || bytes.Contains(raw, []byte(query.PayloadDigest)) {
					t.Fatal("HTTP preflight served unsafe success, evidence or a non-contract error")
				}
				if mode == "sidecar" && (result.Error.Code != "forbidden" || result.Error.Reason != "legal_hold" || result.Error.HoldID != hold.ID) {
					t.Fatal("sidecar pollution suppressed the tenant-scoped structured Hold denial")
				}
				if mode == "source-actor" && (result.Error.Code != "internal_error" || result.Error.Message != "internal server error") {
					t.Fatal("bad current evidence exposed original diagnostics")
				}
				if mode == "Hold-change" && (result.Error.Code != "conflict" || !holdChanged.Load() || objects.gets.Load() != 2) {
					t.Fatal("HTTP preflight did not fence a real current-Hold change")
				}
				if err := st.ReadControl(testCtx, func(data *store.Snapshot) error {
					if len(data.Exports) != 1 || data.Exports[parent.ID].Status != "failed" || len(data.ExportLeases) != 0 {
						t.Fatal("HTTP preflight failure created work or reverted the terminal parent")
					}
					for _, fact := range data.AdminActions {
						if fact.Action == domain.AdminActionExportCreated || fact.Action == domain.AdminActionExportRetried {
							t.Fatal("failed HTTP preflight recorded new-job success")
						}
					}
					return nil
				}); err != nil {
					t.Fatal("could not verify HTTP preflight control outcome")
				}
			})
		}
	}
}
