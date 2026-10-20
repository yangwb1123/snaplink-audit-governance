package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

type exportAccessHTTPArchive struct {
	archive.Store
	drift     string
	onSealed  func()
	once      sync.Once
	sourceGet atomic.Int64
	sealedGet atomic.Int64
}

func (a *exportAccessHTTPArchive) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "exports/") {
		a.sealedGet.Add(1)
		if a.onSealed != nil {
			a.once.Do(a.onSealed)
		}
		return a.Store.Get(ctx, key)
	}
	raw, err := a.Store.Get(ctx, key)
	if err != nil || !strings.HasPrefix(key, "events/") {
		return raw, err
	}
	a.sourceGet.Add(1)
	var source domain.Event
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&source); err != nil {
		return nil, err
	}
	if a.drift == "sidecar" {
		delete(source.Payload, "value"+security.SearchDigestSuffix)
	} else {
		source.Actor.ID = "masked-actor"
	}
	return domain.CanonicalJSON(source)
}

func seedHTTPAccessEvidence(t *testing.T, svc *service.Service) domain.ExportJob {
	t.Helper()
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "access.hold", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}, EncryptedFields: []string{"value"}, SearchableFields: []string{"value"}}); err != nil {
		t.Fatal("could not register exact HTTP access Schema")
	}
	at := svc.Now().Add(10 * time.Second)
	event := domain.Event{EventID: "access-source", SourceSystem: "crm", EventType: "audit.event", SchemaID: "access.hold", SchemaVersion: 1, OccurredAt: at, Actor: domain.Actor{ID: "source-actor"}, Action: "read", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "access-idempotency", Payload: map[string]any{"resource": "invoice", "value": 10}}
	if _, err := svc.Ingest(testCtx, "tenant-a", domain.IngestPrincipal{ClientID: "crm"}, event, domain.StatusArchived); err != nil {
		t.Fatal("could not commit HTTP access evidence")
	}
	job, err := svc.CreateExport(testCtx, "tenant-a", "compliance", domain.Query{From: svc.Now(), To: svc.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal("could not create real HTTP export")
	}
	guard := time.NewTimer(30 * time.Second)
	defer guard.Stop()
	for {
		if err := svc.Store.ReadControl(testCtx, func(data *store.Snapshot) error { job = data.Exports[job.ID]; return nil }); err != nil {
			t.Fatal("could not observe HTTP export completion")
		}
		if job.Status == "completed" {
			return job
		}
		if job.Status == "failed" {
			t.Fatal("HTTP access fixture failed to produce a sealed export")
		}
		select {
		case <-guard.C:
			t.Fatal("HTTP fixture did not reach its real terminal condition")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestHTTPExportAccessAuthenticatesCurrentHoldBeforeServing(t *testing.T) {
	for _, drift := range []string{"sidecar", "source-actor"} {
		for _, stage := range []string{"before-detail", "before-download", "during-sealed-Get"} {
			t.Run(drift+"/"+stage, func(t *testing.T) {
				server, svc, st := gateCostServer(t)
				job := seedHTTPAccessEvidence(t, svc)
				objects := &exportAccessHTTPArchive{Store: svc.Config.Archive, drift: drift}
				var hold domain.LegalHold
				var holdErr error
				createHold := func() {
					query := domain.Query{From: svc.Now(), To: svc.Now().Add(time.Minute), PayloadField: "value"}
					query.PayloadDigest, holdErr = security.SearchDigestBound(10, svc.Config.EncryptionKey, "tenant-a", "value")
					if holdErr != nil {
						return
					}
					hold, holdErr = svc.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "case", Reason: "case", Filter: query})
				}
				if stage == "during-sealed-Get" {
					objects.onSealed = createHold
				} else {
					createHold()
				}
				svc.Config.Archive = objects
				path := "/api/v1/exports/" + job.ID
				if stage != "before-detail" {
					path += "/download"
				}
				request, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
				if err != nil {
					t.Fatal("could not build HTTP access request")
				}
				request.Header.Set("Authorization", "Bearer "+gateComplianceToken)
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					t.Fatal("HTTP access request did not return a response")
				}
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				if err != nil || holdErr != nil || hold.ID == "" {
					t.Fatal("could not observe current Hold HTTP boundary")
				}
				wantStatus := http.StatusForbidden
				if drift == "source-actor" {
					wantStatus = http.StatusInternalServerError
				}
				if response.StatusCode != wantStatus || response.Header.Get("Cache-Control") != "no-store" || strings.Contains(response.Header.Get("Content-Type"), "x-ndjson") || bytes.Contains(body, []byte(`"event_id"`)) || bytes.Contains(body, []byte(`"query"`)) {
					t.Fatal("held or unauthenticated evidence escaped through HTTP access")
				}
				var denied errorEnvelope
				if json.Unmarshal(body, &denied) != nil {
					t.Fatal("access failure did not return the fixed error envelope")
				}
				if drift == "sidecar" && (denied.Error.Code != "forbidden" || denied.Error.Reason != "legal_hold" || denied.Error.HoldID != hold.ID) {
					t.Fatal("sidecar pollution suppressed the structured current Hold denial")
				}
				if drift == "source-actor" && (denied.Error.Code != "internal_error" || denied.Error.Message != "internal server error") {
					t.Fatal("unauthenticated source failure exposed internal diagnostics")
				}
				if objects.sourceGet.Load() == 0 || (stage == "during-sealed-Get" && objects.sealedGet.Load() != 1) || (stage != "during-sealed-Get" && objects.sealedGet.Load() != 0) {
					t.Fatal("HTTP oracle did not reach the intended source/sealed-object boundary")
				}
				var current domain.ExportJob
				if err := st.ReadControl(testCtx, func(data *store.Snapshot) error { current = data.Exports[job.ID]; return nil }); err != nil || !reflect.DeepEqual(current, job) {
					t.Fatal("HTTP access changed a terminal export")
				}
				facts, err := svc.ListAdminActions(testCtx, "tenant-a", false, 100)
				if err != nil {
					t.Fatal("could not read HTTP attempt facts")
				}
				for _, fact := range facts {
					if fact.Action == domain.AdminActionEventExport {
						t.Fatal("denied HTTP access recorded download success")
					}
				}
			})
		}
	}
}

func TestHTTPExportAccessResponsesAreNoStoreIncludingAuthorizationFailures(t *testing.T) {
	server, svc, _ := gateCostServer(t)
	job := seedHTTPAccessEvidence(t, svc)
	for _, suffix := range []string{"", "/download"} {
		for _, credential := range []struct {
			token  string
			status int
		}{{"", http.StatusUnauthorized}, {"dev:tenant-a:viewer", http.StatusForbidden}, {"dev:tenant-b:compliance", http.StatusNotFound}, {gateComplianceToken, http.StatusOK}} {
			request, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/exports/"+job.ID+suffix, nil)
			if err != nil {
				t.Fatal("could not build no-store request")
			}
			if credential.token != "" {
				request.Header.Set("Authorization", "Bearer "+credential.token)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal("could not observe no-store response")
			}
			_, readErr := io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if readErr != nil || response.StatusCode != credential.status || response.Header.Get("Cache-Control") != "no-store" {
				t.Fatal("export access response was cacheable or changed authorization")
			}
		}
	}
}
