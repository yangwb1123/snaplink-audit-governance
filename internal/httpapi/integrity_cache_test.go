package httpapi

// Cache policy is a transport property, including refusal and failure. These
// regressions do not reinterpret an invalid ledger or withdrawn verification
// as a different service result, nor do they replace the original file tests.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

const integrityCachePath = "/api/v1/integrity/verify"

type integrityCacheFixture struct {
	store   *store.Store
	service *service.Service
	api     *Server
	handler http.Handler
	server  *httptest.Server
}

func newIntegrityCacheFixture(t *testing.T) *integrityCacheFixture {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close integrity cache fixture: %v", err)
		}
	})
	svc, err := service.New(st, service.Config{AllowDevSecrets: true, Now: func() time.Time {
		return time.Unix(1_700_000_000, 0).UTC()
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"tenant-a", "tenant-b"} {
		if err := svc.CreateTenant(testCtx, "fixture", domain.Tenant{ID: id, Name: id, Active: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := svc.AddSource(testCtx, "fixture", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema(testCtx, "fixture", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	api := NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0))
	handler := api.Handler()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &integrityCacheFixture{store: st, service: svc, api: api, handler: handler, server: server}
}

func integrityCacheCall(t *testing.T, server *httptest.Server, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	for key, values := range response.Header {
		recorder.Header()[key] = append([]string(nil), values...)
	}
	recorder.WriteHeader(response.StatusCode)
	_, _ = recorder.Write(raw)
	return recorder
}

func assertIntegrityCache(t *testing.T, response *httptest.ResponseRecorder, status int) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("integrity status=%d, want %d", response.Code, status)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("integrity Cache-Control=%q, want no-store (status=%d)", got, response.Code)
	}
}

func TestHTTPIntegrityCacheValidAndCommittedCorruption(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "valid"
		if corrupt {
			name = "committed-corruption"
		}
		t.Run(name, func(t *testing.T) {
			f := newIntegrityCacheFixture(t)
			event := httpReadSeedEvent()
			raw, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			seed := integrityCacheCall(t, f.server, "/api/v1/events?wait_for=ledgered", "dev:tenant-a:service:crm", string(raw))
			if seed.Code != http.StatusAccepted {
				t.Fatalf("real file ingest status=%d", seed.Code)
			}
			if corrupt {
				if err := f.store.Update(testCtx, func(data *store.Snapshot) error {
					key := store.EventKey("tenant-a", event.EventID)
					value, ok := data.Events[key]
					if !ok {
						return errors.New("fixture requires committed hot event")
					}
					value.Payload = map[string]any{"value": 424242}
					value.SourceDigest = "cache-fixture-corruption"
					value.Hash = strings.Repeat("0", len(value.Hash))
					data.Events[key] = value
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			response := integrityCacheCall(t, f.server, integrityCachePath, "dev:tenant-a:compliance", `{}`)
			assertIntegrityCache(t, response, http.StatusOK)
			var result service.IntegrityResult
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal("integrity result must retain its documented JSON shape")
			}
			if result.Valid == corrupt || result.TenantID != "tenant-a" || result.EventCount != 1 {
				t.Fatal("cache policy changed verdict, tenant or committed event count")
			}
			if corrupt && !strings.Contains(strings.Join(result.Errors, "\n"), "content digest mismatch") {
				t.Fatal("corrupt committed evidence lost its diagnostic")
			}
			if corrupt && (f.api.integrityInvalidCount.Load() != 1 || f.api.integrityValidCount.Load() != 0) {
				t.Fatal("invalid result changed integrity metric classification")
			}
			if !corrupt && (f.api.integrityValidCount.Load() != 1 || f.api.integrityInvalidCount.Load() != 0) {
				t.Fatal("valid result changed integrity metric classification")
			}
			trail, err := f.store.ReadAdminTrail(testCtx)
			if err != nil || len(trail) != 1 || trail[0].Action != domain.AdminActionEventRead || trail[0].TargetType != "integrity" {
				t.Fatal("verification verdict must preserve its one durable read fact")
			}
		})
	}
}

func TestHTTPIntegrityCacheRefusalAndInputMatrix(t *testing.T) {
	f := newIntegrityCacheFixture(t)
	for _, test := range []struct {
		name, path, token, body string
		status                  int
	}{
		{"unauthenticated-before-body", integrityCachePath, "", `{broken`, http.StatusUnauthorized},
		{"permission-before-body", integrityCachePath, "dev:tenant-a:service:crm", `{broken`, http.StatusForbidden},
		{"bad-json", integrityCachePath, "dev:tenant-a:compliance", `{broken`, http.StatusBadRequest},
		{"bad-stream-type", integrityCachePath, "dev:tenant-a:compliance", `{"stream_id":1}`, http.StatusBadRequest},
		{"bad-stream-frame", integrityCachePath, "dev:tenant-a:compliance", `{"stream_id":"bad\u0000stream"}`, http.StatusBadRequest},
		{"long-stream", integrityCachePath, "dev:tenant-a:compliance", `{"stream_id":"` + strings.Repeat("s", 257) + `"}`, http.StatusBadRequest},
		{"trailing-json", integrityCachePath, "dev:tenant-a:compliance", `{} {}`, http.StatusBadRequest},
		{"platform-missing-tenant", integrityCachePath, "dev:platform:platform-admin", `{}`, http.StatusBadRequest},
		{"platform-empty-tenant", integrityCachePath + "?tenant_id=", "dev:platform:platform-admin", `{}`, http.StatusBadRequest},
		{"ambiguous-tenant", integrityCachePath + "?tenant_id=tenant-a&tenant_id=tenant-b", "dev:platform:platform-admin", `{}`, http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := integrityCacheCall(t, f.server, test.path, test.token, test.body)
			assertIntegrityCache(t, response, test.status)
			if bytes.Contains(response.Body.Bytes(), []byte(`"valid"`)) {
				t.Fatal("refusal must not expose a partial integrity result")
			}
		})
	}
	trail, err := f.store.ReadAdminTrail(testCtx)
	if err != nil || len(trail) != 0 || f.api.integrityValidCount.Load() != 0 || f.api.integrityInvalidCount.Load() != 0 {
		t.Fatal("refused or malformed request performed verification or appended a successful read fact")
	}
}

func TestHTTPIntegrityCacheSignedTenantAndExplicitPlatformScope(t *testing.T) {
	f := newIntegrityCacheFixture(t)
	token := mintRestoreJWT(t, "cache-reader", "tenant-a", []string{"compliance"})
	response := integrityCacheCall(t, f.server, integrityCachePath+"?tenant_id=tenant-b", token, `{}`)
	assertIntegrityCache(t, response, http.StatusOK)
	var result service.IntegrityResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.TenantID != "tenant-a" {
		t.Fatal("query selector replaced signed tenant authority")
	}
	response = integrityCacheCall(t, f.server, integrityCachePath+"?tenant_id=tenant-b", "dev:platform:platform-admin", `{}`)
	assertIntegrityCache(t, response, http.StatusOK)
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.TenantID != "tenant-b" {
		t.Fatal("platform verification ignored its explicit tenant selector")
	}
}

// decodeBody intentionally permits unknown fields. Preserve that existing
// compatibility while proving a body tenant field cannot replace signed scope.
func TestHTTPIntegrityCacheIgnoredBodyTenantCannotChangeScope(t *testing.T) {
	f := newIntegrityCacheFixture(t)
	token := mintRestoreJWT(t, "cache-reader", "tenant-a", []string{"compliance"})
	response := integrityCacheCall(t, f.server, integrityCachePath, token, `{"tenant_id":"tenant-b"}`)
	assertIntegrityCache(t, response, http.StatusOK)
	var result service.IntegrityResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.TenantID != "tenant-a" {
		t.Fatal("unknown body field replaced signed tenant authority")
	}
	trail, err := f.store.ReadAdminTrail(testCtx)
	if err != nil || len(trail) != 1 || trail[0].TenantID != "tenant-a" {
		t.Fatal("body tenant changed the verification read-fact tenant")
	}
}

type integrityCacheLoadFault struct {
	*scriptedStoreBackend
	loadErr error
}

func (b *integrityCacheLoadFault) Load(ctx context.Context) (*store.Snapshot, error) {
	if b.loadErr != nil {
		return nil, b.loadErr
	}
	return b.scriptedStoreBackend.Load(ctx)
}

func TestHTTPIntegrityCacheStorageAndSelfAuditFailure(t *testing.T) {
	for _, failure := range []string{"storage-timeout", "read-fact-save"} {
		t.Run(failure, func(t *testing.T) {
			backend := &integrityCacheLoadFault{scriptedStoreBackend: &scriptedStoreBackend{data: store.NewSnapshot()}}
			st := store.NewWithBackend(backend)
			t.Cleanup(func() { _ = st.Close() })
			svc, err := service.New(st, service.Config{AllowDevSecrets: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := svc.CreateTenant(testCtx, "fixture", domain.Tenant{ID: "tenant-a", Name: "A", Active: true}); err != nil {
				t.Fatal(err)
			}
			api := NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0))
			server := httptest.NewServer(api.Handler())
			t.Cleanup(server.Close)
			status, code := http.StatusServiceUnavailable, "store_timeout"
			before := len(backend.data.AdminActions)
			if failure == "storage-timeout" {
				backend.loadErr = store.ErrOperationDeadline
			} else {
				backend.saveErr = errors.New("private-cache-read-fact-save-failure")
				status, code = http.StatusInternalServerError, "internal_error"
			}
			response := integrityCacheCall(t, server, integrityCachePath, "dev:tenant-a:compliance", `{}`)
			assertIntegrityCache(t, response, status)
			var envelope errorEnvelope
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil || envelope.Error.Code != code {
				t.Fatal("cache hardening changed original storage/self-audit error class")
			}
			if strings.Contains(response.Body.String(), "private-cache") || bytes.Contains(response.Body.Bytes(), []byte(`"valid"`)) {
				t.Fatal("failed read exposed backend diagnostics or a success result")
			}
			if len(backend.data.AdminActions) != before || api.integrityValidCount.Load() != 0 || api.integrityInvalidCount.Load() != 0 {
				t.Fatal("failed read fact was published or counted as a completed verification")
			}
		})
	}
}

func TestHTTPIntegrityCacheCallerWithdrawalKeepsDiagnosticSemantics(t *testing.T) {
	f := newIntegrityCacheFixture(t)
	ctx, cancel := context.WithCancel(testCtx)
	cancel()
	request := httptest.NewRequest(http.MethodPost, integrityCachePath, strings.NewReader(`{}`)).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response := httptest.NewRecorder()
	f.handler.ServeHTTP(response, request)
	// Existing VerifyIntegrity deliberately reports withdrawal as an interrupted
	// diagnostic result. Do not force an unreachable 499 merely to fill a matrix.
	assertIntegrityCache(t, response, http.StatusOK)
	var result service.IntegrityResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.Valid || !strings.Contains(strings.Join(result.Errors, "\n"), "verification interrupted") {
		t.Fatal("caller withdrawal lost its existing diagnostic result")
	}
	trail, err := f.store.ReadAdminTrail(testCtx)
	if err != nil || len(trail) != 0 || f.api.integrityInvalidCount.Load() != 1 {
		t.Fatal("withdrawal changed original read-fact or metric policy")
	}
}
