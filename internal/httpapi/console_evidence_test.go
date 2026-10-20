package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
)

type consoleHTTPArchive struct {
	archive.Store
	gets    atomic.Int64
	corrupt atomic.Bool
}

func (a *consoleHTTPArchive) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "events/") {
		a.gets.Add(1)
	}
	raw, err := a.Store.Get(ctx, key)
	if err != nil || !a.corrupt.Load() || !strings.HasPrefix(key, "events/") {
		return raw, err
	}
	var event domain.Event
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&event); err != nil {
		return nil, err
	}
	event.Actor.ID = "unauthenticated-actor-drift"
	return domain.CanonicalJSON(event)
}

func TestHTTPConsoleEvidenceScopeAuthorizationAndNoStore(t *testing.T) {
	server, svc := timelineStripFixture(t)
	defer svc.Store.Close()
	defer server.Close()
	objects := &consoleHTTPArchive{Store: svc.Config.Archive}
	svc.Config.Archive = objects
	consoleToken := mintTenantlessConsoleToken(t)
	for _, path := range []string{"/api/v1/compat/snaplink/audit/events", "/api/v1/compat/snaplink/audit/facets"} {
		for _, access := range []struct {
			name, token, selector string
			status, count         int
		}{
			{"unauthenticated", "", "", http.StatusUnauthorized, 0},
			{"no-read-permission", "dev:tenant-a:viewer", "", http.StatusForbidden, 0},
			{"foreign-claim", "dev:tenant-b:auditor", "?tenant_id=tenant-a", http.StatusOK, 0},
			{"signed-claim-wins", "dev:tenant-a:auditor", "?tenant_id=tenant-b", http.StatusOK, 1},
			{"platform-all", "dev:platform:platform-admin", "", http.StatusOK, 1},
			{"platform-filtered", "dev:platform:platform-admin", "?tenant_id=tenant-a", http.StatusOK, 1},
			{"platform-empty-filter", "dev:platform:platform-admin", "?tenant_id=", http.StatusOK, 1},
			{"platform-missing-tenant", "dev:platform:platform-admin", "?tenant_id=tenant-missing", http.StatusOK, 0},
			{"signed-console-all", consoleToken, "", http.StatusOK, 1},
			{"invalid-selector", "dev:platform:platform-admin", "?tenant_id=a:b", http.StatusBadRequest, 0},
			{"repeated-selector", "dev:platform:platform-admin", "?tenant_id=tenant-a&tenant_id=tenant-b", http.StatusBadRequest, 0},
			{"invalid-limit", "dev:tenant-a:auditor", "?limit=0", http.StatusBadRequest, 0},
		} {
			t.Run(path+"/"+access.name, func(t *testing.T) {
				objects.gets.Store(0)
				request, err := http.NewRequest(http.MethodGet, server.URL+path+access.selector, nil)
				if err != nil {
					t.Fatal("could not build Console boundary request")
				}
				if access.token != "" {
					request.Header.Set("Authorization", "Bearer "+access.token)
				}
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					t.Fatal("Console request did not return a response")
				}
				body, readErr := io.ReadAll(response.Body)
				response.Body.Close()
				if readErr != nil || response.StatusCode != access.status || response.Header.Get("Cache-Control") != "no-store" || bytes.Contains(body, []byte("__search_digest")) {
					t.Fatal("Console scope/auth/no-store or recursive digest stripping boundary changed")
				}
				if access.status != http.StatusOK {
					var denied errorEnvelope
					if json.Unmarshal(body, &denied) != nil || denied.Error.Code == "" || bytes.Contains(body, []byte(`"events"`)) || bytes.Contains(body, []byte(`"facets"`)) || objects.gets.Load() != 0 {
						t.Fatal("denied Console request read or returned evidence")
					}
					return
				}
				var result struct {
					Events []domain.Event     `json:"events"`
					Count  int                `json:"count"`
					Facets domain.EventFacets `json:"facets"`
				}
				if json.Unmarshal(body, &result) != nil {
					t.Fatal("Console success response could not be decoded")
				}
				count := result.Count
				if strings.HasSuffix(path, "/facets") {
					count = result.Facets.Total
				} else if len(result.Events) != access.count {
					t.Fatal("Console event membership crossed its signed scope")
				}
				if count != access.count || (objects.gets.Load() > 0) != (access.count > 0) {
					t.Fatal("Console scope count/Get behavior changed")
				}
			})
		}
	}
	if objects.gets.Load() != 0 {
		t.Fatal("invalid request unexpectedly read archived evidence")
	}
	// The internal actor-free read adds no fact and verifies that response
	// stripping never removed the stored top-level/nested derived metadata.
	stored, err := svc.GetEvent(testCtx, "tenant-a", "", "strip-evt-1")
	if err != nil || !hasDigestKey(stored.Payload) {
		t.Fatal("Console HTTP digest stripping mutated stored evidence")
	}
}

func TestHTTPConsoleEvidenceCorruptionCannotHideBehindFilters(t *testing.T) {
	server, svc := timelineStripFixture(t)
	defer svc.Store.Close()
	defer server.Close()
	objects := &consoleHTTPArchive{Store: svc.Config.Archive}
	objects.corrupt.Store(true)
	svc.Config.Archive = objects
	for _, path := range []string{"/api/v1/compat/snaplink/audit/events", "/api/v1/compat/snaplink/audit/facets"} {
		for _, token := range []string{"dev:tenant-a:auditor", "dev:platform:platform-admin"} {
			objects.gets.Store(0)
			request, err := http.NewRequest(http.MethodGet, server.URL+path+"?actor_id=does-not-match", nil)
			if err != nil {
				t.Fatal("could not prepare corrupted Console evidence request")
			}
			request.Header.Set("Authorization", "Bearer "+token)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal("corrupted Console evidence request did not return")
			}
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			var denied errorEnvelope
			if readErr != nil || response.StatusCode != http.StatusInternalServerError || response.Header.Get("Cache-Control") != "no-store" || json.Unmarshal(body, &denied) != nil || denied.Error.Code != "internal_error" || denied.Error.Message != "internal server error" || bytes.Contains(body, []byte(`"events"`)) || bytes.Contains(body, []byte(`"facets"`)) || bytes.Contains(body, []byte(`"payload"`)) || objects.gets.Load() != 1 {
				t.Fatal("Console corruption became a normal empty result, leaked diagnostics or served partial data")
			}
		}
	}
	facts, err := svc.ListAdminActions(testCtx, "", true, 100)
	if err != nil {
		t.Fatal("could not inspect Console read facts")
	}
	for _, fact := range facts {
		if fact.Action == domain.AdminActionEventRead {
			t.Fatal("unauthenticated Console evidence appended a successful read fact")
		}
	}
}
