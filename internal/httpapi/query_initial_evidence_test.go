package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
)

type initialQueryHTTPArchive struct {
	archive.Store
	drift  string
	forged string // synthetic query value; never logged or placed in a URL
	gets   atomic.Int64
}

func (a *initialQueryHTTPArchive) Get(ctx context.Context, key string) ([]byte, error) {
	raw, err := a.Store.Get(ctx, key)
	if err != nil || !strings.HasPrefix(key, "events/") {
		return raw, err
	}
	a.gets.Add(1)
	var event domain.Event
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&event); err != nil {
		return nil, err
	}
	switch a.drift {
	case "missing-sidecar":
		delete(event.Payload, "email"+security.SearchDigestSuffix)
	case "forged-sidecar":
		event.Payload["email"+security.SearchDigestSuffix] = a.forged
	case "actor":
		event.Actor.ID = "hidden-actor"
	}
	return domain.CanonicalJSON(event)
}

func TestHTTPInitialQueryAuthorizationAuthenticationAndNoStore(t *testing.T) {
	server, svc := timelineStripFixture(t) // real file state and FileStore source
	defer svc.Store.Close()
	defer server.Close()
	objects := &initialQueryHTTPArchive{Store: svc.Config.Archive}
	svc.Config.Archive = objects
	query := domain.Query{From: svc.Now(), To: svc.Now().Add(time.Minute), PageSize: 1, ActorID: "user-1"}
	reads := 0
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		for _, access := range []struct {
			name, token, selector string
			status, count         int
			evidence              bool
		}{
			{"tenant", "dev:tenant-a:auditor", "", http.StatusOK, 1, true},
			{"signed-scope-wins", "dev:tenant-a:auditor", "tenant-b", http.StatusOK, 1, true},
			{"platform-selected", "dev:platform:platform-admin", "tenant-a", http.StatusOK, 1, true},
			{"missing-credential", "", "", http.StatusUnauthorized, 0, false},
			{"missing-permission", "dev:tenant-a:viewer", "", http.StatusForbidden, 0, false},
			{"foreign-scope", "dev:tenant-b:auditor", "tenant-a", http.StatusOK, 0, false},
			{"platform-unselected", "dev:platform:platform-admin", "", http.StatusBadRequest, 0, false},
		} {
			t.Run(method+"/"+access.name, func(t *testing.T) {
				values := url.Values{}
				if access.selector != "" {
					values.Set("tenant_id", access.selector)
				}
				path := "/api/v1/events"
				var body io.Reader
				if method == http.MethodPost {
					path += ":query"
					raw, err := json.Marshal(query)
					if err != nil {
						t.Fatal("could not prepare query body")
					}
					body = bytes.NewReader(raw)
				} else {
					values.Set("from", query.From.Format(time.RFC3339Nano))
					values.Set("to", query.To.Format(time.RFC3339Nano))
					values.Set("actor_id", query.ActorID)
				}
				request, err := http.NewRequest(method, server.URL+path+"?"+values.Encode(), body)
				if err != nil {
					t.Fatal("could not prepare scoped initial-query request")
				}
				if access.token != "" {
					request.Header.Set("Authorization", "Bearer "+access.token)
				}
				objects.gets.Store(0)
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					t.Fatal("initial-query request did not return a response")
				}
				raw, readErr := io.ReadAll(response.Body)
				response.Body.Close()
				if readErr != nil || response.StatusCode != access.status || response.Header.Get("Cache-Control") != "no-store" || access.evidence != (objects.gets.Load() > 0) {
					t.Fatal("initial query changed scope/no-store or performed Get before authorization")
				}
				if access.status == http.StatusOK {
					var page domain.QueryResult
					if json.Unmarshal(raw, &page) != nil || page.Count != access.count || len(page.Items) != access.count || page.Snapshot == nil || page.Snapshot.Source != "ledger" || page.Snapshot.Completeness != "not_asserted" {
						t.Fatal("initial query changed its fixed-member response shape")
					}
					if access.count != 0 && hasDigestKey(page.Items[0].Payload) {
						t.Fatal("initial HTTP query exposed a search sidecar")
					}
					reads++
					return
				}
				var denied errorEnvelope
				if json.Unmarshal(raw, &denied) != nil || denied.Error.Code == "" || bytes.Contains(raw, []byte(`"items"`)) || bytes.Contains(raw, []byte(`"snapshot"`)) {
					t.Fatal("denied initial query served partial membership")
				}
			})
		}
	}
	query.PayloadField = "email"
	var err error
	query.PayloadDigest, err = security.SearchDigestBound("alice@example.test", svc.Config.EncryptionKey, "tenant-a", "email")
	if err != nil {
		t.Fatal("could not prepare synthetic sensitive body condition")
	}
	for _, mode := range []string{"missing-sidecar", "forged-sidecar", "actor"} {
		objects.drift = mode
		want, status := 1, http.StatusOK
		if mode == "forged-sidecar" {
			query.PayloadDigest, err = security.SearchDigestBound("different@example.test", svc.Config.EncryptionKey, "tenant-a", "email")
			if err != nil {
				t.Fatal("could not prepare synthetic forged sidecar condition")
			}
			objects.forged, want = query.PayloadDigest, 0
		}
		if mode == "actor" {
			status = http.StatusInternalServerError
		}
		rawQuery, err := json.Marshal(query)
		if err != nil {
			t.Fatal("could not prepare sensitive initial-query body")
		}
		request, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events:query", bytes.NewReader(rawQuery))
		if err != nil {
			t.Fatal("could not prepare initial-query evidence request")
		}
		request.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal("initial-query evidence request did not return a response")
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil || response.StatusCode != status || response.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("initial-query authentication changed status/no-store")
		}
		if status == http.StatusOK {
			var page domain.QueryResult
			if json.Unmarshal(body, &page) != nil || page.Count != want || len(page.Items) != want || (want != 0 && hasDigestKey(page.Items[0].Payload)) {
				t.Fatal("HTTP initial query trusted sidecar presence/value instead of original authenticated content")
			}
			reads++
			continue
		}
		var denied errorEnvelope
		if json.Unmarshal(body, &denied) != nil || denied.Error.Code != "internal_error" || denied.Error.Message != "internal server error" || bytes.Contains(body, []byte(`"items"`)) || bytes.Contains(body, []byte(`"count"`)) || bytes.Contains(body, []byte(`"snapshot"`)) {
			t.Fatal("unauthenticated initial query served membership or leaked backend diagnostics")
		}
	}
	facts, err := svc.ListAdminActions(testCtx, "", true, 100)
	if err != nil {
		t.Fatal("could not inspect independent initial-query read facts")
	}
	actual := 0
	for _, fact := range facts {
		if fact.Action == domain.AdminActionEventRead {
			actual++
			if fact.TargetType != "query" || fact.TargetID != "" || fact.Actor == "" {
				t.Fatal("HTTP initial query changed its original scoped read-fact target")
			}
		}
	}
	if actual != reads || reads != 10 {
		t.Fatal("initial-query denial/authentication failure appended success or valid empty observation was not audited")
	}
}
