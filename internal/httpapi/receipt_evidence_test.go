package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestHTTPReceiptReadScopeMetadataAndNoStore(t *testing.T) {
	server, svc := timelineStripFixture(t) // actual file ledger and archived event
	defer svc.Store.Close()
	defer server.Close()
	original, err := svc.Store.Snapshot(testCtx)
	if err != nil {
		t.Fatal("could not capture original committed HTTP receipt")
	}
	objects := &initialQueryHTTPArchive{Store: svc.Config.Archive}
	svc.Config.Archive = objects
	reads := 0
	for _, access := range []struct {
		name, token, selector, id string
		status                    int
	}{
		{"tenant", "dev:tenant-a:auditor", "", "strip-evt-1", http.StatusOK},
		{"signed-scope-wins", "dev:tenant-a:auditor", "tenant-b", "strip-evt-1", http.StatusOK},
		{"platform-selected", "dev:platform:platform-admin", "tenant-a", "strip-evt-1", http.StatusOK},
		{"missing-credential", "", "", "strip-evt-1", http.StatusUnauthorized},
		{"missing-permission", "dev:tenant-a:viewer", "", "strip-evt-1", http.StatusForbidden},
		{"foreign-tenant", "dev:tenant-b:auditor", "tenant-a", "strip-evt-1", http.StatusNotFound},
		{"platform-unselected", "dev:platform:platform-admin", "", "strip-evt-1", http.StatusBadRequest},
		{"absent-event", "dev:tenant-a:auditor", "", "absent-event", http.StatusNotFound},
	} {
		t.Run(access.name, func(t *testing.T) {
			values := url.Values{}
			if access.selector != "" {
				values.Set("tenant_id", access.selector)
			}
			request, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events/"+access.id+"/receipt?"+values.Encode(), nil)
			if err != nil {
				t.Fatal("could not prepare scoped receipt request")
			}
			if access.token != "" {
				request.Header.Set("Authorization", "Bearer "+access.token)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal("receipt request did not return a response")
			}
			raw, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil || response.StatusCode != access.status || response.Header.Get("Cache-Control") != "no-store" || objects.gets.Load() != 0 {
				t.Fatal("receipt read changed scope/no-store or read source evidence")
			}
			if access.status == http.StatusOK {
				var receipt domain.EventReceipt
				if json.Unmarshal(raw, &receipt) != nil || !reflect.DeepEqual(receipt, original.Receipts[store.EventKey("tenant-a", "strip-evt-1")]) {
					t.Fatal("HTTP receipt reclassified or changed retained metadata")
				}
				reads++
				return
			}
			var denied errorEnvelope
			if json.Unmarshal(raw, &denied) != nil || denied.Error.Code == "" || bytes.Contains(raw, []byte(`"event_id"`)) || bytes.Contains(raw, []byte(`"hash"`)) || bytes.Contains(raw, []byte(`"idempotency_key"`)) {
				t.Fatal("denied receipt read served partial metadata")
			}
		})
	}
	facts, err := svc.ListAdminActions(testCtx, "tenant-a", false, 100)
	actual := 0
	for _, fact := range facts {
		if fact.Action == domain.AdminActionEventRead {
			actual++
			if fact.TargetID != "strip-evt-1" || fact.Detail != "receipt" || fact.TargetType != "event" {
				t.Fatal("HTTP receipt read changed original fact target")
			}
		}
	}
	if err != nil || actual != reads || reads != 3 {
		t.Fatal("denied receipt appended success or permitted read lost self-audit")
	}
}

func TestHTTPReceiptReadMisboundIdentityFailsClosed(t *testing.T) {
	backend := &scriptedStoreBackend{data: store.NewSnapshot()}
	server, svc := testHTTPServerWithBackend(t, backend)
	defer svc.Store.Close()
	defer server.Close()
	if _, err := svc.Ingest(testCtx, "tenant-a", domain.IngestPrincipal{ClientID: "crm"}, httpReadSeedEvent(), domain.StatusArchived); err != nil {
		t.Fatal("could not prepare owned HTTP receipt identity fixture")
	}
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", "http-read-evt")
		receipt := data.Receipts[key]
		receipt.TenantID = "tenant-b"
		data.Receipts[key] = receipt
		return nil
	}); err != nil {
		t.Fatal("could not install owned HTTP receipt identity fault")
	}
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events/http-read-evt/receipt", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal("receipt identity request did not return a response")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	var denied errorEnvelope
	if err != nil || response.StatusCode != http.StatusConflict || response.Header.Get("Cache-Control") != "no-store" || json.Unmarshal(raw, &denied) != nil || denied.Error.Code != "conflict" || denied.Error.Message != "conflict: receipt identity mismatch" || bytes.Contains(raw, []byte(`"event_id"`)) || len(httpReadFacts(t, server)) != 0 {
		t.Fatal("misbound HTTP receipt exposed metadata, lost safe denial or appended success")
	}
}
