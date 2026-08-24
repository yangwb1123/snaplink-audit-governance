package outbox

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestTenantHTTPDelivererClassifiesExactTenantMismatch(t *testing.T) {
	newServer := func(body string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if got := r.Header.Get("Authorization"); got != "Bearer tenant-a-token" {
				t.Errorf("authorization=%q, want deliberately mis-scoped token", got)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = fmt.Fprint(w, body)
		}))
	}
	cases := []struct {
		name             string
		body             string
		wantMismatch     bool
		wantPermanent    bool
		wantScopeBlocked bool
	}{
		{name: "tenant mismatch", body: `{"error":{"code":"tenant_mismatch","message":"wrong tenant"}}`, wantMismatch: true, wantScopeBlocked: true},
		{name: "schema not found", body: `{"error":{"code":"schema_not_found","message":"missing"}}`, wantPermanent: true},
		{name: "wrong response shape", body: `{"code":"tenant_mismatch"}`, wantPermanent: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newServer(tc.body)
			defer server.Close()
			deliver := HTTPDelivererForTenant(server.URL, func(context.Context, string) (string, error) {
				return "tenant-a-token", nil
			}, nil)
			_, err := deliver(context.Background(), domain.Event{EventID: "evt-b", TenantID: "tenant-b"})
			if err == nil {
				t.Fatal("422 must fail")
			}
			var deliveryErr *DeliveryError
			if !errors.As(err, &deliveryErr) {
				t.Fatalf("error=%v, want DeliveryError", err)
			}
			if deliveryErr.TenantMismatch != tc.wantMismatch || deliveryErr.TenantScopeBlocked != tc.wantScopeBlocked || deliveryErr.Permanent != tc.wantPermanent {
				t.Fatalf("classification=%+v, want mismatch=%v scope=%v permanent=%v", deliveryErr, tc.wantMismatch, tc.wantScopeBlocked, tc.wantPermanent)
			}
		})
	}
}

func TestStaticTenantCredentialResolverFailsClosed(t *testing.T) {
	resolver := StaticTenantCredentialResolver("tenant-b", "tenant-b-secret")
	if token, err := resolver(context.Background(), "tenant-b"); err != nil || token != "tenant-b-secret" {
		t.Fatalf("in-scope resolver returned token=%q err=%v", token, err)
	}
	for _, tenant := range []string{"tenant-a", ""} {
		token, err := resolver(context.Background(), tenant)
		if err == nil || token != "" {
			t.Fatalf("tenant=%q token=%q err=%v, want no credential", tenant, token, err)
		}
		var credentialErr *TenantCredentialError
		if !errors.As(err, &credentialErr) || strings.Contains(err.Error(), "secret") {
			t.Fatalf("tenant=%q err=%v, want structured redacted credential error", tenant, err)
		}
	}
	missing := StaticTenantCredentialResolver("tenant-b", "")
	if token, err := missing(context.Background(), "tenant-b"); err == nil || token != "" {
		t.Fatalf("missing credential token=%q err=%v, want pending configuration error", token, err)
	}
}
