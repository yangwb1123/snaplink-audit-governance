package security

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newVaultTestServer(t *testing.T, token string) (*httptest.Server, *[]map[string]any) {
	t.Helper()
	var requests []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Vault-Token") != token {
			http.Error(w, `{"errors":["permission denied"]}`, http.StatusForbidden)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, `{"errors":["bad request"]}`, http.StatusBadRequest)
			return
		}
		requests = append(requests, body)
		key := strings.TrimPrefix(r.URL.Path, "/v1/transit/sign/")
		key = strings.TrimPrefix(key, "/v1/transit/verify/")
		switch {
		case strings.Contains(r.URL.Path, "/sign/"):
			w.Write([]byte(fmt.Sprintf(`{"data":{"signature":"vault:v1:%s:fake-signature"}}`, key)))
		case strings.Contains(r.URL.Path, "/verify/"):
			valid := body["signature"] == "vault:v1:test-key:fake-signature"
			w.Write([]byte(fmt.Sprintf(`{"data":{"valid":%t}}`, valid)))
		default:
			http.Error(w, `{"errors":["not found"]}`, http.StatusNotFound)
		}
	}))
	return server, &requests
}

func TestVaultTransitSignAndVerify(t *testing.T) {
	server, requests := newVaultTestServer(t, "s.token")
	defer server.Close()
	signer := NewVaultTransitSigner(server.URL, "s.token", "test-key")
	if signer.Algorithm() != "vault-transit:test-key" {
		t.Fatalf("algorithm=%s", signer.Algorithm())
	}
	data := []byte("manifest-hash")
	signature, err := signer.Sign(data)
	if err != nil {
		t.Fatal(err)
	}
	if signature != "vault:v1:test-key:fake-signature" {
		t.Fatalf("signature=%s", signature)
	}
	valid, err := signer.Verify(data, signature)
	if err != nil || !valid {
		t.Fatalf("verify valid=%v err=%v", valid, err)
	}
	invalid, err := signer.Verify(data, "vault:v1:test-key:wrong")
	if err != nil || invalid {
		t.Fatalf("verify must reject wrong signature: valid=%v err=%v", invalid, err)
	}
	if len(*requests) != 3 {
		t.Fatalf("vault requests=%d, want 3", len(*requests))
	}
	// 输入必须是 base64 编码的原始字节。
	input := (*requests)[0]["input"].(string)
	decoded, err := base64.StdEncoding.DecodeString(input)
	if err != nil || string(decoded) != "manifest-hash" {
		t.Fatalf("input round-trip failed: %v %q", err, decoded)
	}
}

func TestVaultTransitUnauthorized(t *testing.T) {
	server, _ := newVaultTestServer(t, "s.token")
	defer server.Close()
	signer := NewVaultTransitSigner(server.URL, "wrong-token", "test-key")
	if _, err := signer.Sign([]byte("x")); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestVaultTransitUnreachable(t *testing.T) {
	signer := NewVaultTransitSigner("http://127.0.0.1:1", "t", "k")
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	_ = ctx
	if _, err := signer.Sign([]byte("x")); err == nil {
		t.Fatal("unreachable vault must fail")
	}
}
