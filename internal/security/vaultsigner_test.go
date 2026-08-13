package security

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
	signature, err := signer.Sign(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	if signature != "vault:v1:test-key:fake-signature" {
		t.Fatalf("signature=%s", signature)
	}
	valid, err := signer.Verify(context.Background(), data, signature)
	if err != nil || !valid {
		t.Fatalf("verify valid=%v err=%v", valid, err)
	}
	invalid, err := signer.Verify(context.Background(), data, "vault:v1:test-key:wrong")
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
	if _, err := signer.Sign(context.Background(), []byte("x")); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestVaultTransitUnreachable(t *testing.T) {
	signer := NewVaultTransitSigner("http://127.0.0.1:1", "t", "k")
	if _, err := signer.Sign(context.Background(), []byte("x")); err == nil {
		t.Fatal("unreachable vault must fail")
	}
}

// TestVaultTransitSignRejectsForeignKey is AC-2: a Vault response whose
// signature carries a transit key name different from the configured key is
// rejected by Sign (fail-closed — no evidence is recorded under an unbound
// key). The error names both keys and never echoes the payload.
func TestVaultTransitSignRejectsForeignKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":{"signature":"vault:v1:other-key:fake-signature"}}`))
	}))
	defer server.Close()
	signer := NewVaultTransitSigner(server.URL, "s.token", "test-key")
	_, err := signer.Sign(context.Background(), []byte("x"))
	if err == nil {
		t.Fatal("foreign-key signature must be rejected")
	}
	if !strings.Contains(err.Error(), "test-key") || !strings.Contains(err.Error(), "other-key") {
		t.Fatalf("error must name configured and observed key, got: %v", err)
	}
	if strings.Contains(err.Error(), "fake-signature") {
		t.Fatalf("error must never echo the signature payload: %v", err)
	}
}

// TestVaultTransitSignRejectsMalformed is AC-2: malformed/truncated prefixes
// fail closed instead of being accepted as any non-empty signature. The
// trailing-colon case (vault:v1:key:) pins F5 — a missing payload must be
// rejected even when the prefix parses into four segments.
func TestVaultTransitSignRejectsMalformed(t *testing.T) {
	cases := []struct {
		name      string
		signature string
	}{
		{"garbage", "garbage"},
		{"empty prefix", "vault:v1:"},
		{"bad version", "vault:x:test-key:x"},
		{"missing payload", "vault:v1:test-key"},
		{"trailing colon", "vault:v1:test-key:"},
		{"uppercase scheme", "VAULT:v1:test-key:x"},
		{"unicode version digit", "vault:v١:test-key:x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(fmt.Sprintf(`{"data":{"signature":%q}}`, tc.signature)))
			}))
			defer server.Close()
			signer := NewVaultTransitSigner(server.URL, "s.token", "test-key")
			if _, err := signer.Sign(context.Background(), []byte("x")); err == nil {
				t.Fatalf("signature %q must be rejected", tc.signature)
			}
		})
	}
}

// TestVaultTransitSignVerifyCancelledContext is AC-3: a pre-cancelled
// context aborts both Sign and Verify promptly with the context error, so a
// caller can never be stuck on the client timeout.
func TestVaultTransitSignVerifyCancelledContext(t *testing.T) {
	server, _ := newVaultTestServer(t, "s.token")
	defer server.Close()
	signer := NewVaultTransitSigner(server.URL, "s.token", "test-key")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := signer.Sign(ctx, []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sign with cancelled ctx: err=%v, want context.Canceled", err)
	}
	if _, err := signer.Verify(ctx, []byte("x"), "vault:v1:test-key:fake-signature"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Verify with cancelled ctx: err=%v, want context.Canceled", err)
	}
}

// TestVaultTransitInFlightCancellation is AC-3: cancelling the context while
// the request is genuinely in flight (the handler is blocked) aborts the
// round trip promptly, and the handler observes the cancellation. The
// handler-side r.Context().Err() observation is the proof the request was
// actually in flight — no sleeps, no trivially-passing race. The handler
// drains the request body first: verified against Go 1.26 stdlib that a
// client abort on a POST whose body the handler never consumed is not
// reliably observable server-side (the read loop stays blocked in the body
// read), so the drain is required for the observation to fire.
func TestVaultTransitInFlightCancellation(t *testing.T) {
	handlerSeenCancel := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
			if r.Context().Err() != context.Canceled {
				t.Errorf("handler saw err=%v, want context.Canceled", r.Context().Err())
			}
			close(handlerSeenCancel)
		case <-release:
		}
	}))
	defer server.Close()
	signer := NewVaultTransitSigner(server.URL, "s.token", "test-key")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := signer.Sign(ctx, []byte("x")); done <- err }()
	// Give the request a chance to reach the handler, then cancel mid-flight.
	select {
	case <-handlerSeenCancel:
		t.Fatal("handler cancelled before we cancelled the caller context")
	case <-time.After(200 * time.Millisecond):
		cancel()
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Sign err=%v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Sign did not return promptly after cancellation")
	}
	select {
	case <-handlerSeenCancel:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never observed the request context cancellation")
	}
}

// TestVaultTransitRedirectDoesNotForwardToken pins F-1: the signer refuses
// to follow redirects, so X-Vault-Token is never forwarded to a redirect
// target. A 3xx surfaces as a fail-closed error and the target receives zero
// requests.
func TestVaultTransitRedirectDoesNotForwardToken(t *testing.T) {
	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetRequests.Add(1)
		http.Error(w, "unreachable", http.StatusTeapot)
	}))
	defer target.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirector.Close()
	signer := NewVaultTransitSigner(redirector.URL, "s.token", "test-key")
	_, err := signer.Sign(context.Background(), []byte("x"))
	if err == nil {
		t.Fatal("a redirect must surface as a fail-closed error, not be followed")
	}
	if got := targetRequests.Load(); got != 0 {
		t.Fatalf("redirect target received %d requests, want 0 (X-Vault-Token must never be forwarded)", got)
	}
}

// TestVaultTransitSlowResponseTimesOut pins the timeout-uncertain path: a
// Vault that accepts the connection but never answers fails within the
// injected client timeout instead of hanging the caller. The timeout is
// injectable through the constructor seam so the test is deterministic (no
// sleeps).
func TestVaultTransitSlowResponseTimesOut(t *testing.T) {
	blocked := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(blocked)
		// Drain the body first: a client abort on a POST whose body the
		// handler never consumed is not reliably observable server-side, so
		// r.Context().Done() would never fire and the handler would leak
		// (mirrors TestVaultTransitInFlightCancellation).
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	// LIFO: release must close before server.Close() waits on the handler,
	// otherwise Close() blocks forever if the client abort was not observed.
	defer close(release)
	defer server.Close()
	signer := newVaultTransitSigner(server.URL, "t", "k", 50*time.Millisecond)
	done := make(chan error, 1)
	go func() { _, err := signer.Sign(context.Background(), []byte("x")); done <- err }()
	<-blocked
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "Client.Timeout") {
			t.Fatalf("expected client timeout error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Sign did not return within the injected client timeout")
	}
}

// FuzzCheckSignatureKey pins the signature-prefix parser (testing spec §4:
// parsers/protocol parsing → fuzz). Invariants: no panic; a well-formed
// signature bound to the configured key is accepted; every rejection maps to
// exactly one of the two documented error shapes (malformed literal, or
// foreign-key naming both keys) — never an error that interpolates the
// payload (F4 discipline: data is never echoed; a short payload like "x"
// coincidentally appears inside the fixed literal's own words, which is why
// the shape is pinned instead of a substring check).
func FuzzCheckSignatureKey(f *testing.F) {
	for _, seed := range []string{
		"vault:v1:test-key:fake-signature",
		"garbage",
		"vault:v1:",
		"vault:x:k:x",
		"vault:v1:k",
		"VAULT:v1:k:x",
		"vault:v١:k:x",
	} {
		f.Add(seed)
	}
	signer := NewVaultTransitSigner("http://127.0.0.1:1", "t", "test-key")
	const malformedShape = "vault transit sign: signature has malformed prefix: expected vault:v<version>:<key>:<payload>"
	f.Fuzz(func(t *testing.T, signature string) {
		parts := strings.Split(signature, ":")
		wellFormedBound := len(parts) >= 4 && parts[0] == "vault" && vaultVersionSegment(parts[1]) && parts[2] == signer.key && parts[3] != ""
		err := signer.checkSignatureKey(signature)
		if wellFormedBound {
			if err != nil {
				t.Fatalf("well-formed signature bound to the configured key must be accepted, got %v", err)
			}
			return
		}
		if err == nil {
			t.Fatalf("malformed or foreign-key signature %q must be rejected", signature)
		}
		if len(parts) >= 4 && parts[0] == "vault" && vaultVersionSegment(parts[1]) && parts[2] != "" && parts[2] != signer.key && parts[3] != "" {
			want := fmt.Sprintf("vault transit sign: signature bound to transit key %q, want %q: refusing to record evidence under a different key", parts[2], signer.key)
			if err.Error() != want {
				t.Fatalf("foreign-key error = %q, want %q (payload must never be interpolated)", err, want)
			}
			return
		}
		if err.Error() != malformedShape {
			t.Fatalf("malformed error = %q, want fixed literal %q (input must never be echoed)", err, malformedShape)
		}
	})
}
