package security

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// VaultTransitSigner signs and verifies checkpoint manifests through the
// HashiCorp Vault Transit engine. The signature key stays inside Vault: the
// application can request signatures but can never export the private key
// (architecture plan section 10). The signer performs no transport security
// itself; plaintext http is reachable only through the explicit loopback
// opt-in (AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK) enforced upstream by
// SigningArchive.resolveVaultTransport, and https is enforced fail-closed.
type VaultTransitSigner struct {
	addr   string
	token  string
	key    string
	client *http.Client
}

// vaultClientTimeout is the per-round-trip http.Client timeout: it bounds
// connect, headers and body read for an idle Vault that never answers.
const vaultClientTimeout = 10 * time.Second

// vaultCallBudget bounds the synchronous Vault round trip even when the
// caller detached cancellation (ingest commits must survive client
// disconnect): a black-holed or degraded Vault can therefore stall the
// store write lock for at most this long per call. An earlier caller
// deadline still wins.
const vaultCallBudget = 3 * time.Second

// redirectReject refuses to follow any redirect (http.ErrUseLastResponse):
// the X-Vault-Token header must never be forwarded to a redirect target. A
// 3xx therefore surfaces as a non-200 status error (fail-closed, nothing
// recorded). Mirrors the JWKS client's restrictedHTTPClient posture.
var redirectReject = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

func NewVaultTransitSigner(addr, token, key string) *VaultTransitSigner {
	return newVaultTransitSigner(addr, token, key, vaultClientTimeout)
}

// newVaultTransitSigner is the constructor seam for tests: it lets a test
// shrink the client timeout to exercise the timeout-uncertain path
// deterministically (a blocked handler fails within the deadline instead of
// waiting out the production default).
func newVaultTransitSigner(addr, token, key string, clientTimeout time.Duration) *VaultTransitSigner {
	return &VaultTransitSigner{
		addr:   addr,
		token:  token,
		key:    key,
		client: &http.Client{Timeout: clientTimeout, CheckRedirect: redirectReject},
	}
}

// Algorithm reports the checkpoint algorithm label stored with segments.
func (v *VaultTransitSigner) Algorithm() string { return "vault-transit:" + v.key }

func (v *VaultTransitSigner) Sign(ctx context.Context, data []byte) (string, error) {
	return v.call(ctx, "sign", map[string]any{"input": base64.StdEncoding.EncodeToString(data)}, func(decoded map[string]any) (string, error) {
		signature, _ := decoded["signature"].(string)
		if signature == "" {
			return "", fmt.Errorf("vault transit sign: empty signature")
		}
		// REQ-2 key binding: a compromised or misconfigured Vault that signs
		// under a different transit key must never have that evidence recorded.
		if err := v.checkSignatureKey(signature); err != nil {
			return "", err
		}
		return signature, nil
	})
}

func (v *VaultTransitSigner) Verify(ctx context.Context, data []byte, signature string) (bool, error) {
	valid, err := v.call(ctx, "verify", map[string]any{"input": base64.StdEncoding.EncodeToString(data), "signature": signature}, func(decoded map[string]any) (string, error) {
		if valid, ok := decoded["valid"].(bool); ok {
			return fmt.Sprintf("%t", valid), nil
		}
		return "", fmt.Errorf("vault transit verify: missing valid flag")
	})
	if err != nil {
		return false, err
	}
	return valid == "true", nil
}

// checkSignatureKey verifies the returned Vault Transit signature is bound
// to the configured key. Vault signatures have the shape
// vault:v<version>:<key-name>:<payload>. Key names are not secrets; the
// payload is never echoed (F4 discipline: credentials/data are never
// echoed, and a signature's payload is data). The version segment is
// informational only — key-name binding is the requirement (AC-2).
func (v *VaultTransitSigner) checkSignatureKey(signature string) error {
	parts := strings.Split(signature, ":")
	if len(parts) < 4 || parts[0] != "vault" || !vaultVersionSegment(parts[1]) || parts[2] == "" || parts[3] == "" {
		return fmt.Errorf("vault transit sign: signature has malformed prefix: expected vault:v<version>:<key>:<payload>")
	}
	if observed := parts[2]; observed != v.key {
		return fmt.Errorf("vault transit sign: signature bound to transit key %q, want %q: refusing to record evidence under a different key", observed, v.key)
	}
	return nil
}

// vaultVersionSegment reports whether s is a Vault signature version token
// ("v1", "v2", …): a leading 'v' followed by one or more ASCII digits.
// ASCII-only on purpose: a unicode digit is not a Vault version token and
// must fail closed like any other malformed prefix.
func vaultVersionSegment(s string) bool {
	if len(s) < 2 || s[0] != 'v' {
		return false
	}
	for _, r := range s[1:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// call posts a transit operation and extracts a string result from
// response.data. The caller's context is threaded into the request so
// cancellation (client disconnect, gRPC cancel, worker signal, test) aborts
// the round trip promptly instead of blocking on the client timeout.
func (v *VaultTransitSigner) call(ctx context.Context, operation string, body map[string]any, extract func(map[string]any) (string, error)) (string, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	url := fmt.Sprintf("%s/v1/transit/%s/%s", v.addr, operation, v.key)
	// Per-call budget: bounds lock hold time when the caller detached
	// cancellation (context.WithoutCancel at the ingest boundary). An
	// earlier caller deadline still wins.
	ctx, cancel := context.WithTimeout(ctx, vaultCallBudget)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return "", err
	}
	request.Header.Set("X-Vault-Token", v.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := v.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("vault transit %s: %w", operation, err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if response.StatusCode != http.StatusOK {
		// Truncate the echoed body so a hostile Vault cannot inject
		// arbitrary (newline-terminated) text into operator logs; %q keeps
		// control characters escaped.
		return "", fmt.Errorf("vault transit %s: status %s body=%q", operation, response.Status, truncateEcho(string(payload)))
	}
	var decoded struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return "", fmt.Errorf("vault transit %s: decode: %w", operation, err)
	}
	return extract(decoded.Data)
}

// truncateEcho caps the Vault response body echoed into error text (F-4):
// logs stay bounded and cannot be log-injection vectors.
func truncateEcho(body string) string {
	const maxEcho = 256
	if len(body) <= maxEcho {
		return body
	}
	return body[:maxEcho] + "…(truncated)"
}
