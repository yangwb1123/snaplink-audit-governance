package outbox

import (
	"testing"
)

// F3 gate: ValidateAPIURL enforces the bearer-transport rule (RFC 6750 §1) —
// https always, plaintext http only for loopback hosts or the explicit
// dev/verify-stack opt-in, malformed URLs fail closed. Mirrors the JWKS URL
// gate (internal/auth.validateJWKSURL).
func TestValidateAPIURLAcceptsHTTPS(t *testing.T) {
	for _, raw := range []string{
		"https://audit.example.com",
		"https://audit.example.com:8443",
		"https://localhost",
	} {
		if err := ValidateAPIURL(raw, false); err != nil {
			t.Fatalf("ValidateAPIURL(%q) = %v, want nil", raw, err)
		}
	}
}

func TestValidateAPIURLAcceptsLoopbackHTTP(t *testing.T) {
	for _, raw := range []string{
		"http://localhost:8089",
		"http://127.0.0.1:8089",
		"http://[::1]:8089",
		"http://host.docker.internal:8089",
		"http://gateway.docker.internal:8089",
		"http://LOCALHOST:8089",
	} {
		if err := ValidateAPIURL(raw, false); err != nil {
			t.Fatalf("ValidateAPIURL(%q) = %v, want nil (loopback http)", raw, err)
		}
	}
}

func TestValidateAPIURLRejectsNonLoopbackPlaintextHTTP(t *testing.T) {
	for _, raw := range []string{
		"http://audit.example.com",
		"http://audit-api:8089", // container-network hostname: not loopback
		"http://10.0.0.5:8089",
	} {
		if err := ValidateAPIURL(raw, false); err != nil {
			continue // expected rejection
		}
		t.Fatalf("ValidateAPIURL(%q) = nil, want rejection (bearer credential over plaintext http)", raw)
	}
}

func TestValidateAPIURLOptInAllowsPlaintextHTTP(t *testing.T) {
	if err := ValidateAPIURL("http://audit.example.com", true); err != nil {
		t.Fatalf("ValidateAPIURL with allowInsecureHTTP=true = %v, want nil (explicit dev opt-in)", err)
	}
}

func TestValidateAPIURLRejectsMalformed(t *testing.T) {
	for _, raw := range []string{
		"",                    // no URL at all
		"  http://x",          // untrimmed
		"audit.example.com",   // missing scheme
		"localhost:8089",      // parsed as scheme, no host
		"https://user@host/x", // userinfo
		"https://host/x#frag", // fragment
		"ftp://audit.example.com",
	} {
		if err := ValidateAPIURL(raw, true); err == nil {
			t.Fatalf("ValidateAPIURL(%q) = nil, want rejection (malformed)", raw)
		}
	}
}

func TestIsInsecureHTTPURL(t *testing.T) {
	cases := []struct {
		raw  string
		want bool
	}{
		{"http://audit-api:8089", true},
		{"http://audit.example.com", true},
		{"http://localhost:8089", false},
		{"http://127.0.0.1:8089", false},
		{"http://host.docker.internal:8089", false},
		{"https://audit.example.com", false},
		{"not-a-url", false},
	}
	for _, tc := range cases {
		if got := IsInsecureHTTPURL(tc.raw); got != tc.want {
			t.Fatalf("IsInsecureHTTPURL(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}
