package outbox

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
)

// APIURLInsecureEnv is the explicit dev/verify-stack escape hatch for the
// plaintext-HTTP api-url gate (protocol F3, RFC 6750 §1: a bearer credential
// must never travel over a clear transport). Non-loopback http:// api-urls
// fail closed at startup in every client binary unless this variable is
// exactly "true". It mirrors the JWKS/Vault loopback opt-ins
// (AUDIT_ALLOW_INSECURE_JWKS_LOOPBACK / AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK)
// and must never be set in production: the ingest token is a bearer
// credential, and a misconfigured production AUDIT_OUTBOX_API_URL would
// otherwise ship it in clear over the network.
const APIURLInsecureEnv = "AUDIT_ALLOW_INSECURE_API_URL"

// InsecureAPIURLAllowed reports whether the explicit dev/verify-stack
// escape hatch for non-loopback plaintext http api-urls is set. Client
// binaries call this once at startup.
func InsecureAPIURLAllowed() bool {
	return os.Getenv(APIURLInsecureEnv) == "true"
}

// ValidateAPIURL enforces the bearer-transport rule (RFC 6750 §1) for an
// audit API base URL: https is always allowed; plaintext http is allowed
// only for loopback targets (localhost, loopback IPs, host.docker.internal)
// or under the explicit allowInsecureHTTP opt-in. Malformed URLs (missing
// scheme/host, userinfo, fragment) fail closed. Mirror of
// internal/auth.validateJWKSURL, which applies the same rule to the JWKS
// trust source; api-urls additionally carry the ingest bearer credential.
func ValidateAPIURL(rawURL string, allowInsecureHTTP bool) error {
	trimmed := strings.TrimSpace(rawURL)
	if rawURL != trimmed {
		return fmt.Errorf("invalid api URL")
	}
	endpoint, err := url.Parse(trimmed)
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.Fragment != "" {
		return fmt.Errorf("invalid api URL")
	}
	if endpoint.Scheme == "https" {
		return nil
	}
	if endpoint.Scheme == "http" && (allowInsecureHTTP || loopbackHost(endpoint.Hostname())) {
		return nil
	}
	return fmt.Errorf("audit API URL must use HTTPS; plaintext http is allowed only for loopback hosts, or set %s=true for local verification stacks (never production: the ingest bearer token would travel in clear)", APIURLInsecureEnv)
}

// IsInsecureHTTPURL reports whether the URL is plaintext http to a
// non-loopback host — the transport the startup gate rejects. Used by the
// binaries to warn loudly when the explicit opt-in re-opens it.
func IsInsecureHTTPURL(rawURL string) bool {
	endpoint, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || endpoint.Host == "" {
		return false
	}
	return endpoint.Scheme == "http" && !loopbackHost(endpoint.Hostname())
}

// loopbackHost mirrors internal/auth.loopbackHost (same rule, same
// rationale): localhost, IPv4/IPv6 loopback addresses, and the Docker
// host-gateway aliases used by local verification stacks.
func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if strings.EqualFold(host, "host.docker.internal") || strings.EqualFold(host, "gateway.docker.internal") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}
