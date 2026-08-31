package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

type Claims struct {
	Subject     string
	ClientID    string
	TenantID    string
	Roles       []string
	Permissions map[string]bool
	Platform    bool
	Service     bool
	// ConsoleAuditRead maps the audience-bound Snaplink admin:read scope only
	// onto event reads; it grants no governance mutation permission.
	ConsoleAuditRead bool
	// CrossTenantAuditRead is restricted to Console tokens without a signed
	// tenant claim. A tenant-scoped token can never select another tenant.
	CrossTenantAuditRead bool
}

type Authenticator struct {
	JWTSecret                 string
	JWTPublicKeyPEM           string
	JWTPublicKeyAlgorithm     string
	JWKSURL                   string
	JWKSHTTPClient            *http.Client
	Issuer                    string
	Audience                  string
	AllowDev                  bool
	AllowLocalHS256           bool
	AllowInsecureJWKSLoopback bool
	// JWKSRefreshInterval controls how long a parsed JWKS set is reused
	// without refetching (0 means 10 minutes). A shorter interval trades
	// IdP round trips for faster key rotation adoption. During an IdP outage,
	// last-known-good keys may be trusted for at most three times the effective
	// interval; with the default interval that maximum stale grace period is
	// 30 minutes, after which refresh failure fails authentication closed.
	JWKSRefreshInterval time.Duration
	// ClockSkew extends the acceptance window for exp and nbf checks.
	// A positive value accepts tokens that expired up to ClockSkew ago
	// or are not-yet-active up to ClockSkew in the future. The zero
	// value (default) preserves strict zero-tolerance behaviour.
	ClockSkew time.Duration
}

func (a Authenticator) Authenticate(r *http.Request) (Claims, error) {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(header, "Bearer ") {
		return Claims{}, fmt.Errorf("authorization header is required")
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	return a.AuthenticateTokenContext(r.Context(), token)
}

func (a Authenticator) AuthenticateToken(token string) (Claims, error) {
	return a.AuthenticateTokenContext(context.Background(), token)
}

func (a Authenticator) AuthenticateTokenContext(ctx context.Context, token string) (Claims, error) {
	if err := a.ValidateConfiguration(); err != nil {
		return Claims{}, err
	}
	if a.AllowDev && strings.HasPrefix(token, "dev:") {
		return parseDevToken(token)
	}
	return a.parseJWT(ctx, token)
}

func parseDevToken(token string) (Claims, error) {
	parts := strings.Split(token, ":")
	if len(parts) < 3 || len(parts) > 4 || parts[1] == "" || parts[2] == "" {
		return Claims{}, fmt.Errorf("invalid development token")
	}
	// Key-framing charset rule: the dev-token subject becomes the tenant
	// context (TenantID), which is a composite-key component. A subject
	// embedding KeySeparator (0x1F) would collide with another tenant's keys
	// (EventKey("a\x1fb","evt") == EventKey("a","b\x1fevt")); control
	// characters, whitespace and path separators are rejected for the same
	// hygiene reasons as JWT tenant claims. ':' is additionally rejected
	// (tenant layer): the subject segment is the tenant ID, which prefixes
	// every Event.Stream() frame and must never be ambiguous with the
	// ':'-delimited dev-token framing itself. The rejection reuses the exact
	// malformed-token error text so a valid-vs-invalid subject is never an
	// oracle for token format.
	if err := domain.ValidTenantIDComponent("tenant id", parts[1]); err != nil {
		return Claims{}, fmt.Errorf("invalid development token")
	}
	roles := strings.Split(parts[2], ",")
	clientID := parts[1]
	if len(parts) == 4 {
		clientID = parts[3]
	}
	if clientID == "" {
		return Claims{}, fmt.Errorf("invalid development token")
	}
	claims := Claims{Subject: parts[1], ClientID: clientID, TenantID: parts[1], Roles: roles, Permissions: map[string]bool{}}
	claims.Permissions = permissionsForRoles(roles)
	// Platform derives from the same rule as the JWT path (parseJWT): the
	// explicit audit:platform:cross_tenant permission or the platform-admin
	// role. The tenant name "platform" alone grants nothing — a dev token
	// whose subject is "platform" but whose roles are non-admin (auditor,
	// compliance, service, ...) stays tenant-scoped, matching the JWT trust
	// model (fail-closed). Only permissionsForRoles can grant the permission,
	// and only the platform-admin role maps to it, so Platform is set iff
	// the platform-admin role is present.
	claims.Platform = claims.Permissions["audit:platform:cross_tenant"] || contains(roles, "platform-admin")
	if contains(roles, "service") || contains(roles, "event-writer") {
		claims.Service = true
	}
	return claims, nil
}

func (a Authenticator) parseJWT(ctx context.Context, token string) (Claims, error) {
	verifiedPayload, err := a.verifyJWT(ctx, token)
	if err != nil {
		return Claims{}, err
	}
	var payload map[string]any
	if err := json.Unmarshal(verifiedPayload, &payload); err != nil {
		return Claims{}, fmt.Errorf("invalid token claims: %w", err)
	}
	claims := Claims{Permissions: map[string]bool{}}
	now := time.Now().Unix()
	// exp is mandatory: a bearer token without an expiry is valid forever,
	// which turns token theft into a permanent replay credential in an audit
	// system. Requiring the claim keeps the verifier fail-closed even when a
	// misconfigured IdP omits it.
	expiry, hasExpiry := numericClaim(payload, "exp")
	if !hasExpiry {
		return Claims{}, fmt.Errorf("token must contain exp")
	}
	if now >= int64(expiry)+int64(a.ClockSkew.Seconds()) {
		return Claims{}, fmt.Errorf("token is expired")
	}
	if notBefore, ok := numericClaim(payload, "nbf"); ok && now < int64(notBefore)-int64(a.ClockSkew.Seconds()) {
		return Claims{}, fmt.Errorf("token is not active")
	}
	// Asymmetric trust always requires both configured pins. This remains
	// fail-closed even when AllowDev lets a local process start with an
	// incomplete asymmetric configuration: dev tokens take the separate path,
	// while a real JWT cannot bypass issuer/audience validation.
	if a.requiresIssuerAudience() {
		if strings.TrimSpace(a.Issuer) == "" || stringClaim(payload, "iss") != a.Issuer {
			return Claims{}, fmt.Errorf("token issuer mismatch")
		}
		if strings.TrimSpace(a.Audience) == "" || !audienceClaim(payload, a.Audience) {
			return Claims{}, fmt.Errorf("token audience mismatch")
		}
	} else if a.Issuer != "" && stringClaim(payload, "iss") != a.Issuer {
		return Claims{}, fmt.Errorf("token issuer mismatch")
	} else if a.Audience != "" && !audienceClaim(payload, a.Audience) {
		return Claims{}, fmt.Errorf("token audience mismatch")
	}
	subject, hasSubject, err := strictIdentityClaim(payload, "sub")
	if err != nil {
		return Claims{}, err
	}
	if !hasSubject {
		return Claims{}, fmt.Errorf("token must contain sub")
	}
	claims.Subject = subject
	claims.ClientID, err = clientIdentity(payload)
	if err != nil {
		return Claims{}, err
	}
	tenant, hasTenant, err := tenantClaim(payload, "tenant_id")
	if err != nil {
		return Claims{}, err
	}
	if hasTenant {
		claims.TenantID = tenant
	} else if tenant, hasTenant, err := tenantClaim(payload, "tenant"); err != nil {
		return Claims{}, err
	} else if hasTenant {
		claims.TenantID = tenant
	}
	claims.Roles = stringSliceClaim(payload, "roles")
	claims.Permissions = permissionsForRoles(claims.Roles)
	for _, permission := range stringSliceClaim(payload, "permissions") {
		claims.Permissions[permission] = true
	}
	if stringClaim(payload, "scope") != "" {
		for _, permission := range strings.Fields(stringClaim(payload, "scope")) {
			claims.Permissions[permission] = true
		}
	}
	claims.ConsoleAuditRead = claims.Permissions["admin:read"] || claims.Permissions["admin:*"]
	claims.CrossTenantAuditRead = claims.ConsoleAuditRead && claims.TenantID == ""
	claims.Platform = claims.Permissions["audit:platform:cross_tenant"] || contains(claims.Roles, "platform-admin")
	claims.Service = contains(claims.Roles, "service") || contains(claims.Roles, "event-writer")
	return claims, nil
}

func clientIdentity(payload map[string]any) (string, error) {
	clientID, hasClientID, err := strictIdentityClaim(payload, "client_id")
	if err != nil {
		return "", err
	}
	authorizedParty, hasAuthorizedParty, err := strictIdentityClaim(payload, "azp")
	if err != nil {
		return "", err
	}
	if hasClientID && hasAuthorizedParty && clientID != authorizedParty {
		return "", fmt.Errorf("token client identity mismatch")
	}
	if hasClientID {
		return clientID, nil
	}
	return authorizedParty, nil
}

// tenantClaim extracts the tenant context claim under the same strictness as
// sub/client_id, delegating the charset rule to domain.ValidTenantIDComponent —
// the canonical tenant rule every other tenant input uses. Control
// characters (incl. KeySeparator 0x1F), whitespace, '/'/'\\' and ':' are
// rejected so a padded or separator-embedding tenant_id can never reach the
// tenant consistency comparison (where " a" vs "a" would surface as a
// confusing 422 instead of an authentication failure) or collide with
// composite snapshot keys or stream frames.
func tenantClaim(payload map[string]any, name string) (string, bool, error) {
	value, present := payload[name]
	if !present {
		return "", false, nil
	}
	identity, ok := value.(string)
	if !ok {
		return "", false, fmt.Errorf("token %s claim is invalid", name)
	}
	if identity == "" {
		// Empty tenant scope is equivalent to absent: platform tokens
		// legitimately carry tenant_id: "" and select the tenant via the
		// ?tenant_id= query parameter (documented escape hatch for restore
		// approval). Strictness still applies to any non-empty value below.
		return "", false, nil
	}
	// Delegation to the canonical tenant rule — the same rejection set as
	// every other tenant input (control chars incl. KeySeparator 0x1F,
	// whitespace, '/' and '\\', plus ':' for stream-frame/dev-token
	// ambiguity). The previous ad-hoc IsControl/IsSpace loop is fully
	// subsumed: unicode.IsSpace covers every rune TrimSpace trims, so the
	// padding rejection is unchanged. The error text is preserved exactly so
	// token rejection stays a single non-oracular string.
	if err := domain.ValidTenantIDComponent(name, identity); err != nil {
		return "", false, fmt.Errorf("token %s claim is invalid", name)
	}
	return identity, true, nil
}

func strictIdentityClaim(payload map[string]any, name string) (string, bool, error) {
	value, present := payload[name]
	if !present {
		return "", false, nil
	}
	identity, ok := value.(string)
	if !ok || identity == "" || identity != strings.TrimSpace(identity) {
		return "", false, fmt.Errorf("token %s claim is invalid", name)
	}
	return identity, true, nil
}

func (c Claims) Allows(permission string) bool {
	return c.Permissions[permission] || c.Platform ||
		(permission == "audit:event:read" && c.ConsoleAuditRead)
}

func permissionsForRoles(roles []string) map[string]bool {
	permissions := map[string]bool{}
	for _, role := range roles {
		switch role {
		case "service", "event-writer":
			permissions["audit:event:write"] = true
		case "auditor", "tenant-auditor":
			permissions["audit:event:read"] = true
			permissions["audit:operation:read"] = true
		case "compliance":
			permissions["audit:event:read"] = true
			permissions["audit:operation:read"] = true
			permissions["audit:export:create"] = true
			permissions["audit:integrity:verify"] = true
			permissions["audit:legal_hold:manage"] = true
		case "tenant-admin":
			permissions["audit:event:read"] = true
			permissions["audit:operation:read"] = true
			permissions["audit:export:create"] = true
			permissions["audit:integrity:verify"] = true
			permissions["audit:legal_hold:manage"] = true
			permissions["audit:policy:read"] = true
			permissions["audit:policy:write"] = true
		case "platform-admin":
			permissions["audit:platform:cross_tenant"] = true
			permissions["audit:event:read"] = true
			permissions["audit:event:write"] = true
			permissions["audit:export:create"] = true
			permissions["audit:integrity:verify"] = true
			permissions["audit:legal_hold:manage"] = true
			permissions["audit:policy:read"] = true
			permissions["audit:policy:write"] = true
		}
	}
	return permissions
}

func stringClaim(values map[string]any, name string) string {
	value, _ := values[name].(string)
	return value
}

func stringSliceClaim(values map[string]any, name string) []string {
	var result []string
	switch value := values[name].(type) {
	case []any:
		for _, item := range value {
			if text, ok := item.(string); ok {
				result = append(result, text)
			}
		}
	case string:
		result = strings.Fields(value)
	}
	return result
}

func numericClaim(values map[string]any, name string) (float64, bool) {
	value, ok := values[name].(float64)
	return value, ok
}

func audienceClaim(values map[string]any, wanted string) bool {
	switch value := values["aud"].(type) {
	case string:
		return value == wanted
	case []any:
		for _, item := range value {
			if text, ok := item.(string); ok && text == wanted {
				return true
			}
		}
	}
	return false
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
