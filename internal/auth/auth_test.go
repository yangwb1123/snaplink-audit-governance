package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// testHMACSecret is the shared >=32-byte local HS256 test key (256-bit,
// matching minJWTSecretBytes): signJWT and every Authenticator fixture must
// use the same constant so minted tokens verify against the configured trust
// source. It must stay >=32 bytes or every HS256 fixture fails the gate.
const testHMACSecret = "test-secret-0123456789abcdefghijklmnopqrs"

func TestDevTokenClaims(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer dev:tenant-a:tenant-admin")
	claims, err := (Authenticator{AllowDev: true}).Authenticate(r)
	if err != nil {
		t.Fatal(err)
	}
	if claims.TenantID != "tenant-a" || claims.ClientID != "tenant-a" || !claims.Allows("audit:export:create") {
		t.Fatalf("unexpected claims: %+v", claims)
	}
}

func TestDevTokenCanNameSourceClient(t *testing.T) {
	claims, err := (Authenticator{AllowDev: true}).AuthenticateToken("dev:tenant-a:service:snaplink-commerce")
	if err != nil {
		t.Fatal(err)
	}
	if claims.ClientID != "snaplink-commerce" {
		t.Fatalf("client ID = %q", claims.ClientID)
	}
}

func TestDevAuthRejectsWithoutOptIn(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	if _, err := (Authenticator{}).Authenticate(r); err == nil {
		t.Fatal("expected development token to be rejected")
	}
}

func TestJWTClientIdentityUsesClientIDOrAZP(t *testing.T) {
	authenticator := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}
	base := map[string]any{"sub": "service-subject", "tenant_id": "tenant-a", "exp": time.Now().Add(time.Hour).Unix()}
	withClientID := cloneClaims(base)
	withClientID["client_id"] = "snaplink-commerce"
	claims, err := authenticator.AuthenticateToken(signJWT(t, withClientID))
	if err != nil || claims.ClientID != "snaplink-commerce" {
		t.Fatalf("client_id was not accepted: claims=%+v err=%v", claims, err)
	}
	withAZP := cloneClaims(base)
	withAZP["azp"] = "compatible-client"
	claims, err = authenticator.AuthenticateToken(signJWT(t, withAZP))
	if err != nil || claims.ClientID != "compatible-client" {
		t.Fatalf("azp fallback was not accepted: claims=%+v err=%v", claims, err)
	}
	claims, err = authenticator.AuthenticateToken(signJWT(t, base))
	if err != nil || claims.ClientID != "" {
		t.Fatalf("sub must not become client identity: claims=%+v err=%v", claims, err)
	}
}

func TestJWTRejectsConflictingClientIdentityClaims(t *testing.T) {
	payload := map[string]any{
		"sub": "service-subject", "tenant_id": "tenant-a", "exp": time.Now().Add(time.Hour).Unix(),
		"client_id": "snaplink-commerce", "azp": "attacker-client",
	}
	if _, err := (Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}).AuthenticateToken(signJWT(t, payload)); err == nil {
		t.Fatal("expected conflicting client_id and azp to be rejected")
	}
}

func TestJWTRejectsMalformedClientIdentityClaim(t *testing.T) {
	payload := map[string]any{
		"sub": "service-subject", "tenant_id": "tenant-a", "exp": time.Now().Add(time.Hour).Unix(),
		"client_id": []string{"snaplink-commerce"}, "azp": "snaplink-commerce",
	}
	if _, err := (Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}).AuthenticateToken(signJWT(t, payload)); err == nil {
		t.Fatal("expected a non-string client_id to be rejected")
	}
}

func TestJWTRejectsWhitespacePaddedSubject(t *testing.T) {
	// H-1 canonicalization: a whitespace-padded sub would pass the old
	// empty-string check while comparing unequal to the actor recorded by
	// CreateRestore, letting the same principal evade a same-actor check
	// that compares exact strings.
	payload := map[string]any{
		"sub": " service-subject ", "tenant_id": "tenant-a", "exp": time.Now().Add(time.Hour).Unix(),
	}
	if _, err := (Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}).AuthenticateToken(signJWT(t, payload)); err == nil {
		t.Fatal("expected a whitespace-padded sub to be rejected")
	}
}

func TestJWTRejectsNonStringSubject(t *testing.T) {
	payload := map[string]any{
		"sub": []string{"service-subject"}, "tenant_id": "tenant-a", "exp": time.Now().Add(time.Hour).Unix(),
	}
	if _, err := (Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}).AuthenticateToken(signJWT(t, payload)); err == nil {
		t.Fatal("expected a non-string sub to be rejected")
	}
}

func TestJWTRejectsMissingSubject(t *testing.T) {
	payload := map[string]any{
		"tenant_id": "tenant-a", "exp": time.Now().Add(time.Hour).Unix(),
	}
	if _, err := (Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}).AuthenticateToken(signJWT(t, payload)); err == nil {
		t.Fatal("expected a missing sub to be rejected")
	}
}

func TestJWTRejectsMissingExpiry(t *testing.T) {
	// A bearer token without exp never expires: a stolen token would stay
	// replayable forever in an audit system. The verifier must fail closed
	// when the claim is absent, even if the IdP omitted it.
	payload := map[string]any{
		"sub": "service-subject", "tenant_id": "tenant-a",
	}
	if _, err := (Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}).AuthenticateToken(signJWT(t, payload)); err == nil || !strings.Contains(err.Error(), "exp") {
		t.Fatalf("missing exp was not rejected: %v", err)
	}
}

func TestJWTRejectsPaddedTenantIDClaim(t *testing.T) {
	// Mirrors the padded-sub rejection: a padded or control-character
	// tenant_id must fail authentication (not reach the envelope-mismatch
	// 422 comparison with confusing raw-string equality).
	authenticator := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}
	cases := []map[string]any{
		{"sub": "service-subject", "tenant_id": " tenant-a", "exp": time.Now().Add(time.Hour).Unix()},
		{"sub": "service-subject", "tenant_id": "tenant-a\t", "exp": time.Now().Add(time.Hour).Unix()},
		{"sub": "service-subject", "tenant_id": "tenant\x1fa", "exp": time.Now().Add(time.Hour).Unix()},
		{"sub": "service-subject", "tenant": " tenant-a", "exp": time.Now().Add(time.Hour).Unix()},
	}
	for _, payload := range cases {
		if _, err := authenticator.AuthenticateToken(signJWT(t, payload)); err == nil {
			t.Fatalf("expected padded/control-character tenant_id to be rejected: %v", payload)
		}
	}
	// Positive companion: an exact-string tenant_id still authenticates.
	claims, err := authenticator.AuthenticateToken(signJWT(t, map[string]any{"sub": "service-subject", "tenant_id": "tenant-a", "exp": time.Now().Add(time.Hour).Unix()}))
	if err != nil || claims.TenantID != "tenant-a" {
		t.Fatalf("valid tenant_id must authenticate: claims=%+v err=%v", claims, err)
	}
}

func TestJWTSubjectSurvivesStrictParsing(t *testing.T) {
	// Positive companion: strict parsing keeps exact-string subjects and
	// the plain missing-sub message intact.
	payload := map[string]any{
		"sub": "service-subject", "tenant_id": "tenant-a", "exp": time.Now().Add(time.Hour).Unix(),
	}
	claims, err := (Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}).AuthenticateToken(signJWT(t, payload))
	if err != nil {
		t.Fatalf("valid sub must authenticate: %v", err)
	}
	if claims.Subject != "service-subject" {
		t.Fatalf("subject = %q", claims.Subject)
	}
	payload = map[string]any{
		"tenant_id": "tenant-a", "exp": time.Now().Add(time.Hour).Unix(),
	}
	if _, err := (Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}).AuthenticateToken(signJWT(t, payload)); err == nil || !strings.Contains(err.Error(), "token must contain sub") {
		t.Fatalf("missing sub error = %v, want preserved message", err)
	}
}

func TestDevAuthRejectedDespiteConfiguredTrustSource(t *testing.T) {
	// Genuinely new coverage: with a real trust source configured and
	// AllowDev=false, rejection must come from the dev gate, not the
	// missing-trust-source path exercised by (Authenticator{}) today.
	authenticator := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}
	if _, err := authenticator.AuthenticateToken("dev:tenant-a:auditor"); err == nil {
		t.Fatal("dev token must be rejected when AllowDev is false")
	} else if strings.Contains(err.Error(), "no JWT verification trust source is configured") {
		t.Fatalf("rejection must come from the dev gate, not the missing-trust-source path: %v", err)
	}
	payload := map[string]any{"sub": "service-subject", "tenant_id": "tenant-a", "exp": time.Now().Add(time.Hour).Unix()}
	if _, err := authenticator.AuthenticateToken(signJWT(t, payload)); err != nil {
		t.Fatalf("a valid JWT with the same trust source must still authenticate: %v", err)
	}
}

// TestDevAuthRejectsKeyFramingSubjects is AC-4: the dev-token subject
// becomes the tenant context, a composite-key component, so a subject
// embedding KeySeparator (0x1F) — or other control characters, whitespace
// or path separators — is rejected with the exact malformed-token error
// text (no oracle distinguishing valid-format from invalid-charset).
func TestDevAuthRejectsKeyFramingSubjects(t *testing.T) {
	authenticator := Authenticator{AllowDev: true}
	for _, token := range []string{
		"dev:a\x1fb:auditor",
		"dev:a b:auditor",
		"dev:a/b:auditor",
		"dev:a\\b:auditor",
		"dev:\x00:auditor",
		"dev:\n:auditor",
	} {
		if _, err := authenticator.AuthenticateToken(token); err == nil {
			t.Errorf("dev token %q must be rejected", token)
		} else if err.Error() != "invalid development token" {
			t.Errorf("dev token %q error = %q, want the canonical malformed-token text", token, err.Error())
		}
	}
	// Positive controls: valid subjects keep authenticating with intact
	// tenant/platform flags, including the service-client form.
	claims, err := authenticator.AuthenticateToken("dev:tenant-a:auditor")
	if err != nil {
		t.Fatalf("valid dev token rejected: %v", err)
	}
	if claims.TenantID != "tenant-a" || claims.Platform {
		t.Fatalf("unexpected tenant claims: %+v", claims)
	}
	platform, err := authenticator.AuthenticateToken("dev:platform:platform-admin")
	if err != nil {
		t.Fatalf("valid platform dev token rejected: %v", err)
	}
	if !platform.Platform || platform.TenantID != "platform" {
		t.Fatalf("unexpected platform claims: %+v", platform)
	}
	if _, err := authenticator.AuthenticateToken("dev:tenant-a:service:crm"); err != nil {
		t.Fatalf("valid service dev token rejected: %v", err)
	}
}

// TestJWTRejectsKeyFramingTenantClaims is REQ-6's acceptance: the JWT
// tenant_id/tenant claim path delegates to the canonical key-framing rule,
// so the same corpus AC-4 applies to dev tokens rejects here too — control
// characters (incl. KeySeparator 0x1F), whitespace and '/'/'\\', with the
// exact generic claim-invalid text for every class (no charset oracle).
// Empty claims stay legal (platform escape-hatch: TenantID == "").
func TestJWTRejectsKeyFramingTenantClaims(t *testing.T) {
	authenticator := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}
	for _, name := range []string{"tenant_id", "tenant"} {
		for _, bad := range []string{"a/b", `a\b`, "a\x1fb", "a b", "\x00", "\n", " a"} {
			payload := map[string]any{"sub": "service-subject", name: bad, "exp": time.Now().Add(time.Hour).Unix()}
			_, err := authenticator.AuthenticateToken(signJWT(t, payload))
			if err == nil {
				t.Errorf("%s claim %q must be rejected", name, bad)
				continue
			}
			if want := fmt.Sprintf("token %s claim is invalid", name); err.Error() != want {
				t.Errorf("%s claim %q error = %q, want the canonical generic text %q", name, bad, err.Error(), want)
			}
		}
	}
	// Positive controls: exact-string claims authenticate with the expected
	// tenant context, and empty claims keep TenantID == "" (platform
	// escape-hatch preserved by the early return).
	for _, tc := range []struct {
		claimName string
		value     string
		wantID    string
	}{
		{"tenant_id", "tenant-a", "tenant-a"},
		{"tenant", "tenant-a", "tenant-a"},
		{"tenant_id", "", ""},
		{"tenant", "", ""},
	} {
		payload := map[string]any{"sub": "service-subject", tc.claimName: tc.value, "exp": time.Now().Add(time.Hour).Unix()}
		claims, err := authenticator.AuthenticateToken(signJWT(t, payload))
		if err != nil {
			t.Fatalf("%s=%q must authenticate: %v", tc.claimName, tc.value, err)
		}
		if claims.TenantID != tc.wantID {
			t.Fatalf("%s=%q → TenantID %q, want %q", tc.claimName, tc.value, claims.TenantID, tc.wantID)
		}
	}
}

func TestDevAuthAcceptedWhenAllowlisted(t *testing.T) {
	// D4 companion: AllowDev stays a complete runtime trust source (the
	// fail-closed flip changes only the default and the check-config gate),
	// and allowlisted dev auth still rejects malformed dev tokens and
	// non-dev garbage while real JWTs keep verifying.
	if err := (Authenticator{AllowDev: true}).ValidateConfiguration(); err != nil {
		t.Fatalf("AllowDev alone must remain a valid runtime trust source: %v", err)
	}
	authenticator := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true, AllowDev: true}
	claims, err := authenticator.AuthenticateToken("dev:tenant-a:auditor")
	if err != nil {
		t.Fatalf("allowlisted dev auth must accept dev tokens: %v", err)
	}
	if claims.TenantID != "tenant-a" || !claims.Allows("audit:event:read") {
		t.Fatalf("unexpected dev claims: %+v", claims)
	}
	if _, err := authenticator.AuthenticateToken("dev:only"); err == nil {
		t.Fatal("malformed dev token must be rejected even when allowlisted")
	}
	if _, err := authenticator.AuthenticateToken("not-a-dev-token"); err == nil {
		t.Fatal("non-dev garbage must be rejected even when allowlisted")
	}
	payload := map[string]any{"sub": "service-subject", "tenant_id": "tenant-a", "exp": time.Now().Add(time.Hour).Unix()}
	if _, err := authenticator.AuthenticateToken(signJWT(t, payload)); err != nil {
		t.Fatalf("real JWTs must still verify when dev auth is allowlisted: %v", err)
	}
}

// TestDevTokenPlatformRequiresPlatformAdminRole is AC-1/AC-4: Platform on a
// dev token is derived from the JWT rule (permissionsForRoles-derived
// audit:platform:cross_tenant or the platform-admin role), never from the
// subject/tenant name. A subject of "platform" with any non-admin role must
// stay tenant-scoped: Platform false, cross-tenant permission denied, tenant
// context preserved. Platform is true iff the platform-admin role is present,
// independent of the subject name.
func TestDevTokenPlatformRequiresPlatformAdminRole(t *testing.T) {
	authenticator := Authenticator{AllowDev: true}
	// Roles and role combos from permissionsForRoles; platform-admin is the
	// only role that maps to audit:platform:cross_tenant.
	roleCases := []struct {
		roles    string
		platform bool
		service  bool
	}{
		{"auditor", false, false},
		{"tenant-auditor", false, false},
		{"compliance", false, false},
		{"tenant-admin", false, false},
		{"service", false, true},
		{"event-writer", false, true},
		{"auditor,compliance", false, false},
		{"tenant-admin,compliance", false, false},
		{"service,event-writer", false, true},
		{"platform-admin", true, false},
		{"platform-admin,auditor", true, false},
	}
	for _, subject := range []string{"platform", "tenant-a", "tenant-b"} {
		for _, roleCase := range roleCases {
			name := "dev:" + subject + ":" + roleCase.roles
			claims, err := authenticator.AuthenticateToken(name)
			if err != nil {
				t.Errorf("%s: unexpected error: %v", name, err)
				continue
			}
			if claims.Platform != roleCase.platform {
				t.Errorf("%s: Platform=%v, want %v (role-based, subject must not matter)", name, claims.Platform, roleCase.platform)
			}
			if got := claims.Allows("audit:platform:cross_tenant"); got != roleCase.platform {
				t.Errorf("%s: Allows(audit:platform:cross_tenant)=%v, want %v", name, got, roleCase.platform)
			}
			if claims.TenantID != subject {
				t.Errorf("%s: TenantID=%q, want %q (tenant context preserved)", name, claims.TenantID, subject)
			}
			if claims.Service != roleCase.service {
				t.Errorf("%s: Service=%v, want %v", name, claims.Service, roleCase.service)
			}
		}
	}
	// AC-1.2 unit form: 4-part service token with a "platform" subject must
	// not flip Platform either (L1 review fold).
	claims, err := authenticator.AuthenticateToken("dev:platform:service:crm")
	if err != nil {
		t.Fatal(err)
	}
	if claims.Platform || !claims.Service || claims.ClientID != "crm" || claims.TenantID != "platform" {
		t.Fatalf("dev:platform:service:crm -> Platform=%v Service=%v ClientID=%q TenantID=%q, want non-Platform service claims", claims.Platform, claims.Service, claims.ClientID, claims.TenantID)
	}
}

// TestJWTPlatformTenantWithAuditorRoleIsNotPlatform is AC-3: the JWT parity
// pin. A JWT carrying tenant_id "platform" with role auditor must be
// non-Platform (tenant context preserved, privilege absent) — the same shape
// as the fixed dev path, so neither trust path can drift.
func TestJWTPlatformTenantWithAuditorRoleIsNotPlatform(t *testing.T) {
	authenticator := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}
	payload := map[string]any{
		"sub":       "subject-a",
		"tenant_id": "platform",
		"roles":     []string{"auditor"},
		"exp":       time.Now().Add(time.Hour).Unix(),
	}
	claims, err := authenticator.AuthenticateToken(signJWT(t, payload))
	if err != nil {
		t.Fatal(err)
	}
	if claims.Platform {
		t.Fatalf("JWT tenant_id=platform + role auditor must not be Platform: %+v", claims)
	}
	if claims.Allows("audit:platform:cross_tenant") {
		t.Fatalf("JWT tenant_id=platform + role auditor must not allow cross-tenant: %+v", claims)
	}
	if claims.TenantID != "platform" {
		t.Fatalf("JWT tenant context must be preserved: TenantID=%q, want platform", claims.TenantID)
	}
	// Positive control: platform-admin role stays Platform via the same path.
	admin := cloneClaims(payload)
	admin["roles"] = []string{"platform-admin"}
	adminClaims, err := authenticator.AuthenticateToken(signJWT(t, admin))
	if err != nil {
		t.Fatal(err)
	}
	if !adminClaims.Platform || !adminClaims.Allows("audit:platform:cross_tenant") {
		t.Fatalf("JWT platform-admin role must be Platform: %+v", adminClaims)
	}
}

// TestAuthenticateTokenContextRejectsShortHS256Secret is AC-2: the
// ValidateConfiguration gate fires before any token parsing or signature
// verification, so a short-secret HS256 config rejects even a well-formed
// token (gate at auth.go precedes parseJWT), and fails closed under AllowDev
// with a dev token. Positive control: a compliant 32-byte secret
// authenticates a valid signed token.
func TestAuthenticateTokenContextRejectsShortHS256Secret(t *testing.T) {
	short := Authenticator{JWTSecret: "short", AllowLocalHS256: true}
	if _, err := short.AuthenticateTokenContext(context.Background(), "not-a-token"); err == nil || !strings.Contains(err.Error(), "at least 32 bytes") {
		t.Fatalf("per-request gate must fire before parsing, got: %v", err)
	}
	// Fail-closed with dev mode: a weak HS256 trust source blocks the
	// process even when dev tokens are enabled.
	dev := Authenticator{JWTSecret: "short", AllowLocalHS256: true, AllowDev: true}
	if _, err := dev.AuthenticateToken("dev:tenant-a:auditor"); err == nil || !strings.Contains(err.Error(), "at least 32 bytes") {
		t.Fatalf("dev fail-closed must surface the length error, got: %v", err)
	}
	// Positive control: a compliant secret authenticates a valid token.
	strong := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}
	payload := map[string]any{"sub": "service-subject", "tenant_id": "tenant-a", "exp": time.Now().Add(time.Hour).Unix()}
	claims, err := strong.AuthenticateToken(signJWT(t, payload))
	if err != nil || claims.TenantID != "tenant-a" {
		t.Fatalf("compliant secret must authenticate a valid token: claims=%+v err=%v", claims, err)
	}
}

func signJWT(t *testing.T, payload map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": "HS256", "typ": "at+jwt"})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	signed := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, []byte(testHMACSecret))
	_, _ = mac.Write([]byte(signed))
	return signed + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func cloneClaims(source map[string]any) map[string]any {
	result := make(map[string]any, len(source)+1)
	for key, value := range source {
		result[key] = value
	}
	return result
}
