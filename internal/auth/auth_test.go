package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

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
	authenticator := Authenticator{JWTSecret: "test-secret", AllowLocalHS256: true}
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
	if _, err := (Authenticator{JWTSecret: "test-secret", AllowLocalHS256: true}).AuthenticateToken(signJWT(t, payload)); err == nil {
		t.Fatal("expected conflicting client_id and azp to be rejected")
	}
}

func TestJWTRejectsMalformedClientIdentityClaim(t *testing.T) {
	payload := map[string]any{
		"sub": "service-subject", "tenant_id": "tenant-a", "exp": time.Now().Add(time.Hour).Unix(),
		"client_id": []string{"snaplink-commerce"}, "azp": "snaplink-commerce",
	}
	if _, err := (Authenticator{JWTSecret: "test-secret", AllowLocalHS256: true}).AuthenticateToken(signJWT(t, payload)); err == nil {
		t.Fatal("expected a non-string client_id to be rejected")
	}
}

func TestDevAuthRejectedDespiteConfiguredTrustSource(t *testing.T) {
	// Genuinely new coverage: with a real trust source configured and
	// AllowDev=false, rejection must come from the dev gate, not the
	// missing-trust-source path exercised by (Authenticator{}) today.
	authenticator := Authenticator{JWTSecret: "test-secret", AllowLocalHS256: true}
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

func TestDevAuthAcceptedWhenAllowlisted(t *testing.T) {
	// D4 companion: AllowDev stays a complete runtime trust source (the
	// fail-closed flip changes only the default and the check-config gate),
	// and allowlisted dev auth still rejects malformed dev tokens and
	// non-dev garbage while real JWTs keep verifying.
	if err := (Authenticator{AllowDev: true}).ValidateConfiguration(); err != nil {
		t.Fatalf("AllowDev alone must remain a valid runtime trust source: %v", err)
	}
	authenticator := Authenticator{JWTSecret: "test-secret", AllowLocalHS256: true, AllowDev: true}
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
	mac := hmac.New(sha256.New, []byte("test-secret"))
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
