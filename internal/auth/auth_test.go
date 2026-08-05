package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
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
