package auth

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
)

type signingFixture struct {
	name       string
	algorithm  jwa.SignatureAlgorithm
	privateKey any
	publicKey  jwk.Key
	keyID      string
}

func TestRemoteJWKSSupportedAlgorithms(t *testing.T) {
	for _, fixture := range signingFixtures(t) {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			server := serveJWK(t, fixture.publicKey)
			defer server.Close()
			authenticator := remoteAuthenticator(server.URL)
			claims, err := authenticator.AuthenticateToken(signFixtureJWT(t, fixture, validJWTClaims()))
			if err != nil {
				t.Fatal(err)
			}
			if claims.ClientID != "snaplink-commerce" || claims.TenantID != "" {
				t.Fatalf("identity binding regression: %+v", claims)
			}
		})
	}
}

func TestRemoteJWKSRejectsKeyMetadataMismatch(t *testing.T) {
	fixtures := signingFixtures(t)
	rsaFixture := fixtureNamed(t, fixtures, "RS256")
	p256Fixture := fixtureNamed(t, fixtures, "ES256")
	tests := []struct {
		name string
		body []byte
	}{
		{"alg", mutateJWK(t, rsaFixture.publicKey, map[string]any{"alg": "PS256"})},
		{"kty", mutateJWK(t, p256Fixture.publicKey, map[string]any{"kid": rsaFixture.keyID, "alg": "RS256"})},
		{"use", mutateJWK(t, rsaFixture.publicKey, map[string]any{"use": "enc"})},
		{"key_ops", mutateJWK(t, rsaFixture.publicKey, map[string]any{"key_ops": []string{"sign"}})},
	}
	token := signFixtureJWT(t, rsaFixture, validJWTClaims())
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := serveJWKSBody(test.body)
			defer server.Close()
			if _, err := remoteAuthenticator(server.URL).AuthenticateToken(token); err == nil {
				t.Fatal("expected mismatched JWK metadata to be rejected")
			}
		})
	}
}

func TestRemoteJWKSRejectsWrongECDSACurve(t *testing.T) {
	fixtures := signingFixtures(t)
	p256Fixture := fixtureNamed(t, fixtures, "ES256")
	p384Fixture := fixtureNamed(t, fixtures, "ES384")
	body := mutateJWK(t, p384Fixture.publicKey, map[string]any{
		"kid": p256Fixture.keyID, "alg": "ES256", "use": "sig", "key_ops": []string{"verify"},
	})
	server := serveJWKSBody(body)
	defer server.Close()
	_, err := remoteAuthenticator(server.URL).AuthenticateToken(signFixtureJWT(t, p256Fixture, validJWTClaims()))
	if err == nil || !strings.Contains(err.Error(), "curve") {
		t.Fatalf("wrong ECDSA curve was not rejected before verification: %v", err)
	}
}

func TestRemoteJWKSRejectsNoneHMACAndUnlistedAlgorithms(t *testing.T) {
	fixture := fixtureNamed(t, signingFixtures(t), "RS256")
	server := serveJWK(t, fixture.publicKey)
	defer server.Close()
	authenticator := remoteAuthenticator(server.URL)
	payload, err := json.Marshal(validJWTClaims())
	if err != nil {
		t.Fatal(err)
	}
	none := compactUnsignedToken(t, map[string]any{"alg": "none", "kid": fixture.keyID}, payload)
	if _, err := authenticator.AuthenticateToken(none); err == nil {
		t.Fatal("alg none was accepted")
	}
	hmacToken := signHMAC(t, validJWTClaims(), []byte("remote-secret"))
	if _, err := authenticator.AuthenticateToken(hmacToken); err == nil {
		t.Fatal("HS256 downgrade against remote JWKS was accepted")
	}
	hmac384, err := jws.Sign(payload, jws.WithKey(jwa.HS384(), []byte("remote-secret")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authenticator.AuthenticateToken(string(hmac384)); err == nil {
		t.Fatal("HS384 token was accepted")
	}
	rsa384 := fixture
	rsa384.algorithm = jwa.RS384()
	if _, err := authenticator.AuthenticateToken(signFixtureJWT(t, rsa384, validJWTClaims())); err == nil {
		t.Fatal("unlisted RS384 algorithm was accepted")
	}
}

func TestLocalHMACIsExplicitAndMutuallyExclusive(t *testing.T) {
	token := signHMAC(t, validJWTClaims(), []byte("local-secret"))
	withoutOptIn := Authenticator{JWTSecret: "local-secret"}
	if _, err := withoutOptIn.AuthenticateToken(token); err == nil {
		t.Fatal("HS256 worked without explicit local opt-in")
	}
	localOnly := Authenticator{JWTSecret: "local-secret", AllowLocalHS256: true}
	if _, err := localOnly.AuthenticateToken(token); err != nil {
		t.Fatalf("explicit local HS256 failed: %v", err)
	}
	mixed := Authenticator{JWTSecret: "local-secret", AllowLocalHS256: true, JWKSURL: "https://issuer.example/jwks"}
	if _, err := mixed.AuthenticateToken(token); err == nil {
		t.Fatal("mixed symmetric and remote trust configuration was accepted")
	}
}

func TestLocalPublicKeyPinsAlgorithmAgainstHMACConfusion(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	publicPEM := marshalPublicKey(t, &privateKey.PublicKey)
	authenticator := Authenticator{JWTPublicKeyPEM: publicPEM, JWTPublicKeyAlgorithm: "RS256"}
	hmacToken := signHMAC(t, validJWTClaims(), []byte(publicPEM))
	if _, err := authenticator.AuthenticateToken(hmacToken); err == nil {
		t.Fatal("RSA public key was accepted as an HMAC secret")
	}
	fixture := newFixture(t, "RS256", jwa.RS256(), privateKey, &privateKey.PublicKey)
	if _, err := authenticator.AuthenticateToken(signFixtureJWT(t, fixture, validJWTClaims())); err != nil {
		t.Fatalf("pinned RS256 verification failed: %v", err)
	}
}

func TestJWKSURLRequiresHTTPSOrExplicitLoopback(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		loopback  bool
		wantError bool
	}{
		{"https", "https://snaplink.example/.well-known/jwks.json", false, false},
		{"remote HTTP", "http://snaplink.example/.well-known/jwks.json", true, true},
		{"loopback HTTP default", "http://127.0.0.1:8080/jwks", false, true},
		{"loopback HTTP explicit", "http://127.0.0.1:8080/jwks", true, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := (Authenticator{JWKSURL: test.url, AllowInsecureJWKSLoopback: test.loopback}).ValidateConfiguration()
			if (err != nil) != test.wantError {
				t.Fatalf("ValidateConfiguration() error=%v wantError=%v", err, test.wantError)
			}
		})
	}
}

func TestVerifiedJWTClaimsChecksRemainEnforced(t *testing.T) {
	fixture := fixtureNamed(t, signingFixtures(t), "EdDSA")
	server := serveJWK(t, fixture.publicKey)
	defer server.Close()
	authenticator := remoteAuthenticator(server.URL)
	tests := []struct {
		name   string
		claims map[string]any
	}{
		{"expired", mergeClaims(validJWTClaims(), map[string]any{"exp": time.Now().Add(-time.Minute).Unix()})},
		{"issuer", mergeClaims(validJWTClaims(), map[string]any{"iss": "https://other.example"})},
		{"audience", mergeClaims(validJWTClaims(), map[string]any{"aud": "other-audience"})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := authenticator.AuthenticateToken(signFixtureJWT(t, fixture, test.claims)); err == nil {
				t.Fatal("expected claim validation failure")
			}
		})
	}
}

func signingFixtures(t *testing.T) []signingFixture {
	t.Helper()
	edPublic, edPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p256 := generateECDSA(t, elliptic.P256())
	p384 := generateECDSA(t, elliptic.P384())
	p521 := generateECDSA(t, elliptic.P521())
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return []signingFixture{
		newFixture(t, "EdDSA", jwa.EdDSA(), edPrivate, edPublic),
		newFixture(t, "ES256", jwa.ES256(), p256, &p256.PublicKey),
		newFixture(t, "ES384", jwa.ES384(), p384, &p384.PublicKey),
		newFixture(t, "ES512", jwa.ES512(), p521, &p521.PublicKey),
		newFixture(t, "RS256", jwa.RS256(), rsaKey, &rsaKey.PublicKey),
		newFixture(t, "PS256", jwa.PS256(), rsaKey, &rsaKey.PublicKey),
	}
}

func newFixture(t *testing.T, name string, algorithm jwa.SignatureAlgorithm, privateKey, publicKey any) signingFixture {
	t.Helper()
	key, err := jwk.Import(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	keyID := "kid-" + strings.ToLower(name)
	for field, value := range map[string]any{
		jwk.KeyIDKey: keyID, jwk.AlgorithmKey: algorithm,
		jwk.KeyUsageKey: "sig",
	} {
		if err := key.Set(field, value); err != nil {
			t.Fatal(err)
		}
	}
	if name == "PS256" {
		if err := key.Set(jwk.KeyOpsKey, jwk.KeyOperationList{jwk.KeyOpVerify}); err != nil {
			t.Fatal(err)
		}
	}
	return signingFixture{name: name, algorithm: algorithm, privateKey: privateKey, publicKey: key, keyID: keyID}
}

func generateECDSA(t *testing.T, curve elliptic.Curve) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func fixtureNamed(t *testing.T, fixtures []signingFixture, name string) signingFixture {
	t.Helper()
	for _, fixture := range fixtures {
		if fixture.name == name {
			return fixture
		}
	}
	t.Fatalf("fixture %s not found", name)
	return signingFixture{}
}

func validJWTClaims() map[string]any {
	return map[string]any{
		"sub": "snaplink-commerce", "client_id": "snaplink-commerce",
		"iss": "https://snaplink.example", "aud": "audit-governance", "scope": "audit:event:write",
		"exp": time.Now().Add(time.Hour).Unix(),
	}
}

func mergeClaims(base, updates map[string]any) map[string]any {
	result := make(map[string]any, len(base)+len(updates))
	for key, value := range base {
		result[key] = value
	}
	for key, value := range updates {
		result[key] = value
	}
	return result
}

func signFixtureJWT(t *testing.T, fixture signingFixture, claims map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	headers := jws.NewHeaders()
	if err := headers.Set(jws.KeyIDKey, fixture.keyID); err != nil {
		t.Fatal(err)
	}
	signed, err := jws.Sign(payload, jws.WithKey(fixture.algorithm, fixture.privateKey, jws.WithProtectedHeaders(headers)))
	if err != nil {
		t.Fatal(err)
	}
	return string(signed)
}

func signHMAC(t *testing.T, claims map[string]any, secret []byte) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	header, err := json.Marshal(map[string]any{"alg": "HS256", "typ": "at+jwt"})
	if err != nil {
		t.Fatal(err)
	}
	signed := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(signed))
	return signed + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func compactUnsignedToken(t *testing.T, header map[string]any, payload []byte) string {
	t.Helper()
	encodedHeader, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(encodedHeader) + "." + base64.RawURLEncoding.EncodeToString(payload) + "."
}

func remoteAuthenticator(jwksURL string) Authenticator {
	return Authenticator{
		JWKSURL: jwksURL, AllowInsecureJWKSLoopback: true,
		Issuer: "https://snaplink.example", Audience: "audit-governance",
	}
}

func serveJWK(t *testing.T, key jwk.Key) *httptest.Server {
	t.Helper()
	set := jwk.NewSet()
	if err := set.AddKey(key); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	return serveJWKSBody(body)
}

func serveJWKSBody(body []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write(body)
	}))
}

func mutateJWK(t *testing.T, key jwk.Key, updates map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(key)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for name, value := range updates {
		fields[name] = value
	}
	body, err := json.Marshal(map[string]any{"keys": []any{fields}})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func marshalPublicKey(t *testing.T, publicKey any) string {
	t.Helper()
	encoded, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded}))
}

// TestJWKSCacheFetchesOnceWithinTTLAndNeverForGarbage pins the M-2 contract:
// the parsed JWKS set is cached, so N valid authentications within the
// refresh interval cause exactly one fetch, and syntactically valid garbage
// tokens (which reach key lookup but fail signature verification) cause zero
// additional fetches. Without the cache every request, including
// unauthenticated garbage, amplified a full HTTPS fetch to the IdP.
func TestJWKSCacheFetchesOnceWithinTTLAndNeverForGarbage(t *testing.T) {
	fixture := fixtureNamed(t, signingFixtures(t), "RS256")
	var fetches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		body, err := json.Marshal(jwkSetOf(t, fixture.publicKey))
		if err != nil {
			t.Error(err)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write(body)
	}))
	defer server.Close()
	authenticator := remoteAuthenticator(server.URL)
	authenticator.JWKSRefreshInterval = time.Hour
	valid := signFixtureJWT(t, fixture, validJWTClaims())
	for i := 0; i < 10; i++ {
		if _, err := authenticator.AuthenticateToken(valid); err != nil {
			t.Fatalf("valid token %d: %v", i, err)
		}
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("fetches=%d for 10 valid tokens within TTL, want 1", got)
	}
	for i := 0; i < 10; i++ {
		if _, err := authenticator.AuthenticateToken("garbage.garbage.garbage"); err == nil {
			t.Fatal("garbage token accepted")
		}
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("fetches=%d after 10 garbage tokens, want still 1", got)
	}
}

// TestJWKSCacheServesStaleSetDuringIdPOutage pins the stale-on-outage
// posture: once a set was fetched successfully, an IdP outage must not turn
// into a total auth outage; the cached set keeps authenticating until the
// refresh interval recovers. A set that never fetched stays fail-closed.
func TestJWKSCacheServesStaleSetDuringIdPOutage(t *testing.T) {
	fixture := fixtureNamed(t, signingFixtures(t), "RS256")
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) > 1 {
			http.Error(response, "idp down", http.StatusServiceUnavailable)
			return
		}
		body, err := json.Marshal(jwkSetOf(t, fixture.publicKey))
		if err != nil {
			t.Error(err)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write(body)
	}))
	defer server.Close()
	authenticator := remoteAuthenticator(server.URL)
	authenticator.JWKSRefreshInterval = 10 * time.Millisecond
	valid := signFixtureJWT(t, fixture, validJWTClaims())
	if _, err := authenticator.AuthenticateToken(valid); err != nil {
		t.Fatalf("prime cache: %v", err)
	}
	time.Sleep(30 * time.Millisecond) // let the TTL expire so the next auth refetches
	if _, err := authenticator.AuthenticateToken(valid); err != nil {
		t.Fatalf("stale set must keep authentication working during IdP outage: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests=%d, want 2 (prime + failed refresh)", got)
	}
}

// TestJWKSCacheRefreshesOnKidMiss pins the rotation path: a token whose kid
// is absent from the cached set forces exactly one refresh, so a rotated-in
// key becomes usable without waiting for the TTL, and the refreshed set is
// then reused without further fetches.
func TestJWKSCacheRefreshesOnKidMiss(t *testing.T) {
	fixtures := signingFixtures(t)
	oldFixture := fixtureNamed(t, fixtures, "ES256")
	newFixture := fixtureNamed(t, fixtures, "ES384")
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		key := oldFixture.publicKey
		if requests.Add(1) > 1 {
			key = newFixture.publicKey
		}
		body, err := json.Marshal(jwkSetOf(t, key))
		if err != nil {
			t.Error(err)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write(body)
	}))
	defer server.Close()
	authenticator := remoteAuthenticator(server.URL)
	authenticator.JWKSRefreshInterval = time.Hour
	// Prime the cache with the old set, then authenticate with the new kid:
	// the first fetch misses, the forced refresh finds the rotated key.
	token := signFixtureJWT(t, newFixture, validJWTClaims())
	if _, err := authenticator.AuthenticateToken(token); err != nil {
		t.Fatalf("kid-miss refresh did not pick up the rotated key: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests=%d, want 2 (prime miss + forced refresh)", got)
	}
	// The refreshed set is cached: no further fetches for the same kid.
	if _, err := authenticator.AuthenticateToken(token); err != nil {
		t.Fatalf("refreshed key stopped working: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests=%d after reuse, want still 2", got)
	}
}

func jwkSetOf(t *testing.T, key jwk.Key) jwk.Set {
	t.Helper()
	set := jwk.NewSet()
	if err := set.AddKey(key); err != nil {
		t.Fatal(err)
	}
	return set
}
