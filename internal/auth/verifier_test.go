package auth

import (
	"context"
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
	"fmt"
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
	token := signHMAC(t, validJWTClaims(), []byte(testHMACSecret))
	withoutOptIn := Authenticator{JWTSecret: testHMACSecret}
	if _, err := withoutOptIn.AuthenticateToken(token); err == nil {
		t.Fatal("HS256 worked without explicit local opt-in")
	}
	localOnly := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}
	if _, err := localOnly.AuthenticateToken(token); err != nil {
		t.Fatalf("explicit local HS256 failed: %v", err)
	}
	mixed := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true, JWKSURL: "https://issuer.example/jwks"}
	if _, err := mixed.AuthenticateToken(token); err == nil {
		t.Fatal("mixed symmetric and remote trust configuration was accepted")
	}
}

// TestValidateConfigurationRejectsShortHS256Secret is AC-1: a local HS256
// trust source whose JWTSecret is shorter than minJWTSecretBytes (32 bytes)
// is rejected with the exact minimum-length error, at the exact boundary
// (31 bytes fails; 32 and 33 pass), and without disturbing the existing
// error precedence (no opt-in, and mutual exclusivity with an asymmetric
// trust source, both keep their original errors). The message is formatted
// from the constant, so the "at least 32 bytes" substring cannot drift.
func TestValidateConfigurationRejectsShortHS256Secret(t *testing.T) {
	if err := (Authenticator{JWTSecret: "short", AllowLocalHS256: true}).ValidateConfiguration(); err == nil || !strings.Contains(err.Error(), "at least 32 bytes") {
		t.Fatalf("short secret must be rejected with the minimum-length error, got: %v", err)
	}
	// Boundary: 31 bytes fails, 32 and 33 pass.
	if err := (Authenticator{JWTSecret: strings.Repeat("s", 31), AllowLocalHS256: true}).ValidateConfiguration(); err == nil {
		t.Fatal("31-byte secret must be rejected")
	}
	if err := (Authenticator{JWTSecret: strings.Repeat("s", 32), AllowLocalHS256: true}).ValidateConfiguration(); err != nil {
		t.Fatalf("32-byte secret must pass: %v", err)
	}
	if err := (Authenticator{JWTSecret: strings.Repeat("s", 33), AllowLocalHS256: true}).ValidateConfiguration(); err != nil {
		t.Fatalf("33-byte secret must pass: %v", err)
	}
	// Byte-vs-rune boundary (F-1/SEC-6): the gate measures bytes, not runes.
	// 16 multibyte runes are 32 bytes and must pass; 15 runes (30 bytes)
	// must fail. A rune-counting implementation would invert both cells.
	if err := (Authenticator{JWTSecret: strings.Repeat("é", 16), AllowLocalHS256: true}).ValidateConfiguration(); err != nil {
		t.Fatalf("16-rune (32-byte) multibyte secret must pass the byte gate: %v", err)
	}
	if err := (Authenticator{JWTSecret: strings.Repeat("é", 15), AllowLocalHS256: true}).ValidateConfiguration(); err == nil || !strings.Contains(err.Error(), "at least 32 bytes") {
		t.Fatalf("15-rune (30-byte) multibyte secret must be rejected as too short, got: %v", err)
	}
	// Precedence: no opt-in keeps the original error; a shared asymmetric
	// trust source keeps the mutual-exclusivity error (gate placed after
	// check 2 and the exclusivity check).
	if err := (Authenticator{JWTSecret: "short"}).ValidateConfiguration(); err == nil || !strings.Contains(err.Error(), "explicit local-only opt-in") {
		t.Fatalf("no-opt-in must keep the original error, got: %v", err)
	}
	if err := (Authenticator{JWTSecret: "short", AllowLocalHS256: true, JWKSURL: "https://issuer.example/jwks"}).ValidateConfiguration(); err == nil || !strings.Contains(err.Error(), "cannot share an asymmetric trust configuration") {
		t.Fatalf("mutual exclusivity must keep the original error, got: %v", err)
	}
	// Local-PEM variant of the same precedence branch (F-9): a short secret
	// shared with a local public key hits the same check-4 branch as the
	// JWKS variant, before the length gate.
	if err := (Authenticator{JWTSecret: "short", AllowLocalHS256: true, JWTPublicKeyPEM: "-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----"}).ValidateConfiguration(); err == nil || !strings.Contains(err.Error(), "cannot share an asymmetric trust configuration") {
		t.Fatalf("mutual exclusivity with a local public key must keep the original error, got: %v", err)
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
		{"host-gateway HTTP explicit", "http://host.docker.internal:8080/jwks", true, false},
		{"host-gateway HTTP default", "http://host.docker.internal:8080/jwks", false, true},
		{"gateway.docker.internal explicit", "http://gateway.docker.internal:8080/jwks", true, false},
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

// compactAttackToken builds a compact JWS with the given header and payload
// and a non-empty, invalid signature segment. parseVerifiedHeader only
// requires the compact shape, an allowed algorithm, and a signature that
// decodes to at least one byte, so the token reaches key lookup (and the
// kid-miss path) without any valid signature. The empty-signature shape of
// compactUnsignedToken is rejected at the header gate and would make every
// kid-miss test vacuous (zero fetches).
func compactAttackToken(t *testing.T, header map[string]any, payload []byte) string {
	t.Helper()
	encodedHeader, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(encodedHeader) + "." +
		base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString([]byte("AA"))
}

// slowJWKSServer serves the first request immediately and blocks every
// subsequent request on its release channel, counting both total requests
// and blocked (slow) requests. t.Cleanup releases and closes the server so
// a failing test cannot deadlock on a blocked handler.
type slowJWKSServer struct {
	server       *httptest.Server
	release      chan struct{}
	requests     atomic.Int32
	slowRequests atomic.Int32
	setBody      []byte
}

func newSlowJWKSServer(t *testing.T, key jwk.Key) *slowJWKSServer {
	t.Helper()
	body, err := json.Marshal(jwkSetOf(t, key))
	if err != nil {
		t.Fatal(err)
	}
	slow := &slowJWKSServer{release: make(chan struct{}), setBody: body}
	slow.server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		if slow.requests.Add(1) == 1 {
			slow.respond(response)
			return
		}
		slow.slowRequests.Add(1)
		<-slow.release
		slow.respond(response)
	}))
	t.Cleanup(func() {
		select {
		case <-slow.release:
		default:
			close(slow.release)
		}
		slow.server.Close()
	})
	return slow
}

func (s *slowJWKSServer) respond(response http.ResponseWriter) {
	response.Header().Set("Content-Type", "application/json")
	_, _ = response.Write(s.setBody)
}

// waitFor polls until the predicate holds or the deadline expires, for the
// blocked-server tests that need a deterministic "the fetch is in flight"
// sync point.
func waitFor(t *testing.T, what string, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !predicate() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !predicate() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// roundTripFunc adapts a function to http.RoundTripper for tests that need
// to inject failure or panic behavior into the JWKS fetch.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

// jwksBodyWithKeys concatenates the key arrays of the given JWKS bodies into
// one JWKS body (used to build sets with duplicate kids).
func jwksBodyWithKeys(t *testing.T, bodies ...[]byte) []byte {
	t.Helper()
	var combined struct {
		Keys []json.RawMessage `json:"keys"`
	}
	for _, body := range bodies {
		var parsed struct {
			Keys []json.RawMessage `json:"keys"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			t.Fatal(err)
		}
		combined.Keys = append(combined.Keys, parsed.Keys...)
	}
	encoded, err := json.Marshal(combined)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
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

// TestCompactAttackTokenPassesHeaderGate pins the header-gate contract the
// kid-miss tests rely on: an unsigned compact token with an empty signature
// segment is rejected at parse time (zero fetches), while the attack-token
// shape with a non-empty signature segment reaches key lookup.
func TestCompactAttackTokenPassesHeaderGate(t *testing.T) {
	payload, err := json.Marshal(validJWTClaims())
	if err != nil {
		t.Fatal(err)
	}
	attack := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": "attacker-0"}, payload)
	if _, err := parseVerifiedHeader(attack); err != nil {
		t.Fatalf("attack token must pass the header gate to exercise key lookup: %v", err)
	}
	emptySig := compactUnsignedToken(t, map[string]any{"alg": "RS256", "kid": "attacker-0"}, payload)
	if _, err := parseVerifiedHeader(emptySig); err == nil {
		t.Fatal("empty-signature token must be rejected at the header gate")
	}
}

// TestJWKSUnknownKidMissesBoundedWithinInterval (A1a) pins FR-1/FR-2: N
// distinct unknown kids within one refresh interval cost exactly the initial
// fetch plus one forced refresh (2 fetches total), independent of N; the
// remaining misses are rejected from the negative cache / exhausted budget
// with no network fetch. Replaying an already-negated kid stays fetch-free,
// and every gated rejection carries the same message as today's
// unknown-kid error.
func TestJWKSUnknownKidMissesBoundedWithinInterval(t *testing.T) {
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
	payload, err := json.Marshal(validJWTClaims())
	if err != nil {
		t.Fatal(err)
	}
	knownError := ""
	for i := 0; i < 100; i++ {
		token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": fmt.Sprintf("attacker-%d", i)}, payload)
		if _, err := authenticator.AuthenticateToken(token); err == nil {
			t.Fatalf("unknown kid %d was accepted", i)
		} else if i >= 1 {
			if knownError == "" {
				knownError = err.Error()
			} else if err.Error() != knownError {
				t.Fatalf("kid-miss rejection text differs: %q vs %q", knownError, err.Error())
			}
		}
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches=%d for 100 distinct unknown kids, want 2 (initial + one forced refresh)", got)
	}
	// Negative-cache hits: replaying a kid recorded as known-missing is
	// rejected with no additional fetch and the same message.
	for _, kid := range []string{"attacker-0", "attacker-50"} {
		token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": kid}, payload)
		if _, err := authenticator.AuthenticateToken(token); err == nil {
			t.Fatalf("negatively cached kid %s was accepted", kid)
		} else if err.Error() != knownError {
			t.Fatalf("negative-cache rejection text differs: %q", err.Error())
		}
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches=%d after replaying negated kids, want still 2", got)
	}
}

// TestJWKSUnknownKidMissesEvictNegativeCache (A1a eviction variant) pins the
// FIFO bound: a flood of distinct unknown kids cannot grow the per-URL
// negative cache past negativeCacheCap entries, and the fetch bound still
// holds.
func TestJWKSUnknownKidMissesEvictNegativeCache(t *testing.T) {
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
	payload, err := json.Marshal(validJWTClaims())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": fmt.Sprintf("attacker-%d", i)}, payload)
		if _, err := authenticator.AuthenticateToken(token); err == nil {
			t.Fatalf("unknown kid %d was accepted", i)
		}
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches=%d for 300 distinct unknown kids, want 2", got)
	}
	entryValue, ok := jwksCache.Load(server.URL)
	if !ok {
		t.Fatal("cache entry missing")
	}
	entry := entryValue.(*jwksCacheEntry)
	entry.mu.RLock()
	cached := len(entry.negCache)
	order := len(entry.negOrder)
	entry.mu.RUnlock()
	if cached != negativeCacheCap || order != negativeCacheCap {
		t.Fatalf("negative cache size=%d order=%d, want %d", cached, order, negativeCacheCap)
	}
}

// TestJWKSUnknownKidMissesBoundedPerInterval (A1b) pins the per-interval
// bound: at most one forced refresh per elapsed refresh interval, never
// proportional to the number of distinct kids (FR-2), and negative entries
// expire after the interval so a re-presented old kid triggers exactly one
// new fetch and is rejected again (FR-1 expiry).
func TestJWKSUnknownKidMissesBoundedPerInterval(t *testing.T) {
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
	authenticator.JWKSRefreshInterval = 50 * time.Millisecond
	payload, err := json.Marshal(validJWTClaims())
	if err != nil {
		t.Fatal(err)
	}
	sendBatch := func(prefix string, count int) {
		for i := 0; i < count; i++ {
			token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": fmt.Sprintf("%s-%d", prefix, i)}, payload)
			if _, err := authenticator.AuthenticateToken(token); err == nil {
				t.Fatalf("unknown kid %s-%d was accepted", prefix, i)
			}
		}
	}
	sendBatch("w1", 10)
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches=%d after the first window, want 2 (initial + forced)", got)
	}
	time.Sleep(150 * time.Millisecond) // cross the refresh interval
	sendBatch("w2", 10)
	if got := fetches.Load(); got != 3 {
		t.Fatalf("fetches=%d after the second window, want 3 (one forced refresh per interval)", got)
	}
	time.Sleep(150 * time.Millisecond)
	// The window-1 kid's negative entry has expired: re-presenting it
	// triggers exactly one new fetch and is rejected again.
	token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": "w1-0"}, payload)
	if _, err := authenticator.AuthenticateToken(token); err == nil {
		t.Fatal("re-presented unknown kid after expiry was accepted")
	}
	if got := fetches.Load(); got != 4 {
		t.Fatalf("fetches=%d after re-presenting an expired negative entry, want 4 (exactly one new fetch)", got)
	}
	// The window just elapsed: an immediate replay consumes no second fetch
	// within the window (budget path).
	if _, err := authenticator.AuthenticateToken(token); err == nil {
		t.Fatal("unknown kid accepted after re-negation")
	}
	if got := fetches.Load(); got != 4 {
		t.Fatalf("fetches=%d after immediate replay, want still 4", got)
	}
	// Aggregate bound: 1 initial + at most one forced refresh per elapsed
	// interval, independent of kid count.
	if got := fetches.Load(); got > 1+3 {
		t.Fatalf("fetches=%d exceeds the 1+W bound", got)
	}
}

// TestJWKSSlowFetchDoesNotStallValidAuth (A2) pins FR-3a: a valid token
// whose kid is in the fresh cached set completes without waiting on an
// in-flight (blocked) forced refresh caused by an attacker's kid-miss.
func TestJWKSSlowFetchDoesNotStallValidAuth(t *testing.T) {
	fixture := fixtureNamed(t, signingFixtures(t), "RS256")
	slow := newSlowJWKSServer(t, fixture.publicKey)
	authenticator := remoteAuthenticator(slow.server.URL)
	authenticator.JWKSRefreshInterval = time.Hour
	valid := signFixtureJWT(t, fixture, validJWTClaims())
	if _, err := authenticator.AuthenticateToken(valid); err != nil {
		t.Fatalf("prime cache: %v", err)
	}
	payload, err := json.Marshal(validJWTClaims())
	if err != nil {
		t.Fatal(err)
	}
	attackerDone := make(chan error, 1)
	go func() {
		token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": "attacker-x"}, payload)
		_, err := authenticator.AuthenticateToken(token)
		attackerDone <- err
	}()
	waitFor(t, "attacker fetch to block server-side", func() bool { return slow.slowRequests.Load() == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := authenticator.AuthenticateTokenContext(ctx, valid); err != nil {
		t.Fatalf("valid token must be served from the fresh cache while a fetch is blocked: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("valid auth took %v, want < 1s (must not wait on the in-flight fetch)", elapsed)
	}
	// Release the blocked fetch, then join the attacker goroutine.
	close(slow.release)
	if err := <-attackerDone; err == nil {
		t.Fatal("attacker token with unknown kid was accepted")
	}
	if got := slow.requests.Load(); got != 2 {
		t.Fatalf("requests=%d, want 2 (prime + one blocked forced refresh)", got)
	}
}

// TestJWKSForcedRefreshRateLimitedPerURL (A3) pins FR-2: attacker traffic
// cannot reset the forced-refresh window; at most one forced refresh per
// refresh interval, and the window is not reopened by a successful fetch.
func TestJWKSForcedRefreshRateLimitedPerURL(t *testing.T) {
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
	authenticator.JWKSRefreshInterval = 100 * time.Millisecond
	payload, err := json.Marshal(validJWTClaims())
	if err != nil {
		t.Fatal(err)
	}
	miss := func(kid string) error {
		token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": kid}, payload)
		_, err := authenticator.AuthenticateToken(token)
		return err
	}
	for i := 0; i < 50; i++ {
		if err := miss(fmt.Sprintf("w1-%d", i)); err == nil {
			t.Fatalf("unknown kid w1-%d accepted", i)
		}
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches=%d after 50 kids in the first window, want 2 (initial + one forced refresh)", got)
	}
	for i := 0; i < 50; i++ {
		if err := miss(fmt.Sprintf("w2-%d", i)); err == nil {
			t.Fatalf("unknown kid w2-%d accepted", i)
		}
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches=%d after 50 more kids in the same window, want still 2 (window not reset)", got)
	}
	time.Sleep(300 * time.Millisecond) // cross the refresh interval
	if err := miss("w3-0"); err == nil {
		t.Fatal("unknown kid w3-0 accepted")
	}
	if got := fetches.Load(); got != 3 {
		t.Fatalf("fetches=%d after a new window, want 3 (exactly one new forced refresh)", got)
	}
	for i := 1; i < 50; i++ {
		if err := miss(fmt.Sprintf("w3-%d", i)); err == nil {
			t.Fatalf("unknown kid w3-%d accepted", i)
		}
	}
	if got := fetches.Load(); got != 3 {
		t.Fatalf("fetches=%d after the rest of the window, want still 3", got)
	}
	if got := fetches.Load(); got > 1+2 {
		t.Fatalf("fetches=%d exceeds the 1+W bound", got)
	}
}

// TestJWKSConcurrentInitialFetchDedups (S1) pins FR-3b at the in-flight
// level: concurrent kid-miss fetches on an empty cache produce a single
// fetch (joiners wait on the outcome), and the subsequent forced refresh is
// also single-flight.
func TestJWKSConcurrentInitialFetchDedups(t *testing.T) {
	fixture := fixtureNamed(t, signingFixtures(t), "RS256")
	setBody, err := json.Marshal(jwkSetOf(t, fixture.publicKey))
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		<-release
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write(setBody)
	}))
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		server.Close()
	}()
	authenticator := remoteAuthenticator(server.URL)
	authenticator.JWKSRefreshInterval = time.Hour
	payload, err := json.Marshal(validJWTClaims())
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, kid := range []string{"attacker-a", "attacker-b"} {
		go func(kid string) {
			<-start
			token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": kid}, payload)
			_, err := authenticator.AuthenticateToken(token)
			results <- err
		}(kid)
	}
	close(start)
	// Both goroutines are in their initial fetch: at most one request may
	// have reached the server (single-flight dedup).
	time.Sleep(50 * time.Millisecond)
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests=%d while both goroutines prime, want 1 (single-flight)", got)
	}
	close(release) // let the prime fetch complete; the leader then forces a refresh
	for i := 0; i < 2; i++ {
		if err := <-results; err == nil {
			t.Fatalf("unknown kid accepted (result %d)", i)
		}
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests=%d after both goroutines return, want 2 (prime + one forced refresh)", got)
	}
}

// TestJWKSBudgetExhaustedServesLandedKid (S3 unit) pins the check-then-
// negate guard: when the forced-refresh budget is exhausted but a concurrent
// refresh has already landed the kid in the current set, the kid is served
// (never rejected, never negated).
func TestJWKSBudgetExhaustedServesLandedKid(t *testing.T) {
	fixture := fixtureNamed(t, signingFixtures(t), "RS256")
	server := serveJWK(t, fixture.publicKey)
	defer server.Close()
	authenticator := remoteAuthenticator(server.URL)
	authenticator.JWKSRefreshInterval = time.Hour
	valid := signFixtureJWT(t, fixture, validJWTClaims())
	if _, err := authenticator.AuthenticateToken(valid); err != nil {
		t.Fatalf("land the kid in the cache: %v", err)
	}
	entryValue, _ := jwksCache.Load(server.URL)
	entry := entryValue.(*jwksCacheEntry)
	entry.mu.Lock()
	entry.forcedAt = time.Now() // exhaust the forced-refresh budget
	entry.mu.Unlock()
	key, err := authenticator.refreshForMissingKid(context.Background(), fixture.keyID)
	if err != nil {
		t.Fatalf("kid present in the current set must be served despite the exhausted budget: %v", err)
	}
	if kid, _ := key.KeyID(); kid != fixture.keyID {
		t.Fatalf("served key kid=%q, want %q", kid, fixture.keyID)
	}
	entry.mu.RLock()
	_, negated := entry.negCache[fixture.keyID]
	entry.mu.RUnlock()
	if negated {
		t.Fatal("a kid present in the set must never be negatively cached")
	}
	// A genuinely missing kid is still rejected and negated.
	if _, err := authenticator.refreshForMissingKid(context.Background(), "missing-kid"); err == nil {
		t.Fatal("missing kid accepted")
	}
	entry.mu.RLock()
	_, negated = entry.negCache["missing-kid"]
	entry.mu.RUnlock()
	if !negated {
		t.Fatal("missing kid was not negatively cached")
	}
}

// TestJWKSConcurrentRotationBoundedRejection (S3 integration) pins the
// concurrent rotation behavior: while one goroutine's forced refresh is in
// flight, a second goroutine presenting a rotated-in kid is rejected at most
// once (budget exhausted, kid absent from the pre-rotation set), and once
// the refresh lands the kid is served with no further fetches and no
// negative-cache interference.
func TestJWKSConcurrentRotationBoundedRejection(t *testing.T) {
	fixtures := signingFixtures(t)
	oldFixture := fixtureNamed(t, fixtures, "ES256")
	newFixture := fixtureNamed(t, fixtures, "ES384")
	release := make(chan struct{})
	var requests atomic.Int32
	var blocked atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		key := oldFixture.publicKey
		if requests.Add(1) > 1 {
			key = newFixture.publicKey
			blocked.Add(1)
			<-release
		}
		body, err := json.Marshal(jwkSetOf(t, key))
		if err != nil {
			t.Error(err)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write(body)
	}))
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		server.Close()
	}()
	authenticator := remoteAuthenticator(server.URL)
	authenticator.JWKSRefreshInterval = time.Hour
	oldToken := signFixtureJWT(t, oldFixture, validJWTClaims())
	if _, err := authenticator.AuthenticateToken(oldToken); err != nil {
		t.Fatalf("prime cache: %v", err)
	}
	newToken := signFixtureJWT(t, newFixture, validJWTClaims())
	payload, err := json.Marshal(validJWTClaims())
	if err != nil {
		t.Fatal(err)
	}
	leaderDone := make(chan error, 1)
	go func() {
		token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": "attacker-x"}, payload)
		_, err := authenticator.AuthenticateToken(token)
		leaderDone <- err
	}()
	waitFor(t, "forced refresh to block server-side", func() bool { return blocked.Load() == 1 })
	// The budget is exhausted (the leader consumed it at gate time) and the
	// pre-rotation set is still current: the rotated-in kid is rejected
	// once, transiently.
	if _, err := authenticator.AuthenticateToken(newToken); err == nil {
		t.Fatal("rotated-in kid accepted while the forced refresh was blocked")
	}
	close(release)
	if err := <-leaderDone; err == nil {
		t.Fatal("attacker token with unknown kid was accepted")
	}
	// The refresh landed the rotated set: the kid is now served with zero
	// further fetches (the negative cache is consulted only on a miss).
	if _, err := authenticator.AuthenticateToken(newToken); err != nil {
		t.Fatalf("rotated-in kid must authenticate after the refresh landed: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests=%d, want 2 (prime + one forced refresh)", got)
	}
}

// TestJWKSJoinerWaitBoundedByCtx (S2) pins FR-3c: a request that joins an
// in-flight fetch waits at most as long as its own context allows, returns
// promptly, and never triggers a second fetch. (The wait is bounded by the
// caller's context; the signature verification is itself ctx-bounded, so a
// caller whose deadline elapsed cannot authenticate on this path.)
func TestJWKSJoinerWaitBoundedByCtx(t *testing.T) {
	fixture := fixtureNamed(t, signingFixtures(t), "RS256")
	slow := newSlowJWKSServer(t, fixture.publicKey)
	authenticator := remoteAuthenticator(slow.server.URL)
	authenticator.JWKSRefreshInterval = 50 * time.Millisecond
	valid := signFixtureJWT(t, fixture, validJWTClaims())
	if _, err := authenticator.AuthenticateToken(valid); err != nil {
		t.Fatalf("prime cache: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // let the TTL expire
	payload, err := json.Marshal(validJWTClaims())
	if err != nil {
		t.Fatal(err)
	}
	attackerDone := make(chan error, 1)
	go func() {
		token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": "attacker-x"}, payload)
		_, err := authenticator.AuthenticateToken(token)
		attackerDone <- err
	}()
	waitFor(t, "forced refresh to block server-side", func() bool { return slow.slowRequests.Load() == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = authenticator.AuthenticateTokenContext(ctx, valid)
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("joiner took %v, want < 1s (ctx-bounded wait)", elapsed)
	}
	if err == nil {
		t.Fatal("joiner with an expired context must not succeed")
	}
	close(slow.release)
	if err := <-attackerDone; err == nil {
		t.Fatal("attacker token with unknown kid was accepted")
	}
	if got := slow.requests.Load(); got != 2 {
		t.Fatalf("requests=%d, want 2 (prime + one blocked forced refresh)", got)
	}
}

// TestJWKSLeaderCancelConsumesBudget (F5) pins failure mode 4.6: a kid-miss
// forced refresh aborted by the leader's context still consumes the
// forced-refresh budget (no additional fetch within the window), and the
// last-known-good set keeps serving valid tokens.
func TestJWKSLeaderCancelConsumesBudget(t *testing.T) {
	fixture := fixtureNamed(t, signingFixtures(t), "RS256")
	slow := newSlowJWKSServer(t, fixture.publicKey)
	authenticator := remoteAuthenticator(slow.server.URL)
	authenticator.JWKSRefreshInterval = time.Hour
	valid := signFixtureJWT(t, fixture, validJWTClaims())
	if _, err := authenticator.AuthenticateToken(valid); err != nil {
		t.Fatalf("prime cache: %v", err)
	}
	payload, err := json.Marshal(validJWTClaims())
	if err != nil {
		t.Fatal(err)
	}
	attackerDone := make(chan error, 1)
	go func() {
		token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": "attacker-x"}, payload)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_, err := authenticator.AuthenticateTokenContext(ctx, token)
		attackerDone <- err
	}()
	waitFor(t, "forced refresh to block server-side", func() bool { return slow.slowRequests.Load() == 1 })
	if err := <-attackerDone; err == nil {
		t.Fatal("attacker token with unknown kid was accepted")
	}
	// Valid tokens keep authenticating from the fresh cached set.
	if _, err := authenticator.AuthenticateToken(valid); err != nil {
		t.Fatalf("valid token after a cancelled leader fetch: %v", err)
	}
	// The budget was consumed at gate time despite the aborted fetch: a new
	// unknown kid within the same window is rejected with no fetch.
	token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": "attacker-y"}, payload)
	if _, err := authenticator.AuthenticateToken(token); err == nil {
		t.Fatal("unknown kid accepted after the budget was consumed by the aborted fetch")
	}
	if got := slow.requests.Load(); got != 2 {
		t.Fatalf("requests=%d, want 2 (prime + aborted forced refresh; no further fetches)", got)
	}
}

// TestJWKSAmbiguousKidDoesNotConsumeBudget (S5) pins the ambiguous-kid
// handling: a set with duplicate kids rejects the token without a fetch,
// without consuming the forced-refresh budget, and without negating the
// kid; a later genuine kid-miss still gets its budget slot.
func TestJWKSAmbiguousKidDoesNotConsumeBudget(t *testing.T) {
	fixtures := signingFixtures(t)
	rsaFixture := fixtureNamed(t, fixtures, "RS256")
	p256Fixture := fixtureNamed(t, fixtures, "ES256")
	first := mutateJWK(t, rsaFixture.publicKey, map[string]any{"kid": "duplicate-kid", "alg": "RS256", "use": "sig"})
	second := mutateJWK(t, p256Fixture.publicKey, map[string]any{"kid": "duplicate-kid", "alg": "ES256", "use": "sig"})
	body := jwksBodyWithKeys(t, first, second)
	var fetches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write(body)
	}))
	defer server.Close()
	authenticator := remoteAuthenticator(server.URL)
	authenticator.JWKSRefreshInterval = time.Hour
	payload, err := json.Marshal(validJWTClaims())
	if err != nil {
		t.Fatal(err)
	}
	token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": "duplicate-kid"}, payload)
	if _, err := authenticator.AuthenticateToken(token); err == nil {
		t.Fatal("ambiguous kid accepted")
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("fetches=%d for an ambiguous kid, want 1 (initial prime only; no forced refresh)", got)
	}
	// The genuine miss that follows still gets its budget slot.
	miss := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": "genuine-miss"}, payload)
	if _, err := authenticator.AuthenticateToken(miss); err == nil {
		t.Fatal("unknown kid accepted")
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches=%d after a genuine miss, want 2 (budget was not consumed by the ambiguous kid)", got)
	}
	// The ambiguous kid was not negatively cached.
	entryValue, ok := jwksCache.Load(server.URL)
	if !ok {
		t.Fatal("cache entry missing")
	}
	entry := entryValue.(*jwksCacheEntry)
	entry.mu.RLock()
	_, negated := entry.negCache["duplicate-kid"]
	entry.mu.RUnlock()
	if negated {
		t.Fatal("ambiguous kid must not be negatively cached")
	}
}

// TestJWKSOversizedKidNotRetained (security F2) pins the negative-cache
// byte bound: kids longer than maxNegatedKidBytes are rejected through the
// budget path with identical fetch counts but are never retained.
func TestJWKSOversizedKidNotRetained(t *testing.T) {
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
	payload, err := json.Marshal(validJWTClaims())
	if err != nil {
		t.Fatal(err)
	}
	bigKid := strings.Repeat("k", 10*1024)
	for i := 0; i < 10; i++ {
		token := compactAttackToken(t, map[string]any{"alg": "RS256", "kid": bigKid}, payload)
		if _, err := authenticator.AuthenticateToken(token); err == nil {
			t.Fatalf("oversized unknown kid accepted (round %d)", i)
		}
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches=%d for oversized-kid misses, want 2 (initial + one forced refresh)", got)
	}
	entryValue, ok := jwksCache.Load(server.URL)
	if !ok {
		t.Fatal("cache entry missing")
	}
	entry := entryValue.(*jwksCacheEntry)
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	for kid := range entry.negCache {
		if len(kid) > maxNegatedKidBytes {
			t.Fatalf("negative cache retained an oversized kid (%d bytes)", len(kid))
		}
	}
}

// TestJWKSCacheSurvivesFetchPanic (async F3 hardening) pins the
// single-flight cleanup: a panic inside fetchJWKS must not wedge the
// in-flight slot or clobber a last-known-good set; the next fetch leads a
// fresh outcome and succeeds.
func TestJWKSCacheSurvivesFetchPanic(t *testing.T) {
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
	serverTransport := server.Client().Transport
	var panicked atomic.Bool
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if !panicked.Swap(true) {
			panic("injected fetch panic")
		}
		return serverTransport.RoundTrip(request)
	})
	authenticator := remoteAuthenticator(server.URL)
	authenticator.JWKSRefreshInterval = time.Hour
	authenticator.JWKSHTTPClient = &http.Client{Transport: transport}
	valid := signFixtureJWT(t, fixture, validJWTClaims())
	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Error("expected the injected panic to propagate")
			}
		}()
		_, _ = authenticator.AuthenticateToken(valid)
	}()
	// The single-flight slot was cleared despite the panic: the next auth
	// leads a fresh fetch and succeeds.
	if _, err := authenticator.AuthenticateToken(valid); err != nil {
		t.Fatalf("auth after a panicked fetch must succeed: %v", err)
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("fetches=%d, want 1 (the panicked request never reached the server)", got)
	}
}
