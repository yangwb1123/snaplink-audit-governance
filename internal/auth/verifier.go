package auth

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
)

const (
	defaultPublicKeyAlgorithm = "RS256"
	maxJWTBytes               = 1 << 20
	maxJWKSBytes              = 2 << 20
	jwksFetchTimeout          = 5 * time.Second
	// defaultJWKSRefreshInterval bounds JWKS staleness between refetches.
	defaultJWKSRefreshInterval = 10 * time.Minute
)

// jwksCacheEntry holds one parsed JWKS set per JWKS URL. The mutex is held
// across the fetch, so concurrent authentications single-flight the request:
// an unauthenticated attacker flooding garbage tokens can never amplify
// fetches to the identity provider beyond one in flight per URL.
type jwksCacheEntry struct {
	mu        sync.Mutex
	set       jwk.Set
	fetchedAt time.Time
	ttl       time.Duration
}

// jwksCache maps JWKS URL to its cache entry. A process uses one trust
// source (or a small fixed set), so the map stays tiny; per-URL entries keep
// tests with ephemeral httptest servers isolated from each other.
var jwksCache sync.Map

type verifiedHeader struct {
	algorithm jwa.SignatureAlgorithm
	keyID     string
}

func (a Authenticator) ValidateConfiguration() error {
	hasRemote := strings.TrimSpace(a.JWKSURL) != ""
	hasPublicKey := strings.TrimSpace(a.JWTPublicKeyPEM) != ""
	hasSecret := a.JWTSecret != ""
	if hasRemote && hasPublicKey {
		return fmt.Errorf("JWT verifier must use either JWKS URL or local public key, not both")
	}
	if hasSecret && !a.AllowLocalHS256 {
		return fmt.Errorf("HS256 secret requires explicit local-only opt-in")
	}
	if a.AllowLocalHS256 && !hasSecret {
		return fmt.Errorf("local HS256 opt-in requires a secret")
	}
	if hasSecret && (hasRemote || hasPublicKey) {
		return fmt.Errorf("local HS256 cannot share an asymmetric trust configuration")
	}
	if hasRemote {
		return validateJWKSURL(a.JWKSURL, a.AllowInsecureJWKSLoopback)
	}
	if hasPublicKey {
		_, err := a.localPublicKey()
		return err
	}
	if !hasSecret && !a.AllowDev {
		return fmt.Errorf("no JWT verification trust source is configured")
	}
	return nil
}

func (a Authenticator) verifyJWT(ctx context.Context, token string) ([]byte, error) {
	if len(token) == 0 || len(token) > maxJWTBytes {
		return nil, fmt.Errorf("invalid bearer token size")
	}
	header, err := parseVerifiedHeader(token)
	if err != nil {
		return nil, err
	}
	key, err := a.verificationKey(ctx, header)
	if err != nil {
		return nil, err
	}
	payload, err := jws.Verify([]byte(token), jws.WithKey(header.algorithm, key), jws.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("invalid token signature")
	}
	return payload, nil
}

func parseVerifiedHeader(token string) (verifiedHeader, error) {
	message, err := jws.Parse([]byte(token), jws.WithCompact())
	if err != nil || len(message.Signatures()) != 1 {
		return verifiedHeader{}, fmt.Errorf("invalid bearer token")
	}
	headers := message.Signatures()[0].ProtectedHeaders()
	algorithm, ok := headers.Algorithm()
	if !ok || !allowedTokenAlgorithm(algorithm.String()) {
		return verifiedHeader{}, fmt.Errorf("unsupported token algorithm")
	}
	if _, ok := headers.JWK(); ok {
		return verifiedHeader{}, fmt.Errorf("embedded token key is forbidden")
	}
	if _, ok := headers.JWKSetURL(); ok {
		return verifiedHeader{}, fmt.Errorf("token-selected JWKS URL is forbidden")
	}
	if encoded, ok := headers.B64(); ok && !encoded {
		return verifiedHeader{}, fmt.Errorf("unencoded JWT payload is forbidden")
	}
	keyID, _ := headers.KeyID()
	return verifiedHeader{algorithm: algorithm, keyID: keyID}, nil
}

func allowedTokenAlgorithm(algorithm string) bool {
	switch algorithm {
	case "EdDSA", "ES256", "ES384", "ES512", "RS256", "PS256", "HS256":
		return true
	default:
		return false
	}
}

func (a Authenticator) verificationKey(ctx context.Context, header verifiedHeader) (any, error) {
	if header.algorithm.String() == "HS256" {
		if !a.AllowLocalHS256 || a.JWTSecret == "" || a.JWKSURL != "" || a.JWTPublicKeyPEM != "" {
			return nil, fmt.Errorf("HS256 is not enabled for this trust source")
		}
		return []byte(a.JWTSecret), nil
	}
	if a.JWKSURL != "" {
		return a.remoteVerificationKey(ctx, header)
	}
	key, err := a.localPublicKey()
	if err != nil {
		return nil, err
	}
	if header.algorithm.String() != a.localPublicAlgorithm() {
		return nil, fmt.Errorf("token algorithm does not match configured public key algorithm")
	}
	return key, nil
}

func (a Authenticator) localPublicKey() (jwk.Key, error) {
	algorithm := a.localPublicAlgorithm()
	if !allowedAsymmetricAlgorithm(algorithm) {
		return nil, fmt.Errorf("unsupported local public key algorithm %q", algorithm)
	}
	key, err := jwk.ParseKey([]byte(a.JWTPublicKeyPEM), jwk.WithPEM(true))
	if err != nil {
		return nil, fmt.Errorf("invalid local public key: %w", err)
	}
	if err := validateKeyMaterial(key, algorithm); err != nil {
		return nil, err
	}
	return key, nil
}

func (a Authenticator) localPublicAlgorithm() string {
	algorithm := strings.TrimSpace(a.JWTPublicKeyAlgorithm)
	if algorithm == "" {
		return defaultPublicKeyAlgorithm
	}
	return algorithm
}

func allowedAsymmetricAlgorithm(algorithm string) bool {
	switch algorithm {
	case "EdDSA", "ES256", "ES384", "ES512", "RS256", "PS256":
		return true
	default:
		return false
	}
}

func (a Authenticator) remoteVerificationKey(ctx context.Context, header verifiedHeader) (jwk.Key, error) {
	if !allowedAsymmetricAlgorithm(header.algorithm.String()) {
		return nil, fmt.Errorf("remote JWKS accepts asymmetric algorithms only")
	}
	if header.keyID == "" {
		return nil, fmt.Errorf("remote JWKS token requires kid")
	}
	set, err := a.cachedJWKS(ctx, false)
	if err != nil {
		return nil, err
	}
	key, err := uniqueKey(set, header.keyID)
	if err != nil {
		// The kid is missing from the cached set: a rotation may have
		// happened between refreshes. Force exactly one refresh; a still-
		// missing kid is rejected (fail-closed) instead of being served
		// with a stale key.
		set, err = a.cachedJWKS(ctx, true)
		if err != nil {
			return nil, err
		}
		key, err = uniqueKey(set, header.keyID)
		if err != nil {
			return nil, err
		}
	}
	if err := validateRemoteKey(key, header.algorithm.String()); err != nil {
		return nil, err
	}
	return key, nil
}

// cachedJWKS returns the parsed JWKS set for a.JWKSURL, fetching it at most
// once per refresh interval. force bypasses the freshness check (kid-miss
// refresh). On a fetch failure the last known-good set is served when one
// exists (stale-on-outage: an IdP outage is not a total auth outage, and
// staleness is bounded by the refresh interval); a set that never fetched
// keeps the fail-closed behavior.
func (a Authenticator) cachedJWKS(ctx context.Context, force bool) (jwk.Set, error) {
	entryValue, _ := jwksCache.LoadOrStore(a.JWKSURL, &jwksCacheEntry{ttl: a.jwksRefreshInterval()})
	entry := entryValue.(*jwksCacheEntry)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if !force && entry.set != nil && time.Since(entry.fetchedAt) < entry.ttl {
		return entry.set, nil
	}
	set, err := a.fetchJWKS(ctx)
	if err != nil {
		if entry.set != nil {
			return entry.set, nil
		}
		return nil, err
	}
	entry.set = set
	entry.fetchedAt = time.Now()
	return set, nil
}

func (a Authenticator) jwksRefreshInterval() time.Duration {
	if a.JWKSRefreshInterval > 0 {
		return a.JWKSRefreshInterval
	}
	return defaultJWKSRefreshInterval
}

func uniqueKey(set jwk.Set, wantedID string) (jwk.Key, error) {
	var found jwk.Key
	matches := 0
	for index := 0; index < set.Len(); index++ {
		key, ok := set.Key(index)
		if !ok {
			continue
		}
		keyID, ok := key.KeyID()
		if ok && keyID == wantedID {
			found = key
			matches++
		}
	}
	if matches != 1 {
		return nil, fmt.Errorf("JWKS kid must identify exactly one key")
	}
	return found, nil
}

func validateRemoteKey(key jwk.Key, algorithm string) error {
	keyAlgorithm, ok := key.Algorithm()
	if !ok || keyAlgorithm.String() != algorithm {
		return fmt.Errorf("JWK algorithm does not match token algorithm")
	}
	usage, ok := key.KeyUsage()
	if !ok || usage != "sig" {
		return fmt.Errorf("JWK use must be sig")
	}
	if operations, ok := key.KeyOps(); ok {
		if len(operations) != 1 || operations[0] != jwk.KeyOpVerify {
			return fmt.Errorf("JWK key_ops must contain only verify")
		}
	}
	return validateKeyMaterial(key, algorithm)
}

func validateKeyMaterial(key jwk.Key, algorithm string) error {
	if key.Has("d") {
		return fmt.Errorf("private JWK material is forbidden")
	}
	if err := key.Validate(); err != nil {
		return fmt.Errorf("invalid JWK material: %w", err)
	}
	switch algorithm {
	case "EdDSA":
		return validateOKPKey(key, "Ed25519")
	case "ES256":
		return validateECKey(key, "P-256")
	case "ES384":
		return validateECKey(key, "P-384")
	case "ES512":
		return validateECKey(key, "P-521")
	case "RS256", "PS256":
		if _, ok := key.(jwk.RSAPublicKey); !ok {
			return fmt.Errorf("JWK kty does not match RSA algorithm")
		}
		return nil
	default:
		return fmt.Errorf("unsupported asymmetric token algorithm")
	}
}

func validateOKPKey(key jwk.Key, expectedCurve string) error {
	publicKey, ok := key.(jwk.OKPPublicKey)
	if !ok {
		return fmt.Errorf("JWK kty does not match EdDSA")
	}
	curve, ok := publicKey.Crv()
	if !ok || curve.String() != expectedCurve {
		return fmt.Errorf("JWK curve does not match EdDSA")
	}
	return nil
}

func validateECKey(key jwk.Key, expectedCurve string) error {
	publicKey, ok := key.(jwk.ECDSAPublicKey)
	if !ok {
		return fmt.Errorf("JWK kty does not match ECDSA algorithm")
	}
	curve, ok := publicKey.Crv()
	if !ok || curve.String() != expectedCurve {
		return fmt.Errorf("JWK curve does not match ECDSA algorithm")
	}
	return nil
}

func (a Authenticator) fetchJWKS(ctx context.Context) (jwk.Set, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, a.JWKSURL, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch JWKS: invalid URL")
	}
	client := restrictedHTTPClient(a.JWKSHTTPClient)
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch JWKS: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch JWKS: HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxJWKSBytes+1))
	if err != nil {
		return nil, fmt.Errorf("fetch JWKS: %w", err)
	}
	if len(body) > maxJWKSBytes {
		return nil, fmt.Errorf("fetch JWKS: response exceeds size limit")
	}
	set, err := jwk.Parse(body)
	if err != nil {
		return nil, fmt.Errorf("parse JWKS: %w", err)
	}
	return set, nil
}

func restrictedHTTPClient(configured *http.Client) *http.Client {
	if configured == nil {
		configured = &http.Client{}
	}
	client := *configured
	if client.Timeout <= 0 {
		client.Timeout = jwksFetchTimeout
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &client
}

func validateJWKSURL(rawURL string, allowInsecureLoopback bool) error {
	trimmed := strings.TrimSpace(rawURL)
	if rawURL != trimmed {
		return fmt.Errorf("invalid JWKS URL")
	}
	endpoint, err := url.Parse(trimmed)
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.Fragment != "" {
		return fmt.Errorf("invalid JWKS URL")
	}
	if endpoint.Scheme == "https" {
		return nil
	}
	if allowInsecureLoopback && endpoint.Scheme == "http" && loopbackHost(endpoint.Hostname()) {
		return nil
	}
	return fmt.Errorf("JWKS URL must use HTTPS")
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}
