package auth

import (
	"context"
	"errors"
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
	// defaultJWKSRefreshInterval is the default interval between JWKS refreshes.
	defaultJWKSRefreshInterval = 10 * time.Minute
	// jwksMaxStaleFactor bounds trust in a last-known-good JWKS set during
	// an IdP outage. With the default interval, this is a 30-minute grace
	// period; after it, refresh failure fails authentication closed.
	jwksMaxStaleFactor = 3
	// negativeCacheCap bounds the per-URL negative cache so a distinct-kid
	// flood cannot grow memory without bound. Eviction is FIFO; an evicted
	// entry can cause at most one fetch and only after the forced-refresh
	// budget (forcedAt) has elapsed, so the fetch bound is preserved
	// (FR-1/FR-2).
	negativeCacheCap = 256
	// maxNegatedKidBytes caps the size of a kid retained in the negative
	// cache. Real JWKS kids are short; a flood of tokens with oversized kids
	// is rejected through the forced-refresh budget path with identical
	// fetch counts, but is never retained (bounded memory).
	maxNegatedKidBytes = 64
	// minJWTSecretBytes is the minimum local HS256 HMAC key size (256-bit,
	// matching the HS256 key size). Length is measured in bytes: Go string
	// length is byte length, which is the brute-force domain for an HMAC key.
	// The gate is deliberately length-only (no entropy scoring): "a >=32-byte
	// secret passes" is deterministic and testable, which an entropy
	// heuristic would not be. This is intentionally stronger than the
	// service-secret policy (resolveSecrets), which fail-fasts on
	// empty/default/shared but has no numeric minimum: a short HS256 secret
	// is an offline-brute-forceable trust boundary, so a numeric minimum is
	// warranted here.
	minJWTSecretBytes = 32
)

// errKeyNotFound is the fail-closed rejection for a kid absent from a JWKS
// set. Its message is intentionally identical to the ambiguous-kid error
// text so every unknown-kid rejection is observably the same as today
// ("an unknown kid is rejected exactly as today"); the error VALUE differs
// only so the kid-miss gate can tell a genuine miss from a duplicate-kid
// set (which must not consume the forced-refresh budget).
var errKeyNotFound = errors.New("JWKS kid must identify exactly one key")

// fetchOutcome carries the result of one single-flight JWKS fetch. The
// leader writes set/err under the entry lock and then closes done; joiners
// read the fields after receiving from done (channel close provides
// happens-before), so no lock is ever held across the network fetch (FR-3).
type fetchOutcome struct {
	done chan struct{}
	set  jwk.Set
	err  error
}

// jwksCacheEntry holds one parsed JWKS set per JWKS URL. The entry lock
// guards every field below but is never held across the network fetch:
// fresh-set reads take a brief read lock (FR-3a), all fetches funnel
// through one single-flight outcome (at most one fetch in flight per URL,
// FR-3b), and joiners wait bounded by their own context (FR-3c). The
// negative cache records kids known to be absent from the last fetched set
// so repeat kid-misses are rejected with no network fetch (FR-1), and
// forcedAt rate-limits kid-miss forced refreshes to one per interval per
// URL (FR-2).
type jwksCacheEntry struct {
	mu        sync.RWMutex
	set       jwk.Set
	fetchedAt time.Time
	ttl       time.Duration
	// negCache maps a kid known to be absent from the last fetched set to
	// the expiry of that knowledge (insertion time + ttl). Consulted only
	// after a uniqueKey miss, so a kid that a later refresh brings into the
	// set is never falsely rejected (rotation path).
	negCache map[string]time.Time
	negOrder []string // FIFO insertion order for negativeCacheCap eviction
	negCap   int
	// forcedAt is when the last kid-miss forced refresh was permitted. A
	// new forced refresh is permitted only after the refresh interval has
	// elapsed (FR-2). Independent of fetchedAt: a successful forced refresh
	// re-stamps fetchedAt without reopening the forced-refresh budget.
	forcedAt time.Time
	// lastFailureAt/lastFailureErr suppress repeated completed refresh
	// failures for one interval. This cooldown limits outage amplification;
	// it does not change fetchedAt or the stale-grace calculation.
	lastFailureAt  time.Time
	lastFailureErr error
	inflight       *fetchOutcome // non-nil while a fetch is in flight (FR-3b)
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
	if hasSecret && a.AllowLocalHS256 && len(a.JWTSecret) < minJWTSecretBytes {
		return fmt.Errorf("local HS256 JWT secret must be at least %d bytes", minJWTSecretBytes)
	}
	if hasRemote {
		if err := validateJWKSURL(a.JWKSURL, a.AllowInsecureJWKSLoopback); err != nil {
			return err
		}
		return a.validateIssuerAudience(hasRemote)
	}
	if hasPublicKey {
		if _, err := a.localPublicKey(); err != nil {
			return err
		}
		return a.validateIssuerAudience(hasPublicKey)
	}
	if !hasSecret && !a.AllowDev {
		return fmt.Errorf("no JWT verification trust source is configured")
	}
	return nil
}

// validateIssuerAudience makes issuer and audience an explicit trust-source
// boundary for asymmetric JWT verification. Local HS256 is intentionally
// exempt because its explicit opt-in is itself the local trust boundary; the
// development-token-only profile is also exempt. AllowDev permits a local
// development process to start with an asymmetric source while it uses only
// dev tokens, but parseJWT still rejects ordinary JWTs until both pins are
// configured.
func (a Authenticator) requiresIssuerAudience() bool {
	return strings.TrimSpace(a.JWKSURL) != "" || strings.TrimSpace(a.JWTPublicKeyPEM) != ""
}

func (a Authenticator) validateIssuerAudience(asymmetric bool) error {
	if !asymmetric || a.AllowDev {
		return nil
	}
	if strings.TrimSpace(a.Issuer) == "" {
		return fmt.Errorf("asymmetric JWT trust requires a non-empty issuer")
	}
	if strings.TrimSpace(a.Audience) == "" {
		return fmt.Errorf("asymmetric JWT trust requires a non-empty audience")
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
	entry := a.cacheEntry()

	// FR-3a: read the current set without waiting on any in-flight fetch.
	entry.mu.RLock()
	set := entry.set
	fresh := set != nil && time.Since(entry.fetchedAt) < entry.ttl
	entry.mu.RUnlock()

	key, err := uniqueKey(set, header.keyID)
	if err == nil && fresh {
		// Fast path: the kid is present in a fresh set; the token is served
		// without touching the network and without contending with any
		// in-flight fetch.
		if err := validateRemoteKey(key, header.algorithm.String()); err != nil {
			return nil, err
		}
		return key, nil
	}
	if errors.Is(err, errKeyNotFound) {
		// Genuine miss: prime an empty cache first (this preserves the
		// initial-fetch + forced-refresh sequence the pinned rotation test
		// relies on), then run the FR-1/FR-2 gate.
		if set == nil {
			set, err = a.cachedJWKS(ctx, false)
			if err != nil {
				return nil, err
			}
			key, err = uniqueKey(set, header.keyID)
		}
		if errors.Is(err, errKeyNotFound) {
			key, err = a.refreshForMissingKid(ctx, header.keyID)
			if err != nil {
				return nil, err
			}
		}
	}
	if err != nil {
		// Ambiguous kid (present more than once) or any other lookup
		// failure: reject without a fetch, without consuming the
		// forced-refresh budget, and without recording the kid as missing.
		return nil, err
	}
	// The kid is present in the current set: run the normal TTL freshness
	// refresh when due so rotations are adopted and stale-on-outage is
	// preserved. A refreshed set that dropped the kid falls through to the
	// gated miss path.
	refreshed, refreshErr := a.cachedJWKS(ctx, false)
	if refreshErr != nil {
		return nil, refreshErr
	}
	key, err = uniqueKey(refreshed, header.keyID)
	if errors.Is(err, errKeyNotFound) {
		key, err = a.refreshForMissingKid(ctx, header.keyID)
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	if err := validateRemoteKey(key, header.algorithm.String()); err != nil {
		return nil, err
	}
	return key, nil
}

// cachedJWKS returns the parsed JWKS set for a.JWKSURL, fetching it at most
// once per refresh interval. force bypasses the freshness check (kid-miss
// refresh). Last-known-good keys may be trusted for at most three times the
// effective refresh interval while the IdP is unreachable. With the default
// 10-minute interval, the maximum stale grace period is 30 minutes. After
// that period, refresh failure causes authentication to fail closed. A set
// that never fetched also keeps the fail-closed behavior. All fetches —
// initial, TTL refresh, and forced — funnel through one single-flight outcome
// per URL, so at most one fetch is in flight at a time (FR-3b); a fresh-set
// read never waits on an in-flight fetch (FR-3a); joiners wait bounded by
// their own context (FR-3c).
func (a Authenticator) cachedJWKS(ctx context.Context, force bool) (jwk.Set, error) {
	entry := a.cacheEntry()
	entry.mu.RLock()
	if !entry.lastFailureAt.IsZero() && time.Since(entry.lastFailureAt) < entry.ttl {
		set := entry.set
		err := entry.lastFailureErr
		withinGrace := set != nil && staleAgeWithinGrace(entry.fetchedAt, entry.ttl, time.Now())
		entry.mu.RUnlock()
		if withinGrace {
			return set, nil
		}
		return nil, err
	}
	if !force && entry.set != nil && time.Since(entry.fetchedAt) < entry.ttl {
		set := entry.set
		entry.mu.RUnlock()
		return set, nil
	}
	entry.mu.RUnlock()
	outcome, leader := entry.beginFetch()
	if leader {
		// The fetch runs inside an inner closure so finishFetch (deferred)
		// completes before the outcome is read below: a deferred call at
		// this level would run only after the return expression had already
		// captured the still-empty outcome fields.
		var (
			set jwk.Set
			err error
		)
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					// A panic inside the fetch must not wedge the single-flight
					// slot or clobber a last-known-good set: record the outcome
					// as failed, then re-panic to the caller.
					entry.finishFetch(outcome, nil, fmt.Errorf("fetch JWKS: panic"))
					// A panic is not a completed upstream failure; do not let the
					// outage cooldown suppress the recovery fetch.
					entry.clearFetchFailure()
					panic(recovered)
				}
				entry.finishFetch(outcome, set, err)
			}()
			set, err = a.fetchJWKS(ctx)
		}()
	} else {
		select {
		case <-outcome.done:
		case <-ctx.Done():
			// FR-3c: a joiner's wait is bounded by its own context. The
			// caller's deadline has expired by definition here, and the
			// subsequent signature verification is itself ctx-bounded, so
			// serving the stale set could not succeed anyway; return the
			// caller's error honestly.
			return nil, ctx.Err()
		}
	}
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	if outcome.err != nil && entry.set != nil && staleAgeWithinGrace(entry.fetchedAt, entry.ttl, time.Now()) {
		return entry.set, nil // bounded stale-on-outage fallback
	}
	return outcome.set, outcome.err
}

// cacheEntry returns the per-URL cache entry, creating it (with an empty
// negative cache) on first use.
func (a Authenticator) cacheEntry() *jwksCacheEntry {
	entryValue, _ := jwksCache.LoadOrStore(a.JWKSURL, &jwksCacheEntry{
		ttl: a.jwksRefreshInterval(), negCache: map[string]time.Time{}, negCap: negativeCacheCap,
	})
	return entryValue.(*jwksCacheEntry)
}

// beginFetch claims the single-flight slot for one fetch, or joins the
// in-flight one. It holds the lock only for pointer bookkeeping (never
// I/O) and cannot be starved by the read-heavy fresh-set fast path (Go's
// RWMutex is writer-preferring).
func (e *jwksCacheEntry) beginFetch() (*fetchOutcome, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.inflight != nil {
		return e.inflight, false
	}
	outcome := &fetchOutcome{done: make(chan struct{})}
	e.inflight = outcome
	return outcome, true
}

// finishFetch publishes one fetch result and releases the single-flight
// slot. The set is stored and fetchedAt re-stamped only on success; forcedAt
// is deliberately untouched so a successful forced refresh does not reopen
// the forced-refresh budget (FR-2).
func (e *jwksCacheEntry) finishFetch(outcome *fetchOutcome, set jwk.Set, err error) {
	e.mu.Lock()
	outcome.set = set
	outcome.err = err
	if err == nil {
		e.set = set
		e.fetchedAt = time.Now()
		e.lastFailureAt = time.Time{}
		e.lastFailureErr = nil
	} else {
		e.lastFailureAt = time.Now()
		e.lastFailureErr = err
	}
	e.inflight = nil
	close(outcome.done)
	e.mu.Unlock()
}

// clearFetchFailure removes the cooldown after a local fetch panic. A
// panic is rethrown to the caller, and the next request must be able to
// attempt recovery immediately.
func (e *jwksCacheEntry) clearFetchFailure() {
	e.mu.Lock()
	e.lastFailureAt = time.Time{}
	e.lastFailureErr = nil
	e.mu.Unlock()
}

// refreshForMissingKid is the gated kid-miss path (FR-1, FR-2):
//   - kid recorded as known-missing -> reject with no fetch;
//   - budget exhausted -> reject and record the kid as known-missing
//     (fail-closed), unless a concurrent forced refresh has just landed the
//     kid in the current set, in which case the key is served and never
//     negated (rotation safety);
//   - budget available -> consume it, run one forced refresh (single-flight),
//     re-look-up; a still-missing kid is recorded as known-missing; a kid the
//     refreshed set contains is returned and never negatively cached.
func (a Authenticator) refreshForMissingKid(ctx context.Context, kid string) (jwk.Key, error) {
	entry := a.cacheEntry()
	now := time.Now()
	entry.mu.Lock()
	if entry.negatedLocked(kid, now) {
		entry.mu.Unlock()
		return nil, errKeyNotFound
	}
	if !entry.forcedBudgetLocked(now) {
		// Budget exhausted: before recording the kid as missing, re-check
		// the current set — a concurrent forced refresh may have just
		// landed this kid. A kid present in the set is never negated.
		if key, matches := entry.lookupLocked(kid); matches > 0 {
			entry.mu.Unlock()
			if matches == 1 {
				return key, nil
			}
			return nil, errKeyNotFound // ambiguous kid: no fetch, no negation
		}
		entry.negateLocked(kid, now)
		entry.mu.Unlock()
		return nil, errKeyNotFound
	}
	entry.forcedAt = now // the window is not reset by any later miss (FR-2)
	entry.mu.Unlock()
	set, err := a.cachedJWKS(ctx, true) // may join an in-flight fetch; wait bounded by ctx
	if err != nil {
		return nil, err
	}
	key, err := uniqueKey(set, kid)
	if err != nil {
		if errors.Is(err, errKeyNotFound) {
			entry.mu.Lock()
			entry.negateLocked(kid, time.Now())
			entry.mu.Unlock()
		}
		return nil, err
	}
	return key, nil
}

// negatedLocked reports whether kid is recorded as known-missing at now.
func (e *jwksCacheEntry) negatedLocked(kid string, now time.Time) bool {
	expiry, ok := e.negCache[kid]
	return ok && now.Before(expiry)
}

// forcedBudgetLocked reports whether a kid-miss forced refresh is permitted
// at now (at most one per refresh interval per URL, FR-2).
func (e *jwksCacheEntry) forcedBudgetLocked(now time.Time) bool {
	return e.forcedAt.IsZero() || now.Sub(e.forcedAt) >= e.ttl
}

// negateLocked records kid as known-missing for one refresh interval. The
// original expiry is kept on re-presentation (FR-1 records for one interval;
// the rejection still holds through the forced-refresh budget). Oversized
// kids are never retained (bounded memory).
func (e *jwksCacheEntry) negateLocked(kid string, now time.Time) {
	if len(kid) > maxNegatedKidBytes {
		return
	}
	if _, ok := e.negCache[kid]; ok {
		return
	}
	if len(e.negCache) >= e.negCap {
		oldest := e.negOrder[0]
		e.negOrder = e.negOrder[1:]
		delete(e.negCache, oldest)
	}
	e.negCache[kid] = now.Add(e.ttl)
	e.negOrder = append(e.negOrder, kid)
}

// lookupLocked finds kid in the current set, reporting how many keys match.
func (e *jwksCacheEntry) lookupLocked(kid string) (jwk.Key, int) {
	if e.set == nil {
		return nil, 0
	}
	var found jwk.Key
	matches := 0
	for index := 0; index < e.set.Len(); index++ {
		key, ok := e.set.Key(index)
		if !ok {
			continue
		}
		keyID, ok := key.KeyID()
		if ok && keyID == kid {
			found = key
			matches++
		}
	}
	if matches == 1 {
		return found, 1
	}
	return nil, matches
}

func (a Authenticator) jwksRefreshInterval() time.Duration {
	if a.JWKSRefreshInterval > 0 {
		return a.JWKSRefreshInterval
	}
	return defaultJWKSRefreshInterval
}

// staleAgeWithinGrace reports whether a last-known-good JWKS set may still be
// used after a failed refresh. Saturating the multiplication prevents an
// unusually large configured interval from wrapping the stale-age limit.
func staleAgeWithinGrace(fetchedAt time.Time, ttl time.Duration, now time.Time) bool {
	if fetchedAt.IsZero() || ttl <= 0 {
		return false
	}
	age := now.Sub(fetchedAt)
	if age < 0 {
		return true
	}
	const maxDuration = time.Duration(1<<63 - 1)
	maxAge := maxDuration
	if ttl <= maxDuration/jwksMaxStaleFactor {
		maxAge = ttl * jwksMaxStaleFactor
	}
	return age <= maxAge
}

func uniqueKey(set jwk.Set, wantedID string) (jwk.Key, error) {
	if set == nil {
		return nil, errKeyNotFound
	}
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
	if matches == 0 {
		return nil, errKeyNotFound
	}
	if matches > 1 {
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
	// host.docker.internal is the canonical container-side alias for the
	// host's loopback (extra_hosts host-gateway). Accepting it here only
	// matters under the explicit AUDIT_ALLOW_INSECURE_JWKS_LOOPBACK flag:
	// local verification stacks (docker compose) reach a host-run IdP
	// through this name; production stays HTTPS-only because the flag is
	// off by default and rejected by -check-config parity.
	if strings.EqualFold(host, "host.docker.internal") || strings.EqualFold(host, "gateway.docker.internal") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}
