// Package runtimeconfig builds the shared signing/archive configuration for
// the audit-api and audit-governance-worker binaries so both processes use
// the same signature scheme and archive destination (a worker sealing
// segments with HMAC while the API signs with Vault would break the
// evidence chain).
package runtimeconfig

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/service"
)

// Environment variable names for the signing/encryption secrets and the
// external transport knobs, shared by both binaries. A single source for the
// names underpins the cross-process consistency guard: identical env plumbing
// means the API and worker resolve identical keys from the same deployment
// configuration.
const (
	EnvSigningSecret              = "AUDIT_SIGNING_SECRET"
	EnvEncryptionKey              = "AUDIT_ENCRYPTION_KEY"
	EnvDevSecrets                 = "AUDIT_ALLOW_DEV_SECRETS"
	EnvS3UseSSL                   = "AUDIT_S3_USE_SSL"
	EnvAllowInsecureVaultLoopback = "AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK"
	// EnvArchiveRetentionDays is the per-object COMPLIANCE retention duration
	// (in days) applied by every S3 archive Put. Mandatory for an S3 archive:
	// a zero value is a fail-closed configuration error, so production wiring
	// can never reach an S3 store that writes objects without explicit
	// retention (the ingest-path drift neutralizer, F1).
	EnvArchiveRetentionDays = "AUDIT_ARCHIVE_RETENTION_DAYS"
)

// maxArchiveRetentionDays caps the retention duration at 100 years so
// retainFor and RetainUntilDate stay sane and overflow-safe
// (time.Duration(days)*24*time.Hour is exact below the cap).
const maxArchiveRetentionDays = 36500

// SigningArchive carries the optional external-infrastructure settings.
type SigningArchive struct {
	ArchiveDir      string
	SigningSecret   string
	EncryptionKey   string
	VaultAddr       string
	VaultToken      string
	VaultTransitKey string
	S3Endpoint      string
	S3Bucket        string
	S3AccessKey     string
	S3SecretKey     string
	// S3UseSSL resolves into the minio Secure flag and the transport=tls
	// label (REQ-TLS-1). Plaintext is restricted to the established local
	// development hosts and the exact verify-stack endpoint minio:9000.
	S3UseSSL bool
	// ArchiveRetentionDays is the per-object COMPLIANCE retention duration in
	// days applied by every S3 archive Put (shared by both binaries via
	// EnvArchiveRetentionDays). Zero is rejected for an S3 archive by
	// Archive(): production wiring can never reach an S3 store that writes
	// without explicit retention.
	ArchiveRetentionDays uint
	// AllowInsecureVaultLoopback permits plaintext http Vault only on
	// loopback hosts, mirroring the JWKS allowlist (REQ-TLS-4). The opt-in
	// never widens past loopback.
	AllowInsecureVaultLoopback bool
}

// newS3Store is the constructor seam for Archive(). Production behavior is
// exactly archive.NewS3StoreRetention (per-object COMPLIANCE retention
// applied by every Put); tests override it to capture the resolved useSSL
// flag or to inject a scripted client via archive.NewS3StoreWithClient.
var newS3Store = func(endpoint, accessKey, secretKey, bucket string, useSSL bool, retainFor time.Duration) (*archive.S3Store, error) {
	return archive.NewS3StoreRetention(endpoint, accessKey, secretKey, bucket, useSSL, retainFor)
}

// Signer returns the configured checkpoint signer (Vault Transit when all
// three Vault settings are present, HMAC otherwise). A well-known default
// signing secret is rejected whenever any Vault setting is present, so a
// direct caller cannot bypass the service-layer guard (defense in depth;
// validation still completes before any Vault client is constructed).
func (s SigningArchive) Signer() (service.Signer, error) {
	configured := s.VaultAddr != "" || s.VaultToken != "" || s.VaultTransitKey != ""
	if configured && isKnownDefaultSecret(s.SigningSecret) {
		return nil, fmt.Errorf("%w: %s is a well-known default and cannot be combined with Vault Transit", service.ErrDefaultSecret, EnvSigningSecret)
	}
	if configured && (s.VaultAddr == "" || s.VaultToken == "" || s.VaultTransitKey == "") {
		return nil, fmt.Errorf("vault signing requires addr, token and transit key together")
	}
	if s.VaultAddr == "" {
		return nil, nil
	}
	if _, err := s.resolveVaultTransport(); err != nil {
		return nil, err
	}
	return security.NewVaultTransitSigner(s.VaultAddr, s.VaultToken, s.VaultTransitKey), nil
}

// validateS3Retention enforces the pure, client-free retention invariants
// shared by Archive() and ConsistencyKey(). The dedicated consistency-key
// path must fail closed on the same invalid per-object retention values that
// startup and -check-config reject, otherwise the parity checker can go false
// green even though any real S3 write would be non-compliant.
func (s SigningArchive) validateS3Retention() error {
	if s.S3Endpoint == "" {
		return nil
	}
	if s.ArchiveRetentionDays == 0 {
		return fmt.Errorf("s3 archive requires a positive retention duration: set %s (e.g. 365 for one year of COMPLIANCE retention on every archived object); without it every object would be deletable if the bucket default is absent or downgraded", EnvArchiveRetentionDays)
	}
	if s.ArchiveRetentionDays > maxArchiveRetentionDays {
		return fmt.Errorf("s3 archive retention duration %d days exceeds the maximum of %d: set %s to at most 100 years", s.ArchiveRetentionDays, maxArchiveRetentionDays, EnvArchiveRetentionDays)
	}
	return nil
}

// Archive returns the configured compliance archive (S3 Object Lock when
// all four S3 settings are present, local read-only directory otherwise).
// A well-known default encryption key is rejected whenever any S3 setting
// is present (defense in depth, before any S3 client is constructed).
func (s SigningArchive) Archive() (archive.Store, error) {
	configured := s.S3Endpoint != "" || s.S3Bucket != "" || s.S3AccessKey != "" || s.S3SecretKey != ""
	if configured && isKnownDefaultSecret(s.EncryptionKey) {
		return nil, fmt.Errorf("%w: %s is a well-known default and cannot be combined with an S3 archive", service.ErrDefaultSecret, EnvEncryptionKey)
	}
	if configured && (s.S3Endpoint == "" || s.S3Bucket == "" || s.S3AccessKey == "" || s.S3SecretKey == "") {
		return nil, fmt.Errorf("s3 archive requires endpoint, bucket, access key and secret key together")
	}
	if s.S3Endpoint == "" {
		return &archive.FileStore{Dir: s.ArchiveDir}, nil
	}
	endpoint, useSSL, _, err := s.resolveS3Transport()
	if err != nil {
		return nil, err
	}
	if err := s.validateS3Retention(); err != nil {
		return nil, err
	}
	retainFor := time.Duration(s.ArchiveRetentionDays) * 24 * time.Hour
	return newS3Store(endpoint, s.S3AccessKey, s.S3SecretKey, s.S3Bucket, useSSL, retainFor)
}

// ConsistencyKey returns the non-secret identity shared by the API and
// governance worker. It deliberately contains the resolved signing scheme and
// the selected archive destination, but never signing/encryption material,
// Vault tokens, or S3 credentials. The key is a name-level parity signal:
// Vault's algorithm includes the Transit key name and S3 identity includes the
// bucket name; endpoint hosts and retention parameters are validated by their
// respective transport/archive gates, not encoded here.
//
// This method performs no I/O and does not construct an archive client. Its S3
// validation mirrors Archive's pure configuration checks (selection plus the
// per-object retention invariant) so a direct caller cannot silently turn an
// invalid S3 configuration into a successful parity signal. Readiness/network
// checks remain the responsibility of the normal startup and -check-config
// paths.
func (s SigningArchive) ConsistencyKey() (string, error) {
	signer, err := s.Signer()
	if err != nil {
		return "", fmt.Errorf("consistency key: signer: %w", err)
	}
	algorithm := "HMAC-SHA256"
	if signer != nil {
		algorithm = signer.Algorithm()
	}

	configured := s.S3Endpoint != "" || s.S3Bucket != "" || s.S3AccessKey != "" || s.S3SecretKey != ""
	if configured && (s.S3Endpoint == "" || s.S3Bucket == "" || s.S3AccessKey == "" || s.S3SecretKey == "") {
		return "", fmt.Errorf("consistency key: archive: s3 archive requires endpoint, bucket, access key and secret key together")
	}
	archiveID := "file:" + s.ArchiveDir
	if s.S3Endpoint != "" {
		if err := s.validateS3Retention(); err != nil {
			return "", fmt.Errorf("consistency key: archive: %w", err)
		}
		archiveID = "s3:" + s.S3Bucket
	}
	return fmt.Sprintf("%s|%s", algorithm, archiveID), nil
}

// resolveS3Transport returns the minio-ready endpoint (scheme stripped), the
// resolved useSSL flag, and the resolved transport label ("tls"/"http"). It
// is the single source of truth used by both Archive() and Transport(): the
// reported label is the resolver's own output, so the two can never diverge
// (FM-9). All error text quotes sanitizeAddr(...) — userinfo/query/fragment
// are never echoed (F4).
func (s SigningArchive) resolveS3Transport() (endpoint string, useSSL bool, transport string, err error) {
	raw := s.S3Endpoint
	display := sanitizeAddr(raw)
	if strings.TrimSpace(raw) != raw {
		return "", false, "", fmt.Errorf("s3 endpoint %q must not contain surrounding whitespace", display)
	}
	if strings.Contains(raw, "://") {
		scheme, rest, _ := strings.Cut(raw, "://")
		switch strings.ToLower(scheme) {
		case "http", "https":
		default:
			return "", false, "", fmt.Errorf("s3 endpoint %q has unsupported scheme %q: use http or https", display, scheme)
		}
		if rest == "" || strings.Contains(rest, "://") {
			return "", false, "", fmt.Errorf("s3 endpoint %q is malformed: expected host[:port]", display)
		}
		if strings.ToLower(scheme) == "https" && !s.S3UseSSL {
			return "", false, "", fmt.Errorf("s3 endpoint %q carries an https scheme but %s is not enabled: set %s=true or remove the scheme from the endpoint", display, EnvS3UseSSL, EnvS3UseSSL)
		}
		if strings.ToLower(scheme) == "http" && s.S3UseSSL {
			return "", false, "", fmt.Errorf("s3 endpoint %q explicitly uses http but %s=true: remove the scheme or disable TLS", display, EnvS3UseSSL)
		}
		raw = scheme + "://" + rest
	} else {
		raw = "//" + raw // schemeless: parse host[:port] only
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil ||
		parsed.Fragment != "" || parsed.RawQuery != "" || parsed.ForceQuery ||
		(parsed.Path != "" && parsed.Path != "/") {
		return "", false, "", fmt.Errorf("s3 endpoint %q is malformed: expected host[:port] with no path, query, userinfo or fragment", display)
	}
	if s.S3UseSSL {
		return parsed.Host, true, "tls", nil
	}
	if !loopbackHost(parsed.Hostname()) && !isS3DevelopmentAllowlistEndpoint(parsed.Host) {
		return "", false, "", fmt.Errorf("s3 endpoint %q uses plaintext http on a non-loopback host: set %s=true or -s3-use-ssl to enable TLS; plaintext is allowed only for local development endpoints or minio:9000", display, EnvS3UseSSL)
	}
	return parsed.Host, false, "http", nil
}

// isS3DevelopmentAllowlistEndpoint is intentionally narrower than
// loopbackHost: the verification compose stack uses exactly minio:9000.
// Compare the complete host:port so minio:9001 and arbitrary remote hosts do
// not become insecure-S3 escape hatches.
func isS3DevelopmentAllowlistEndpoint(host string) bool {
	return strings.EqualFold(host, "minio:9000")
}

// resolveVaultTransport validates the Vault addr against the
// https-or-explicit-loopback-http rule (mirrors internal/auth validateJWKSURL;
// the loopbackHost rule set is deliberately duplicated — see the helper
// comment) and returns the resolved transport label ("tls"/"http"). Signer()
// consumes it for validation, Transport() for reporting; both share this
// single parse, so the reported label can never diverge from the signer
// actually built (FM-9). All error text quotes sanitizeAddr(...) — userinfo,
// query and fragment are never echoed (F4).
func (s SigningArchive) resolveVaultTransport() (string, error) {
	raw := s.VaultAddr
	if strings.TrimSpace(raw) != raw {
		return "", fmt.Errorf("vault addr %q must not contain surrounding whitespace", sanitizeAddr(raw))
	}
	if !strings.Contains(raw, "://") {
		return "", fmt.Errorf("vault addr %q must include a scheme: use https://host[:port] (http:// is allowed only on loopback hosts with %s=true)", sanitizeAddr(raw), EnvAllowInsecureVaultLoopback)
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil ||
		parsed.Fragment != "" || parsed.RawQuery != "" || parsed.ForceQuery ||
		(parsed.Path != "" && parsed.Path != "/") {
		return "", fmt.Errorf("vault addr %q is malformed: expected scheme://host[:port] with no path, query, userinfo or fragment", sanitizeAddr(raw))
	}
	switch parsed.Scheme { // url.Parse lowercases the scheme; HTTPS://… is handled here
	case "https":
		return "tls", nil
	case "http":
		if s.AllowInsecureVaultLoopback && loopbackHost(parsed.Hostname()) {
			return "http", nil
		}
		if loopbackHost(parsed.Hostname()) {
			return "", fmt.Errorf("vault addr %q uses plaintext http on a loopback host: set %s=true to allow it, or use https://", sanitizeAddr(raw), EnvAllowInsecureVaultLoopback)
		}
		return "", fmt.Errorf("vault addr %q uses plaintext http on a non-loopback host: %s never permits this — use https://", sanitizeAddr(raw), EnvAllowInsecureVaultLoopback)
	default:
		return "", fmt.Errorf("vault addr %q has unsupported scheme %q: use https:// (or http:// on loopback hosts with %s=true)", sanitizeAddr(raw), parsed.Scheme, EnvAllowInsecureVaultLoopback)
	}
}

// Transport reports the resolved transport per external leg: the S3 leg and
// the Vault leg each resolve to "tls", "http", or "local" (leg unconfigured).
// Both legs are always reported so a mixed S3+Vault configuration never hides
// one leg's transport (F3). Each leg fails closed on a partial configuration,
// mirroring Archive()/Signer(). Labels are the resolvers' own outputs — never
// re-derived from the raw address — so the reported transport cannot diverge
// from the store/signer actually built (FM-9).
func (s SigningArchive) Transport() (s3, vault string, err error) {
	s3Configured := s.S3Endpoint != "" || s.S3Bucket != "" || s.S3AccessKey != "" || s.S3SecretKey != ""
	if s3Configured {
		if s.S3Endpoint == "" || s.S3Bucket == "" || s.S3AccessKey == "" || s.S3SecretKey == "" {
			return "", "", fmt.Errorf("s3 archive requires endpoint, bucket, access key and secret key together")
		}
		_, _, s3Transport, resolveErr := s.resolveS3Transport()
		if resolveErr != nil {
			return "", "", resolveErr
		}
		s3 = s3Transport
	} else {
		s3 = "local"
	}
	vaultConfigured := s.VaultAddr != "" || s.VaultToken != "" || s.VaultTransitKey != ""
	if vaultConfigured {
		if s.VaultAddr == "" || s.VaultToken == "" || s.VaultTransitKey == "" {
			return "", "", fmt.Errorf("vault signing requires addr, token and transit key together")
		}
		vaultTransport, resolveErr := s.resolveVaultTransport()
		if resolveErr != nil {
			return "", "", resolveErr
		}
		vault = vaultTransport
	} else {
		vault = "local"
	}
	return s3, vault, nil
}

// sanitizeAddr renders an external address for error messages without
// echoing embedded credentials: URL userinfo (everything before the last
// '@'), query and fragment are stripped. host[:port] is the only accepted
// address shape, so nothing actionable is lost. A value that sanitizes to
// empty is reported as "<addr>" — the raw value is never echoed (F4).
func sanitizeAddr(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if i := strings.IndexAny(trimmed, "?#"); i >= 0 {
		trimmed = trimmed[:i]
	}
	if i := strings.LastIndex(trimmed, "@"); i >= 0 {
		trimmed = trimmed[i+1:]
	}
	if trimmed == "" {
		return "<addr>"
	}
	return trimmed
}

// loopbackHost is a deliberate duplicate of internal/auth/verifier.go's
// loopbackHost (localhost, host.docker.internal, gateway.docker.internal,
// loopback IPs). It must stay in lockstep with that rule set — the mirrored
// acceptance table (TestSignerValidatesVaultAddr vs
// TestJWKSURLRequiresHTTPSOrExplicitLoopback) is the drift guard. Extracting
// to a shared stdlib leaf package is a possible future change; importing the
// auth helper would add a dependency edge the architecture gate rejects.
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

// isKnownDefaultSecret reads the single-source deny-list from the service
// package (exact match only).
func isKnownDefaultSecret(secret string) bool {
	for _, candidate := range service.KnownDefaultSecrets() {
		if secret == candidate {
			return true
		}
	}
	return false
}
