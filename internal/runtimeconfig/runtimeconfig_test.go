package runtimeconfig

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/service"
)

func TestSignerSelection(t *testing.T) {
	cfg := SigningArchive{SigningSecret: "secret"}
	signer, err := cfg.Signer()
	if err != nil || signer != nil {
		t.Fatalf("no vault config: signer=%v err=%v, want nil/nil", signer, err)
	}

	cfg = SigningArchive{SigningSecret: "secret", VaultAddr: "https://vault.example.com:8200", VaultToken: "t", VaultTransitKey: "audit-checkpoints"}
	signer, err = cfg.Signer()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := signer.(*security.VaultTransitSigner); !ok {
		t.Fatalf("signer type=%T, want *security.VaultTransitSigner", signer)
	}
	if signer.Algorithm() != "vault-transit:audit-checkpoints" {
		t.Fatalf("algorithm=%s", signer.Algorithm())
	}

	// 部分配置必须报错而不是静默回退到 HMAC。
	cfg = SigningArchive{SigningSecret: "secret", VaultAddr: "http://vault:8200"}
	if _, err := cfg.Signer(); err == nil || !strings.Contains(err.Error(), "together") {
		t.Fatalf("partial vault config err=%v, want validation error", err)
	}
}

func TestArchiveSelection(t *testing.T) {
	cfg := SigningArchive{ArchiveDir: "/tmp/archive"}
	store, err := cfg.Archive()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.(*archive.FileStore); !ok {
		t.Fatalf("default archive type=%T, want *archive.FileStore", store)
	}

	cfg = SigningArchive{ArchiveDir: "/tmp/archive", S3Endpoint: "localhost:9000", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s"}
	store, err = cfg.Archive()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.(*archive.S3Store); !ok {
		t.Fatalf("s3 archive type=%T, want *archive.S3Store", store)
	}

	cfg = SigningArchive{S3Endpoint: "localhost:9000"}
	if _, err := cfg.Archive(); err == nil {
		t.Fatal("partial s3 config must fail")
	}
}

// TestDefaultsSatisfyService ensures the service layer accepts the
// defaults this package produces.
func TestDefaultsSatisfyService(t *testing.T) {
	cfg := SigningArchive{SigningSecret: "secret", ArchiveDir: t.TempDir()}
	signer, err := cfg.Signer()
	if err != nil {
		t.Fatal(err)
	}
	store, err := cfg.Archive()
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(nil, service.Config{SigningSecret: cfg.SigningSecret, ArchiveDir: cfg.ArchiveDir, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if svc.Config.Signer == nil || svc.Config.Archive == nil {
		t.Fatal("service defaults missing")
	}
	_ = signer
	_ = store
}

// TestSignerRejectsDefaultSecretWithVault pins REQ-5/T10: a well-known
// default signing secret is rejected whenever any Vault setting is present,
// before any Vault client is constructed.
func TestSignerRejectsDefaultSecretWithVault(t *testing.T) {
	cfg := SigningArchive{SigningSecret: "development-signing-key-change-me", VaultAddr: "http://vault:8200", VaultToken: "t", VaultTransitKey: "audit-checkpoints"}
	if _, err := cfg.Signer(); !errors.Is(err, service.ErrDefaultSecret) {
		t.Fatalf("err=%v, want ErrDefaultSecret", err)
	}
}

// TestArchiveRejectsDefaultSecretWithS3 pins REQ-5/T10: a well-known
// default encryption key is rejected whenever any S3 setting is present,
// before any S3 client is constructed.
func TestArchiveRejectsDefaultSecretWithS3(t *testing.T) {
	cfg := SigningArchive{EncryptionKey: "development-encryption-key-change-me", S3Endpoint: "localhost:9000", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s"}
	if _, err := cfg.Archive(); !errors.Is(err, service.ErrDefaultSecret) {
		t.Fatalf("err=%v, want ErrDefaultSecret", err)
	}
}

// TestPartialExternalConfigRejectedWithDefaultSecret covers T11 for the
// default-secret corner: partial Vault/S3 configuration is rejected (never
// a silent HMAC/file fallback) even when the secret is a known default.
func TestPartialExternalConfigRejectedWithDefaultSecret(t *testing.T) {
	cfg := SigningArchive{SigningSecret: "development-signing-key-change-me", VaultAddr: "http://vault:8200"}
	if _, err := cfg.Signer(); err == nil {
		t.Fatal("partial vault config must fail")
	}
	cfg = SigningArchive{EncryptionKey: "development-encryption-key-change-me", S3Endpoint: "localhost:9000"}
	if _, err := cfg.Archive(); err == nil {
		t.Fatal("partial s3 config must fail")
	}
}

// TestEnvNameConstants pins the shared env-var names (T12 golden): both
// binaries resolve secrets from these constants, so identical deployment
// input means identical resolved keys across processes.
func TestEnvNameConstants(t *testing.T) {
	if EnvSigningSecret != "AUDIT_SIGNING_SECRET" {
		t.Fatalf("EnvSigningSecret=%q", EnvSigningSecret)
	}
	if EnvEncryptionKey != "AUDIT_ENCRYPTION_KEY" {
		t.Fatalf("EnvEncryptionKey=%q", EnvEncryptionKey)
	}
	if EnvDevSecrets != "AUDIT_ALLOW_DEV_SECRETS" {
		t.Fatalf("EnvDevSecrets=%q", EnvDevSecrets)
	}
	if EnvS3UseSSL != "AUDIT_S3_USE_SSL" {
		t.Fatalf("EnvS3UseSSL=%q", EnvS3UseSSL)
	}
	if EnvAllowInsecureVaultLoopback != "AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK" {
		t.Fatalf("EnvAllowInsecureVaultLoopback=%q", EnvAllowInsecureVaultLoopback)
	}
}

// fullS3 returns a SigningArchive with a complete S3 leg and a non-default
// encryption key so default-secret and partiality branches never trigger.
func fullS3(t *testing.T, endpoint string, useSSL bool) SigningArchive {
	t.Helper()
	return SigningArchive{EncryptionKey: "test-encryption-key", S3Endpoint: endpoint, S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s", S3UseSSL: useSSL}
}

// fullVault returns a SigningArchive with a complete Vault leg and a
// non-default signing secret so default-secret and partiality branches never
// trigger.
func fullVault(t *testing.T, addr string, allowInsecureLoopback bool) SigningArchive {
	t.Helper()
	return SigningArchive{SigningSecret: "test-secret", VaultAddr: addr, VaultToken: "t", VaultTransitKey: "audit-checkpoints", AllowInsecureVaultLoopback: allowInsecureLoopback}
}

// TestArchiveResolvesS3TransportUseSSL is AC-1 (REQ-TLS-1/2/3): the seam
// captures the resolved endpoint and useSSL flag so the transport is proven
// data-driven. Uppercase-scheme rows (F1) must resolve exactly like their
// lowercase counterparts.
func TestArchiveResolvesS3TransportUseSSL(t *testing.T) {
	cases := []struct {
		name            string
		endpoint        string
		useSSL          bool
		wantUseSSL      bool
		wantEndpoint    string
		wantTransport   string
		wantErrContains string
	}{
		{"schemeless plaintext", "s3.example.com:9000", false, false, "s3.example.com:9000", "http", ""},
		{"schemeless tls", "s3.example.com:9000", true, true, "s3.example.com:9000", "tls", ""},
		{"https scheme stripped", "https://s3.example.com", true, true, "s3.example.com", "tls", ""},
		{"uppercase https scheme stripped", "HTTPS://s3.example.com", true, true, "s3.example.com", "tls", ""},
		{"uppercase http scheme stripped", "HTTP://s3.example.com", false, false, "s3.example.com", "http", ""},
		{"https scheme without flag fails", "https://s3.example.com", false, false, "", "", EnvS3UseSSL},
		{"uppercase https scheme without flag fails", "HTTPS://s3.example.com", false, false, "", "", EnvS3UseSSL},
		{"http scheme conflicts with flag", "http://s3.example.com", true, false, "", "", EnvS3UseSSL},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var capturedEndpoint string
			var capturedUseSSL bool
			swapped := false
			original := newS3Store
			newS3Store = func(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*archive.S3Store, error) {
				capturedEndpoint, capturedUseSSL = endpoint, useSSL
				swapped = true
				return archive.NewS3StoreWithClient(nil, bucket), nil
			}
			t.Cleanup(func() { newS3Store = original })

			cfg := fullS3(t, tc.endpoint, tc.useSSL)
			store, err := cfg.Archive()
			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("err=%v, want error containing %q", err, tc.wantErrContains)
				}
				if swapped {
					t.Fatal("constructor must not run on a resolution error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := store.(*archive.S3Store); !ok {
				t.Fatalf("store type=%T, want *archive.S3Store", store)
			}
			if !swapped || capturedUseSSL != tc.wantUseSSL || capturedEndpoint != tc.wantEndpoint {
				t.Fatalf("seam captured endpoint=%q useSSL=%v (swapped=%v), want endpoint=%q useSSL=%v", capturedEndpoint, capturedUseSSL, swapped, tc.wantEndpoint, tc.wantUseSSL)
			}
			_, _, transport, resolveErr := cfg.resolveS3Transport()
			if resolveErr != nil {
				t.Fatal(resolveErr)
			}
			if transport != tc.wantTransport {
				t.Fatalf("transport=%q, want %q", transport, tc.wantTransport)
			}
		})
	}
}

// TestArchiveConstructorHasNoBooleanLiteral pins REQ-TLS-3 at source level:
// the only NewS3Store call site (the seam default) passes the resolved useSSL
// variable; no literal true/false survives in an S3-constructor call.
func TestArchiveConstructorHasNoBooleanLiteral(t *testing.T) {
	src, err := os.ReadFile("runtimeconfig.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(src), "\n") {
		if !strings.Contains(line, "NewS3Store(") {
			continue
		}
		if strings.Contains(line, ", false)") || strings.Contains(line, ", true)") {
			t.Errorf("S3-constructor call carries a literal boolean: %q", line)
		}
	}
}

// TestSignerValidatesVaultAddr is AC-2 (REQ-TLS-4) and mirrors
// internal/auth/verifier_test.go:TestJWKSURLRequiresHTTPSOrExplicitLoopback
// row-for-row for the loopback rule set (F6 drift guard). Every error case
// also asserts the signer is nil (validation precedes construction).
func TestSignerValidatesVaultAddr(t *testing.T) {
	cases := []struct {
		name          string
		addr          string
		allowInsecure bool
		wantErr       bool
		wantErrText   string
		wantTransport string
	}{
		{"https remote", "https://vault.example.com:8200", false, false, "", "tls"},
		{"uppercase https", "HTTPS://vault.example.com:8200", false, false, "", "tls"},
		{"https loopback", "https://127.0.0.1:8200", false, false, "", "tls"},
		{"http loopback without opt-in", "http://127.0.0.1:8200", false, true, EnvAllowInsecureVaultLoopback, ""},
		{"http loopback with opt-in", "http://127.0.0.1:8200", true, false, "", "http"},
		{"http localhost with opt-in", "http://localhost:8200", true, false, "", "http"},
		{"http host.docker.internal with opt-in", "http://host.docker.internal:8200", true, false, "", "http"},
		{"http gateway.docker.internal with opt-in", "http://gateway.docker.internal:8200", true, false, "", "http"},
		{"http non-loopback never allowed", "http://vault.example.com:8200", true, true, "non-loopback", ""},
		{"schemeless remote", "vault.example.com:8200", true, true, "scheme", ""},
		{"schemeless loopback", "localhost:8200", true, true, "scheme", ""},
		{"trailing whitespace", "http://127.0.0.1:8200 ", true, true, "whitespace", ""},
		{"empty hostname", "https://:8200", false, true, "malformed", ""},
		{"path-bearing addr", "https://vault:8200/v1", false, true, "malformed", ""},
		{"query-bearing addr", "https://vault:8200?x=1", false, true, "malformed", ""},
		{"userinfo embedded", "http://user:pass@vault:8200", false, true, "malformed", ""},
		{"unsupported scheme", "ftp://vault:8200", false, true, "unsupported scheme", ""},
		{"trailing query mark", "https://vault.example.com:8200?", false, true, "malformed", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := fullVault(t, tc.addr, tc.allowInsecure)
			signer, err := cfg.Signer()
			if tc.wantErr {
				if err == nil {
					t.Fatal("validation must fail")
				}
				if signer != nil {
					t.Fatal("no signer may be constructed on a validation error")
				}
				if tc.wantErrText != "" && !strings.Contains(err.Error(), tc.wantErrText) {
					t.Fatalf("err=%v, want text containing %q", err, tc.wantErrText)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if signer == nil || signer.Algorithm() != "vault-transit:audit-checkpoints" {
				t.Fatalf("signer=%v algorithm=%v, want vault-transit signer", signer, algorithmOf(signer))
			}
			transport, resolveErr := cfg.resolveVaultTransport()
			if resolveErr != nil {
				t.Fatal(resolveErr)
			}
			if transport != tc.wantTransport {
				t.Fatalf("transport=%q, want %q", transport, tc.wantTransport)
			}
		})
	}
}

func algorithmOf(signer service.Signer) string {
	if signer == nil {
		return "<nil>"
	}
	return signer.Algorithm()
}

// TestResolveS3TransportRejectsMalformed is the FM-4 malformed-input matrix
// (QA F2/F5): every row must produce an actionable error instead of reaching
// minio's own "Endpoint url cannot have fully qualified paths."
func TestResolveS3TransportRejectsMalformed(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
		useSSL   bool
	}{
		{"port-only", ":9000", false},
		{"authority-only", "//:9000", false},
		{"path", "s3.example.com:9000/path", false},
		{"query", "s3.example.com:9000?x=1", false},
		{"fragment", "s3.example.com:9000#frag", false},
		{"userinfo", "http://user:pass@s3.example.com:9000", false},
		{"leading whitespace", " s3.example.com:9000", false},
		{"trailing whitespace", "s3.example.com:9000 ", false},
		{"repeated scheme", "http://s3.example.com://x", false},
		{"scheme without host", "https://", true},
		{"unsupported scheme", "ftp://s3.example.com", false},
		{"bare path", "/path", false},
		{"trailing query mark", "s3.example.com:9000?", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := fullS3(t, tc.endpoint, tc.useSSL)
			if _, _, _, err := cfg.resolveS3Transport(); err == nil {
				t.Fatalf("endpoint %q must be rejected", tc.endpoint)
			}
			if _, err := cfg.Archive(); err == nil {
				t.Fatalf("Archive() must propagate the malformed-endpoint error for %q", tc.endpoint)
			}
		})
	}
}

// TestTransportPerLegLabels is F3: both legs are always reported, so a mixed
// S3+Vault configuration never hides one leg's transport; unconfigured legs
// read "local"; partial legs fail closed.
func TestTransportPerLegLabels(t *testing.T) {
	cases := []struct {
		name      string
		cfg       SigningArchive
		wantS3    string
		wantVault string
		wantErr   bool
	}{
		{"no external legs", SigningArchive{}, "local", "local", false},
		{"s3 tls only", fullS3(t, "https://s3.example.com", true), "tls", "local", false},
		{"s3 http only", fullS3(t, "s3.example.com:9000", false), "http", "local", false},
		{"vault tls only", fullVault(t, "https://vault.example.com:8200", false), "local", "tls", false},
		{"mixed s3 tls + vault tls", func() SigningArchive {
			cfg := fullS3(t, "https://s3.example.com", true)
			cfg.SigningSecret = "test-secret"
			cfg.VaultAddr = "https://vault.example.com:8200"
			cfg.VaultToken = "t"
			cfg.VaultTransitKey = "audit-checkpoints"
			return cfg
		}(), "tls", "tls", false},
		{"mixed s3 http + vault loopback http", func() SigningArchive {
			cfg := fullS3(t, "http://s3.example.com", false)
			cfg.SigningSecret = "test-secret"
			cfg.VaultAddr = "http://127.0.0.1:8200"
			cfg.VaultToken = "t"
			cfg.VaultTransitKey = "audit-checkpoints"
			cfg.AllowInsecureVaultLoopback = true
			return cfg
		}(), "http", "http", false},
		{"partial s3 fails closed", SigningArchive{S3Endpoint: "s3.example.com:9000"}, "", "", true},
		{"partial vault fails closed", SigningArchive{VaultAddr: "https://vault.example.com:8200"}, "", "", true},
		{"malformed s3 fails closed", fullS3(t, ":9000", false), "", "", true},
		{"http non-loopback vault fails closed", fullVault(t, "http://vault.example.com:8200", true), "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s3, vault, err := tc.cfg.Transport()
			if tc.wantErr {
				if err == nil {
					t.Fatal("Transport() must fail closed")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if s3 != tc.wantS3 || vault != tc.wantVault {
				t.Fatalf("Transport()=(%q,%q), want (%q,%q)", s3, vault, tc.wantS3, tc.wantVault)
			}
		})
	}
}

// TestTransportMatchesResolvedStores is the FM-9 divergence pin (QA F1): the
// store/signer actually built and the reported transport are both derived
// from the same resolver, so for every row the built useSSL flag equals the
// reported S3 label and a built Vault signer implies the reported Vault label.
func TestTransportMatchesResolvedStores(t *testing.T) {
	s3Rows := []struct {
		endpoint string
		useSSL   bool
		want     string
	}{
		{"s3.example.com:9000", false, "http"},
		{"s3.example.com:9000", true, "tls"},
		{"https://s3.example.com", true, "tls"},
		{"HTTPS://s3.example.com", true, "tls"},
		{"HTTP://s3.example.com", false, "http"},
	}
	for _, row := range s3Rows {
		t.Run("s3/"+row.endpoint+"/"+row.want, func(t *testing.T) {
			var builtUseSSL bool
			original := newS3Store
			newS3Store = func(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*archive.S3Store, error) {
				builtUseSSL = useSSL
				return archive.NewS3StoreWithClient(nil, bucket), nil
			}
			t.Cleanup(func() { newS3Store = original })

			cfg := fullS3(t, row.endpoint, row.useSSL)
			if _, err := cfg.Archive(); err != nil {
				t.Fatal(err)
			}
			s3, vault, err := cfg.Transport()
			if err != nil {
				t.Fatal(err)
			}
			if vault != "local" {
				t.Fatalf("vault label=%q, want local", vault)
			}
			if s3 != row.want {
				t.Fatalf("reported transport=%q, want %q (resolver output)", s3, row.want)
			}
			if (builtUseSSL && s3 != "tls") || (!builtUseSSL && s3 != "http") {
				t.Fatalf("built store useSSL=%v diverges from reported transport=%q", builtUseSSL, s3)
			}
		})
	}

	vaultRows := []struct {
		addr  string
		allow bool
		want  string
	}{
		{"https://vault.example.com:8200", false, "tls"},
		{"HTTPS://vault.example.com:8200", false, "tls"},
		{"http://127.0.0.1:8200", true, "http"},
	}
	for _, row := range vaultRows {
		t.Run("vault/"+row.addr, func(t *testing.T) {
			cfg := fullVault(t, row.addr, row.allow)
			signer, err := cfg.Signer()
			if err != nil {
				t.Fatal(err)
			}
			if signer == nil {
				t.Fatal("signer must be built for a valid vault config")
			}
			s3, vault, err := cfg.Transport()
			if err != nil {
				t.Fatal(err)
			}
			if s3 != "local" {
				t.Fatalf("s3 label=%q, want local", s3)
			}
			if vault != row.want {
				t.Fatalf("reported vault transport=%q, want %q", vault, row.want)
			}
		})
	}
}

// TestValidationErrorsNeverEchoCredentials is F4: a credential misplaced as
// URL userinfo (or query) must never appear in the fail-fast error text.
func TestValidationErrorsNeverEchoCredentials(t *testing.T) {
	const password = "super-secret-token-value"
	cases := []struct {
		name string
		cfg  SigningArchive
		call func(SigningArchive) error
	}{
		{"vault userinfo", fullVault(t, "http://user:"+password+"@vault:8200", false), func(c SigningArchive) error {
			_, err := c.Signer()
			return err
		}},
		{"vault bare userinfo", fullVault(t, "http://"+password+"@vault:8200", false), func(c SigningArchive) error {
			_, err := c.Signer()
			return err
		}},
		{"vault query credential", fullVault(t, "https://vault:8200?token="+password, false), func(c SigningArchive) error {
			_, err := c.Signer()
			return err
		}},
		{"s3 userinfo", fullS3(t, "http://user:"+password+"@s3.example.com:9000", false), func(c SigningArchive) error {
			_, err := c.Archive()
			return err
		}},
		{"s3 query credential", fullS3(t, "https://s3.example.com:9000?secret="+password, false), func(c SigningArchive) error {
			_, err := c.Archive()
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(tc.cfg)
			if err == nil {
				t.Fatal("config with an embedded credential must fail validation")
			}
			if strings.Contains(err.Error(), password) {
				t.Fatalf("error text echoes the credential: %q", err.Error())
			}
		})
	}
}

// TestS3PlaintextNonLoopbackPermitted pins the F2 decision (documented
// residual): a scheme-less (or explicit-http) non-loopback S3 endpoint with
// S3UseSSL=false resolves to plaintext http without error. The trade-off is
// deliberate and observable — transport_s3=http — and this test is the marker
// that must change if the posture is ever hardened.
func TestS3PlaintextNonLoopbackPermitted(t *testing.T) {
	for _, endpoint := range []string{"s3.example.com:9000", "http://s3.example.com:9000", "minio:9000"} {
		t.Run(endpoint, func(t *testing.T) {
			original := newS3Store
			newS3Store = func(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*archive.S3Store, error) {
				return archive.NewS3StoreWithClient(nil, bucket), nil
			}
			t.Cleanup(func() { newS3Store = original })

			cfg := fullS3(t, endpoint, false)
			if _, err := cfg.Archive(); err != nil {
				t.Fatalf("plaintext non-loopback S3 must resolve (documented F2 residual): %v", err)
			}
			s3, _, err := cfg.Transport()
			if err != nil {
				t.Fatal(err)
			}
			if s3 != "http" {
				t.Fatalf("transport_s3=%q, want http (observational enforcement)", s3)
			}
		})
	}
}
