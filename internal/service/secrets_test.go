package service

import (
	"errors"
	"strings"
	"testing"
)

func TestNewRejectsMissingSecrets(t *testing.T) {
	svc, err := New(nil, Config{})
	if !errors.Is(err, ErrMissingSecret) {
		t.Fatalf("New with empty secrets err=%v (svc=%v), want ErrMissingSecret", err, svc)
	}
	if !strings.Contains(err.Error(), "AUDIT_SIGNING_SECRET") {
		t.Fatalf("error %q must name AUDIT_SIGNING_SECRET", err)
	}
	_, err = New(nil, Config{SigningSecret: "a-strong-signing-secret"})
	if !errors.Is(err, ErrMissingSecret) {
		t.Fatalf("New with empty encryption key err=%v, want ErrMissingSecret", err)
	}
	if !strings.Contains(err.Error(), "AUDIT_ENCRYPTION_KEY") {
		t.Fatalf("error %q must name AUDIT_ENCRYPTION_KEY", err)
	}
}

func TestNewRejectsDefaultSigningSecret(t *testing.T) {
	_, err := New(nil, Config{SigningSecret: devSigningSecret, EncryptionKey: "a-distinct-strong-encryption-key"})
	if !errors.Is(err, ErrDefaultSecret) {
		t.Fatalf("err=%v, want ErrDefaultSecret", err)
	}
	if !strings.Contains(err.Error(), "AUDIT_SIGNING_SECRET") {
		t.Fatalf("error %q must name AUDIT_SIGNING_SECRET", err)
	}
}

func TestNewRejectsDefaultEncryptionKey(t *testing.T) {
	_, err := New(nil, Config{SigningSecret: "a-strong-signing-secret", EncryptionKey: devEncryptionKey})
	if !errors.Is(err, ErrDefaultSecret) {
		t.Fatalf("err=%v, want ErrDefaultSecret", err)
	}
	if !strings.Contains(err.Error(), "AUDIT_ENCRYPTION_KEY") {
		t.Fatalf("error %q must name AUDIT_ENCRYPTION_KEY", err)
	}
	// The deny-list applies to both strings in both fields.
	_, err = New(nil, Config{SigningSecret: devEncryptionKey, EncryptionKey: "a-distinct-strong-encryption-key"})
	if !errors.Is(err, ErrDefaultSecret) {
		t.Fatalf("signing secret holding the encryption default: err=%v, want ErrDefaultSecret", err)
	}
}

func TestNewAcceptsNearMissSecrets(t *testing.T) {
	// Exact-match deny-list (REQ-6): the distinct local-only values used by
	// deploy/docker-compose.verify.yml and a near-miss suffix must pass.
	for _, cfg := range []Config{
		{SigningSecret: "local-only-change-me", EncryptionKey: "local-only-encryption-key"},
		{SigningSecret: "development-signing-key-change-me2", EncryptionKey: "a-distinct-strong-encryption-key"},
	} {
		svc, err := New(nil, cfg)
		if err != nil {
			t.Fatalf("New(%+v) err=%v, want accepted", cfg, err)
		}
		if svc.Config.SigningSecret != cfg.SigningSecret || svc.Config.EncryptionKey != cfg.EncryptionKey {
			t.Fatalf("resolved secrets %q/%q differ from input", svc.Config.SigningSecret, svc.Config.EncryptionKey)
		}
	}
}

func TestDevModePreservesLegacyBehavior(t *testing.T) {
	// REQ-7: with AllowDevSecrets the resolved config is byte-identical to
	// the pre-fix defaults (empty -> development defaults, encryption falls
	// back to the signing secret, HMAC signer uses the resolved secret).
	svc, err := New(nil, Config{AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if svc.Config.SigningSecret != devSigningSecret {
		t.Fatalf("SigningSecret=%q, want %q", svc.Config.SigningSecret, devSigningSecret)
	}
	if svc.Config.EncryptionKey != svc.Config.SigningSecret {
		t.Fatalf("EncryptionKey=%q, want fallback to SigningSecret %q", svc.Config.EncryptionKey, svc.Config.SigningSecret)
	}
	signer, ok := svc.Config.Signer.(hmacSigner)
	if !ok || signer.secret != devSigningSecret {
		t.Fatalf("signer=%T secret=%q, want hmacSigner with %q", svc.Config.Signer, signer.secret, devSigningSecret)
	}
	if svc.Config.Archive == nil || svc.Config.Now == nil {
		t.Fatal("archive/now defaults missing")
	}
	// REQ-3: dev secrets are independent of the auth-scoped dev flag; the
	// service config has no AllowDev field, so the plain config (what a
	// binary with -allow-dev-auth but without -allow-dev-secrets produces)
	// must still be rejected.
	if _, err := New(nil, Config{}); !errors.Is(err, ErrMissingSecret) {
		t.Fatalf("err=%v, want ErrMissingSecret without AllowDevSecrets", err)
	}
}

func TestKnownDefaultSecretsExactList(t *testing.T) {
	got := KnownDefaultSecrets()
	if len(got) != 2 || got[0] != devSigningSecret || got[1] != devEncryptionKey {
		t.Fatalf("KnownDefaultSecrets()=%v, want exactly {%q, %q}", got, devSigningSecret, devEncryptionKey)
	}
	for _, value := range []string{"local-only-change-me", "local-only-encryption-key", "development-signing-key-change-me2", ""} {
		for _, candidate := range got {
			if value == candidate {
				t.Fatalf("known-default list must not contain %q", value)
			}
		}
	}
}

func TestNewRejectsSharedSecret(t *testing.T) {
	_, err := New(nil, Config{SigningSecret: "same-strong-secret", EncryptionKey: "same-strong-secret"})
	if !errors.Is(err, ErrSharedSecret) {
		t.Fatalf("err=%v, want ErrSharedSecret", err)
	}
	// Dev mode preserves the shared-secret fallback semantics (REQ-7).
	svc, err := New(nil, Config{AllowDevSecrets: true, SigningSecret: "same-strong-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if svc.Config.EncryptionKey != "same-strong-secret" {
		t.Fatalf("EncryptionKey=%q, want shared-secret fallback", svc.Config.EncryptionKey)
	}
}

func TestNewAcceptsStrongDistinctSecrets(t *testing.T) {
	svc, err := New(nil, Config{SigningSecret: "signing-" + strings.Repeat("s", 40), EncryptionKey: "encryption-" + strings.Repeat("e", 40)})
	if err != nil {
		t.Fatal(err)
	}
	signer, ok := svc.Config.Signer.(hmacSigner)
	if !ok || signer.secret != svc.Config.SigningSecret {
		t.Fatalf("signer=%T secret=%q, want hmacSigner keyed by the signing secret", svc.Config.Signer, signer.secret)
	}
}
