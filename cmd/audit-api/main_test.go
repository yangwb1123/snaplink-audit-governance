package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/runtimeconfig"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// validAuthenticator is the AC-3-correct authenticator: a zero-value
// auth.Authenticator fails ValidateConfiguration ("no JWT verification trust
// source is configured"), so every check-config table that must reach the
// external-transport checks uses JWTSecret + AllowLocalHS256.
//
// testJWTSecret is a >=32-byte local HS256 key so every authenticator
// fixture passes the minJWTSecretBytes gate in ValidateConfiguration; it
// must stay >=32 bytes.
const testJWTSecret = "test-secret-0123456789abcdefghijklmnopqrs"

func validAuthenticator() auth.Authenticator {
	return auth.Authenticator{JWTSecret: testJWTSecret, AllowLocalHS256: true}
}

func validConfig() service.Config {
	return service.Config{SigningSecret: "test-secret", EncryptionKey: "test-key", AllowDevSecrets: true}
}

// TestRunCheckConfigS3HTTPSchemeFailsFast is AC-3 (REQ-TLS-2/5): an
// https-scheme endpoint without AUDIT_S3_USE_SSL fails fast with exit 1 and
// an actionable error naming the variable, and check_config=ok is never
// printed; the positive control (flag set) exits 0 with transport_s3=tls.
func TestRunCheckConfigS3HTTPSchemeFailsFast(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	external := runtimeconfig.SigningArchive{S3Endpoint: "https://s3.example.com", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s", S3UseSSL: false}
	exit := runCheckConfig(logger, validConfig(), external, validAuthenticator())
	if exit != 1 {
		t.Fatalf("exit=%d, want 1; log: %q", exit, buf.String())
	}
	out := buf.String()
	for _, want := range []string{runtimeconfig.EnvS3UseSSL, "https"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log must contain %q, got: %q", want, out)
		}
	}
	if strings.Contains(out, "check_config=ok") {
		t.Fatalf("check_config=ok must not be printed, got: %q", out)
	}

	buf.Reset()
	external.S3UseSSL = true
	external.ArchiveRetentionDays = 365
	exit = runCheckConfig(logger, validConfig(), external, validAuthenticator())
	if exit != 0 {
		t.Fatalf("positive control exit=%d, want 0; log: %q", exit, buf.String())
	}
	if !strings.Contains(buf.String(), "check_config=ok") || !strings.Contains(buf.String(), "transport_s3=tls") {
		t.Fatalf("positive control must print transport_s3=tls, got: %q", buf.String())
	}
}

// TestRunCheckConfigVaultFailFast covers REQ-TLS-4/5 through the API's
// check-config path: plaintext non-loopback and schemeless Vault addrs fail
// with exit 1 and actionable text; https passes with transport_vault=tls.
func TestRunCheckConfigVaultFailFast(t *testing.T) {
	cases := []struct {
		name        string
		addr        string
		allow       bool
		wantExit    int
		wantErrText string
	}{
		{"http non-loopback", "http://vault.example.com:8200", false, 1, "non-loopback"},
		{"http non-loopback even with opt-in", "http://vault.example.com:8200", true, 1, "non-loopback"},
		{"schemeless", "vault.example.com:8200", false, 1, "scheme"},
		{"https passes", "https://vault.example.com:8200", false, 0, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := log.New(&buf, "", 0)
			external := runtimeconfig.SigningArchive{VaultAddr: tc.addr, VaultToken: "t", VaultTransitKey: "audit-checkpoints", AllowInsecureVaultLoopback: tc.allow}
			exit := runCheckConfig(logger, validConfig(), external, validAuthenticator())
			if exit != tc.wantExit {
				t.Fatalf("exit=%d, want %d; log: %q", exit, tc.wantExit, buf.String())
			}
			out := buf.String()
			if tc.wantExit == 1 {
				if tc.wantErrText != "" && !strings.Contains(out, tc.wantErrText) {
					t.Fatalf("log must contain %q, got: %q", tc.wantErrText, out)
				}
				if strings.Contains(out, "check_config=ok") {
					t.Fatalf("check_config=ok must not be printed, got: %q", out)
				}
			} else if !strings.Contains(out, "transport_vault=tls") {
				t.Fatalf("ok line must report transport_vault=tls, got: %q", out)
			}
		})
	}
}

// TestCheckConfigTransportLine is AC-4 (REQ-TLS-6): the ok line carries
// per-leg transport labels for both legs, so a mixed S3+Vault configuration
// never hides one leg's transport (F3); unconfigured legs read "local".
func TestCheckConfigTransportLine(t *testing.T) {
	cases := []struct {
		name      string
		external  runtimeconfig.SigningArchive
		wantS3    string
		wantVault string
	}{
		{"no external legs", runtimeconfig.SigningArchive{}, "local", "local"},
		{"s3 tls", runtimeconfig.SigningArchive{S3Endpoint: "s3.example.com:9000", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s", S3UseSSL: true, ArchiveRetentionDays: 365}, "tls", "local"},
		{"s3 http", runtimeconfig.SigningArchive{S3Endpoint: "s3.example.com:9000", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s", S3UseSSL: false, ArchiveRetentionDays: 365}, "http", "local"},
		{"vault tls", runtimeconfig.SigningArchive{VaultAddr: "https://vault.example.com:8200", VaultToken: "t", VaultTransitKey: "audit-checkpoints"}, "local", "tls"},
		{"mixed s3 tls + vault tls", runtimeconfig.SigningArchive{
			S3Endpoint: "https://s3.example.com", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s", S3UseSSL: true, ArchiveRetentionDays: 365,
			VaultAddr: "https://vault.example.com:8200", VaultToken: "t", VaultTransitKey: "audit-checkpoints",
		}, "tls", "tls"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := log.New(&buf, "", 0)
			exit := runCheckConfig(logger, validConfig(), tc.external, validAuthenticator())
			if exit != 0 {
				t.Fatalf("exit=%d, want 0; log: %q", exit, buf.String())
			}
			out := buf.String()
			if !strings.Contains(out, "check_config=ok") {
				t.Fatalf("missing check_config=ok, got: %q", out)
			}
			for _, want := range []string{"transport_s3=" + tc.wantS3, "transport_vault=" + tc.wantVault} {
				if !strings.Contains(out, want) {
					t.Fatalf("log must contain %q, got: %q", want, out)
				}
			}
		})
	}
}

// TestTransportErrorFailsCheckConfig is FM-8: a Transport() failure (partial
// external config) exits 1 and never prints check_config=ok.
func TestTransportErrorFailsCheckConfig(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	external := runtimeconfig.SigningArchive{S3Endpoint: "s3.example.com:9000"} // partial S3
	exit := runCheckConfig(logger, validConfig(), external, validAuthenticator())
	if exit != 1 {
		t.Fatalf("exit=%d, want 1; log: %q", exit, buf.String())
	}
	if strings.Contains(buf.String(), "check_config=ok") {
		t.Fatalf("check_config=ok must not be printed, got: %q", buf.String())
	}
}

// TestRunCheckConfigRequiresArchiveRetentionDays pins the F1 leg-(i) wiring
// fail-closed contract through the API's check-config path: an S3 archive
// without a positive AUDIT_ARCHIVE_RETENTION_DAYS exits 1 naming the
// variable and never prints check_config=ok — the API can never boot or pass
// preflight toward an S3 store that writes without explicit per-object
// retention.
func TestRunCheckConfigRequiresArchiveRetentionDays(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	external := runtimeconfig.SigningArchive{S3Endpoint: "s3.example.com:9000", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s"}
	exit := runCheckConfig(logger, validConfig(), external, validAuthenticator())
	if exit != 1 {
		t.Fatalf("exit=%d, want 1; log: %q", exit, buf.String())
	}
	out := buf.String()
	if !strings.Contains(out, runtimeconfig.EnvArchiveRetentionDays) {
		t.Fatalf("log must name %s, got: %q", runtimeconfig.EnvArchiveRetentionDays, out)
	}
	if strings.Contains(out, "check_config=ok") {
		t.Fatalf("check_config=ok must not be printed, got: %q", out)
	}
}

// scriptedS3Client is a minimal archive.S3Client double for the API boot
// probe tests: it models the bucket configuration states Ready inspects
// (bucket exists, Object Lock enabled with a COMPLIANCE/365/DAYS default
// retention, versioning enabled) without a real endpoint. bucketExistsHook
// lets a test block at BucketExists until the probe context is cancelled.
type scriptedS3Client struct {
	bucket           bool
	lockStatus       string
	versioning       minio.BucketVersioningConfiguration
	bucketExistsHook func(ctx context.Context) (bool, error)
}

func (c *scriptedS3Client) StatObject(context.Context, string, string, minio.StatObjectOptions) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, errors.New("scripted client: StatObject not used")
}

func (c *scriptedS3Client) PutObject(context.Context, string, string, io.Reader, int64, minio.PutObjectOptions) (minio.UploadInfo, error) {
	return minio.UploadInfo{}, errors.New("scripted client: PutObject not used")
}

func (c *scriptedS3Client) GetObject(context.Context, string, string, minio.GetObjectOptions) (io.ReadCloser, error) {
	return nil, errors.New("scripted client: GetObject not used")
}

func (c *scriptedS3Client) BucketExists(ctx context.Context, _ string) (bool, error) {
	if c.bucketExistsHook != nil {
		return c.bucketExistsHook(ctx)
	}
	return c.bucket, nil
}

func (c *scriptedS3Client) GetObjectLockConfig(context.Context, string) (string, *minio.RetentionMode, *uint, *minio.ValidityUnit, error) {
	mode := minio.Compliance
	validity := uint(365)
	unit := minio.Days
	return c.lockStatus, &mode, &validity, &unit, nil
}

func (c *scriptedS3Client) GetBucketVersioning(context.Context, string) (minio.BucketVersioningConfiguration, error) {
	return c.versioning, nil
}

func lockedScriptedS3() *scriptedS3Client {
	return &scriptedS3Client{
		bucket:     true,
		lockStatus: "Enabled",
		versioning: minio.BucketVersioningConfiguration{Status: "Enabled"},
	}
}

// TestProbeArchiveReady is the API boot-probe mirror of the worker's
// TestProbeArchiveReady (F1 companion): the probe fails on a misconfigured
// destination, passes on a healthy one, and skips unconfigured stores, so an
// API pointed at a non-WORM-ready bucket fails fast at boot instead of
// writing objects whose receipts claim WORM protection.
func TestProbeArchiveReady(t *testing.T) {
	t.Run("s3 misconfigured fails", func(t *testing.T) {
		client := lockedScriptedS3()
		client.lockStatus = "" // Object Lock disabled
		s3 := archive.NewS3StoreWithClient(client, "worm-audit")
		var buf bytes.Buffer
		if err := probeArchiveReady(s3, log.New(&buf, "", 0)); err == nil {
			t.Fatal("probe must fail on a misconfigured S3 store")
		}
		if !strings.Contains(buf.String(), "archive_ready=failed store=*archive.S3Store") {
			t.Fatalf("log must identify the destination type, got: %q", buf.String())
		}
	})
	t.Run("s3 healthy passes", func(t *testing.T) {
		s3 := archive.NewS3StoreWithClient(lockedScriptedS3(), "worm-audit")
		var buf bytes.Buffer
		if err := probeArchiveReady(s3, log.New(&buf, "", 0)); err != nil {
			t.Fatalf("probe must pass on a locked bucket: %v", err)
		}
		if !strings.Contains(buf.String(), "archive_ready=ok") {
			t.Fatalf("log must report archive_ready=ok, got: %q", buf.String())
		}
	})
	t.Run("unconfigured store skipped", func(t *testing.T) {
		var buf bytes.Buffer
		logger := log.New(&buf, "", 0)
		if err := probeArchiveReady(&archive.FileStore{Dir: ""}, logger); err != nil {
			t.Fatalf("empty-dir FileStore must be skipped, got error: %v", err)
		}
		if err := probeArchiveReady(nil, logger); err != nil {
			t.Fatalf("nil store must be skipped, got error: %v", err)
		}
		if strings.Count(buf.String(), "archive_ready=skipped") != 2 {
			t.Fatalf("log must report archive_ready=skipped twice, got: %q", buf.String())
		}
	})
}

// TestProbeArchiveReadyBoundedByTimeout is the API boot-probe mirror of the
// worker's T7: a hung S3 endpoint must not block the boot probe beyond
// archiveReadyTimeout. The hook blocks until the probe context is cancelled,
// so the test is deterministic.
func TestProbeArchiveReadyBoundedByTimeout(t *testing.T) {
	client := lockedScriptedS3()
	client.bucketExistsHook = func(ctx context.Context) (bool, error) {
		<-ctx.Done()
		return false, ctx.Err()
	}
	s3 := archive.NewS3StoreWithClient(client, "worm-audit")
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	started := time.Now()
	err := probeArchiveReady(s3, logger)
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("probe must fail when the endpoint hangs")
	}
	if elapsed < 4*time.Second || elapsed > 8*time.Second {
		t.Fatalf("probe elapsed=%v, want bounded by the 5s constant (4-8s)", elapsed)
	}
	if !strings.Contains(buf.String(), "archive_ready=failed") {
		t.Fatalf("log must surface archive_ready=failed, got: %q", buf.String())
	}
}

var (
	apiBinOnce sync.Once
	apiBinPath string
	apiBinErr  error
)

// buildAPIBinary compiles the real audit-api binary once per test process
// into a private temp directory so the strict-bool flag plumbing (FM-1) can
// be exercised end-to-end.
func buildAPIBinary() (string, error) {
	apiBinOnce.Do(func() {
		dir, err := os.MkdirTemp("", "audit-api-test-")
		if err != nil {
			apiBinErr = err
			return
		}
		apiBinPath = filepath.Join(dir, "audit-api")
		cmd := exec.Command("go", "build", "-o", apiBinPath, ".")
		if out, err := cmd.CombinedOutput(); err != nil {
			apiBinErr = fmt.Errorf("go build api binary: %w: %s", err, out)
		}
	})
	return apiBinPath, apiBinErr
}

// TestStrictBoolEnvSubprocessFailsClosed is FM-1 (REQ-TLS-7): a malformed
// AUDIT_S3_USE_SSL value makes the real binary exit 1 naming the variable
// before flag.Parse — there is no fail-open path to plaintext.
func TestStrictBoolEnvSubprocessFailsClosed(t *testing.T) {
	binary, err := buildAPIBinary()
	if err != nil {
		t.Skipf("api binary unavailable: %v", err)
	}
	cmd := exec.Command(binary, "-check-config", "-allow-dev-secrets")
	cmd.Env = append(os.Environ(), "AUDIT_S3_USE_SSL=tru")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("binary must exit non-zero on a malformed AUDIT_S3_USE_SSL, output: %s", out)
	}
	if !strings.Contains(string(out), runtimeconfig.EnvS3UseSSL) || !strings.Contains(string(out), "not a valid boolean") {
		t.Fatalf("output must name the variable and the parse failure, got: %s", out)
	}
}

// TestStrictBoolEnvSubprocessPositive is the FM-1/REQ-TLS-7 wiring control:
// the same strict env vars, when well-formed, parse into the flags and drive
// the resolved transport end-to-end (S3 https → transport_s3=tls; loopback
// http Vault + opt-in → transport_vault=http). Network-free by construction:
// the API's check-config does not probe the archive destination.
func TestStrictBoolEnvSubprocessPositive(t *testing.T) {
	binary, err := buildAPIBinary()
	if err != nil {
		t.Skipf("api binary unavailable: %v", err)
	}
	baseEnv := []string{
		"AUDIT_SIGNING_SECRET=test-secret",
		"AUDIT_ENCRYPTION_KEY=test-key",
		"AUDIT_JWT_SECRET=" + testJWTSecret,
		"AUDIT_ALLOW_LOCAL_HS256=true",
	}
	run := func(env ...string) (string, error) {
		cmd := exec.Command(binary, "-check-config")
		cmd.Env = append(os.Environ(), baseEnv...)
		cmd.Env = append(cmd.Env, env...)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	t.Run("s3 use ssl true with https endpoint", func(t *testing.T) {
		out, err := run("AUDIT_S3_USE_SSL=true", "AUDIT_S3_ENDPOINT=https://s3.example.com",
			"AUDIT_S3_BUCKET=worm", "AUDIT_S3_ACCESS_KEY=k", "AUDIT_S3_SECRET_KEY=s",
			runtimeconfig.EnvArchiveRetentionDays+"=365")
		if err != nil {
			t.Fatalf("check-config must exit 0: %v\n%s", err, out)
		}
		if !strings.Contains(string(out), "check_config=ok") || !strings.Contains(string(out), "transport_s3=tls") {
			t.Fatalf("output must report check_config=ok with transport_s3=tls, got: %s", out)
		}
	})
	t.Run("vault loopback opt-in", func(t *testing.T) {
		out, err := run("AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK=true", "AUDIT_VAULT_ADDR=http://127.0.0.1:8200",
			"AUDIT_VAULT_TOKEN=t", "AUDIT_VAULT_TRANSIT_KEY=audit-checkpoints")
		if err != nil {
			t.Fatalf("check-config must exit 0: %v\n%s", err, out)
		}
		if !strings.Contains(string(out), "check_config=ok") || !strings.Contains(string(out), "transport_vault=http") {
			t.Fatalf("output must report check_config=ok with transport_vault=http, got: %s", out)
		}
	})
}

// TestStartupVaultHTTPFatal is QA F5/REQ-TLS-5: startup fails fast (logger
// path — non-zero exit, actionable text, no listen= line) when the Vault
// addr is plaintext non-loopback, proving preflight and runtime agree.
func TestStartupVaultHTTPFatal(t *testing.T) {
	binary, err := buildAPIBinary()
	if err != nil {
		t.Skipf("api binary unavailable: %v", err)
	}
	cmd := exec.Command(binary, "-state", filepath.Join(t.TempDir(), "state.json"))
	cmd.Env = append(os.Environ(),
		"AUDIT_SIGNING_SECRET=test-secret",
		"AUDIT_ENCRYPTION_KEY=test-key",
		"AUDIT_VAULT_ADDR=http://vault.example.com:8200",
		"AUDIT_VAULT_TOKEN=t",
		"AUDIT_VAULT_TRANSIT_KEY=audit-checkpoints",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("startup with a non-loopback plaintext Vault must exit non-zero, output: %s", out)
	}
	text := string(out)
	if !strings.Contains(text, runtimeconfig.EnvAllowInsecureVaultLoopback) {
		t.Fatalf("startup must surface the actionable validation text naming the opt-in, got: %s", text)
	}
	if strings.Contains(text, "listen=") {
		t.Fatalf("startup must fail before binding listeners, got: %s", text)
	}
}

// TestStrictBoolValue pins the single strict parse path: empty falls back,
// strconv.ParseBool literals parse, and a malformed value yields an error
// naming the variable (the os.Exit wrapper is covered by the subprocess
// tests).
func TestStrictBoolValue(t *testing.T) {
	if parsed, present, err := strictBoolValue("AUDIT_S3_USE_SSL", ""); err != nil || present || parsed {
		t.Fatalf("empty value: parsed=%v present=%v err=%v, want false/false/nil", parsed, present, err)
	}
	for _, literal := range []string{"true", "1", "t", "TRUE"} {
		parsed, present, err := strictBoolValue("AUDIT_S3_USE_SSL", literal)
		if err != nil || !present || !parsed {
			t.Fatalf("%q: parsed=%v present=%v err=%v, want true/true/nil", literal, parsed, present, err)
		}
	}
	for _, literal := range []string{"false", "0"} {
		parsed, present, err := strictBoolValue("AUDIT_S3_USE_SSL", literal)
		if err != nil || !present || parsed {
			t.Fatalf("%q: parsed=%v present=%v err=%v, want false/true/nil", literal, parsed, present, err)
		}
	}
	if _, _, err := strictBoolValue("AUDIT_S3_USE_SSL", "tru"); err == nil || !strings.Contains(err.Error(), "AUDIT_S3_USE_SSL=\"tru\" is not a valid boolean") {
		t.Fatalf("malformed value err=%v, want naming the variable", err)
	}
}

// TestStrictBoolEnvParses covers the non-exiting paths of strictBoolEnv:
// a well-formed value parses, and unset/empty falls back to the default.
// The malformed-value os.Exit path cannot run in-process and is covered by
// TestStrictBoolEnvSubprocessFailsClosed.
func TestStrictBoolEnvParses(t *testing.T) {
	t.Setenv(runtimeconfig.EnvS3UseSSL, "true")
	if !strictBoolEnv(runtimeconfig.EnvS3UseSSL, false) {
		t.Fatal("AUDIT_S3_USE_SSL=true must parse to true")
	}
	t.Setenv(runtimeconfig.EnvS3UseSSL, "false")
	if strictBoolEnv(runtimeconfig.EnvS3UseSSL, true) {
		t.Fatal("AUDIT_S3_USE_SSL=false must parse to false")
	}
	t.Setenv(runtimeconfig.EnvS3UseSSL, "")
	if !strictBoolEnv(runtimeconfig.EnvS3UseSSL, true) {
		t.Fatal("empty value must fall back to the default")
	}
}

func TestEnvOr(t *testing.T) {
	t.Setenv("AUDIT_ENV_OR_PROBE", "set")
	if got := envOr("AUDIT_ENV_OR_PROBE", "fallback"); got != "set" {
		t.Fatalf("envOr=%q, want set", got)
	}
	t.Setenv("AUDIT_ENV_OR_PROBE", "")
	if got := envOr("AUDIT_ENV_OR_PROBE", "fallback"); got != "fallback" {
		t.Fatalf("envOr=%q, want fallback", got)
	}
}

func TestIntEnv(t *testing.T) {
	t.Setenv("AUDIT_INT_ENV_PROBE", "50")
	if got := intEnv("AUDIT_INT_ENV_PROBE", 100); got != 50 {
		t.Fatalf("intEnv=%d, want 50", got)
	}
	t.Setenv("AUDIT_INT_ENV_PROBE", "")
	if got := intEnv("AUDIT_INT_ENV_PROBE", 100); got != 100 {
		t.Fatalf("intEnv=%d, want fallback 100", got)
	}
	t.Setenv("AUDIT_INT_ENV_PROBE", "abc")
	if got := intEnv("AUDIT_INT_ENV_PROBE", 100); got != 100 {
		t.Fatalf("intEnv=%d, want fallback 100 on malformed input", got)
	}
}

// TestDevAuthAllowlisted covers devAuthAllowlisted with a strict env value
// (true) and the unset fallback (false). The malformed-value os.Exit path is
// exercised by the subprocess tests only.
func TestDevAuthAllowlisted(t *testing.T) {
	t.Setenv(envDevAuth, "true")
	if !devAuthAllowlisted() {
		t.Fatal("AUDIT_ALLOW_DEV_AUTH=true must be allowlisted")
	}
	t.Setenv(envDevAuth, "")
	if devAuthAllowlisted() {
		t.Fatal("unset AUDIT_ALLOW_DEV_AUTH must not be allowlisted")
	}
}

// TestOpenStoreFileBackend covers openStore's file branch (the default
// production state backend): the store opens over a fresh path, logs the
// backend, and is usable.
func TestOpenStoreFileBackend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	st, err := openStore(path, "", logger)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if !strings.Contains(buf.String(), "state_backend=file") {
		t.Fatalf("log must report the file backend, got: %q", buf.String())
	}
	if _, err := st.Snapshot(); err != nil {
		t.Fatalf("opened store must be usable: %v", err)
	}
}

// TestOpenStorePostgresRejectsBadDSN covers openStore's postgres branch
// fail-fast: an unparseable DSN fails at the first connection attempt
// without touching the network (pgx parses lazily), so the store is never
// opened and the error propagates.
func TestOpenStorePostgresRejectsBadDSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	st, err := openStore(path, "postgres://user:pass@%zz.invalid:5432/audit?sslmode=disable", logger)
	if st != nil {
		st.Close()
	}
	if err == nil {
		t.Fatal("openStore must fail for an unparseable postgres DSN")
	}
	if strings.Contains(buf.String(), "state_backend=file") {
		t.Fatalf("file backend must not be reported on the postgres path, got: %q", buf.String())
	}
}

// TestBootstrapCreatesTenantAndIsIdempotent covers bootstrap end-to-end:
// the first pass creates tenant/schema/source, the second pass hits the
// already-present branches, and a blank tenant is a no-op.
func TestBootstrapCreatesTenantAndIsIdempotent(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc, err := service.New(st, service.Config{SigningSecret: "test-secret", EncryptionKey: "test-key", AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := bootstrap(svc, "demo"); err != nil {
			t.Fatalf("bootstrap pass %d: %v", i, err)
		}
	}
	if err := bootstrap(svc, "  "); err != nil {
		t.Fatalf("blank tenant bootstrap must be a no-op: %v", err)
	}
	foundTenant := false
	for _, tenant := range mustList(t, svc) {
		if tenant.ID == "demo" {
			foundTenant = true
		}
	}
	if !foundTenant {
		t.Fatal("bootstrap tenant missing after bootstrap")
	}
	if schemas, err := svc.ListSchemas("demo"); err != nil || len(schemas) != 1 {
		t.Fatalf("schemas=%d err=%v, want exactly 1", len(schemas), err)
	}
	if sources, err := svc.ListSources("demo"); err != nil || len(sources) != 1 {
		t.Fatalf("sources=%d err=%v, want exactly 1", len(sources), err)
	}
}

func mustList(t *testing.T, svc *service.Service) []domain.Tenant {
	t.Helper()
	tenants, err := svc.ListTenants()
	if err != nil {
		t.Fatal(err)
	}
	return tenants
}

// TestRunCheckConfigDevAuthGate covers the check-config dev-auth allowlist
// gate (AC-3): a flag-only -allow-dev-auth config fails preflight with the
// auditable marker and never prints check_config=ok; with the environment
// variable present the same config passes.
func TestRunCheckConfigDevAuthGate(t *testing.T) {
	authn := auth.Authenticator{JWTSecret: testJWTSecret, AllowLocalHS256: true, AllowDev: true}
	t.Setenv(envDevAuth, "")
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	exit := runCheckConfig(logger, validConfig(), runtimeconfig.SigningArchive{}, authn)
	if exit != 1 {
		t.Fatalf("exit=%d, want 1; log: %q", exit, buf.String())
	}
	out := buf.String()
	if !strings.Contains(out, "check_config=fail auth=dev_auth_flag_not_allowlisted") || strings.Contains(out, "check_config=ok") {
		t.Fatalf("log must carry the auditable marker without ok, got: %q", out)
	}
	buf.Reset()
	t.Setenv(envDevAuth, "true")
	exit = runCheckConfig(logger, validConfig(), runtimeconfig.SigningArchive{}, authn)
	if exit != 0 || !strings.Contains(buf.String(), "check_config=ok") {
		t.Fatalf("exit=%d, want 0 with check_config=ok; log: %q", exit, buf.String())
	}
}

// TestRunCheckConfigInvalidSecrets covers the secrets fail-fast branch of
// runCheckConfig (missing secrets exit 1 without touching external config).
func TestRunCheckConfigInvalidSecrets(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	exit := runCheckConfig(logger, service.Config{}, runtimeconfig.SigningArchive{}, validAuthenticator())
	if exit != 1 || !strings.Contains(buf.String(), "invalid secrets") {
		t.Fatalf("exit=%d, want 1 with invalid secrets; log: %q", exit, buf.String())
	}
	if strings.Contains(buf.String(), "check_config=ok") {
		t.Fatalf("check_config=ok must not be printed, got: %q", buf.String())
	}
}

// TestRunCheckConfigInvalidAuth covers the authentication-config fail-fast
// branch of runCheckConfig (no JWT trust source exits 1).
func TestRunCheckConfigInvalidAuth(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	exit := runCheckConfig(logger, validConfig(), runtimeconfig.SigningArchive{}, auth.Authenticator{})
	if exit != 1 || !strings.Contains(buf.String(), "invalid authentication configuration") {
		t.Fatalf("exit=%d, want 1 with invalid auth; log: %q", exit, buf.String())
	}
	if strings.Contains(buf.String(), "check_config=ok") {
		t.Fatalf("check_config=ok must not be printed, got: %q", buf.String())
	}
}

// TestRunCheckConfigRejectsShortJWTSecret is AC-3 (direct): a local HS256
// config whose secret is shorter than 32 bytes fails preflight with exit 1,
// the specific minimum-length marker, and no check_config=ok.
func TestRunCheckConfigRejectsShortJWTSecret(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	authn := auth.Authenticator{JWTSecret: "short", AllowLocalHS256: true}
	exit := runCheckConfig(logger, validConfig(), runtimeconfig.SigningArchive{}, authn)
	if exit != 1 {
		t.Fatalf("exit=%d, want 1; log: %q", exit, buf.String())
	}
	out := buf.String()
	if !strings.Contains(out, "invalid authentication configuration") || !strings.Contains(out, "at least 32 bytes") {
		t.Fatalf("log must carry the minimum-length marker, got: %q", out)
	}
	if strings.Contains(out, "check_config=ok") {
		t.Fatalf("check_config=ok must not be printed, got: %q", out)
	}
}

// TestRunCheckConfigReportsJWTSecretLength is AC-3 (direct): a compliant
// >=32-byte HS256 secret passes preflight and check_config=ok reports
// jwt_secret_length alongside the existing length metrics, never the value.
// F-2 pins the positional contract: jwt_secret_length sits between
// encryption_key_length and signer, and its value is an integer length
// (a %s-formatted field or a wrong slot fails here).
func TestRunCheckConfigReportsJWTSecretLength(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	exit := runCheckConfig(logger, validConfig(), runtimeconfig.SigningArchive{}, validAuthenticator())
	if exit != 0 {
		t.Fatalf("exit=%d, want 0; log: %q", exit, buf.String())
	}
	out := buf.String()
	okLine := checkConfigOKLine(out)
	if okLine == "" {
		t.Fatalf("check_config=ok must be printed, got: %q", out)
	}
	posSigning := strings.Index(okLine, "signing_secret_length=")
	posEnc := strings.Index(okLine, "encryption_key_length=")
	posJWT := strings.Index(okLine, "jwt_secret_length=")
	posSigner := strings.Index(okLine, "signer=")
	if !(posSigning >= 0 && posSigning < posEnc && posEnc < posJWT && posJWT < posSigner) {
		t.Fatalf("check_config=ok field order must be signing_secret_length < encryption_key_length < jwt_secret_length < signer, got: %q", okLine)
	}
	rest := okLine[posJWT+len("jwt_secret_length="):]
	if space := strings.Index(rest, " "); space >= 0 {
		rest = rest[:space]
	}
	if n, err := strconv.Atoi(rest); err != nil || n != len(testJWTSecret) {
		t.Fatalf("jwt_secret_length must be an integer length, got %q: %v", rest, err)
	}
	if strings.Contains(out, testJWTSecret) {
		t.Fatalf("secret values must never be printed, got: %q", out)
	}
}

// TestRunCheckConfigReportsZeroJWTLengthWhenUnset is F-3 (REQ-6): the
// jwt_secret_length field is unconditional — 0 when no JWT trust source is
// configured (JWKS-only and dev-only configs) — never omitted and never the
// value.
func TestRunCheckConfigReportsZeroJWTLengthWhenUnset(t *testing.T) {
	cases := []struct {
		name  string
		authn auth.Authenticator
	}{
		{"jwks-only", auth.Authenticator{JWKSURL: "https://issuer.example/jwks"}},
		{"dev-only", auth.Authenticator{AllowDev: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The dev-only cell needs the environment allowlist, or the
			// flag-only dev-auth gate fails the preflight by design.
			t.Setenv(envDevAuth, "true")
			var buf bytes.Buffer
			logger := log.New(&buf, "", 0)
			exit := runCheckConfig(logger, validConfig(), runtimeconfig.SigningArchive{}, tc.authn)
			if exit != 0 {
				t.Fatalf("exit=%d, want 0; log: %q", exit, buf.String())
			}
			okLine := checkConfigOKLine(buf.String())
			if okLine == "" || !strings.Contains(okLine, "jwt_secret_length=0") {
				t.Fatalf("check_config=ok must report jwt_secret_length=0 when unset, got: %q", buf.String())
			}
		})
	}
}

// TestRunCheckConfigRejectsJWTSecretSharedWithServiceSecrets is SEC-2: the
// local HS256 secret must be distinct from the signing and encryption
// secrets, so one operator-chosen string cannot both forge tokens and sign
// checkpoints or decrypt protected fields and exports. The distinctness
// check runs after the length gate, so the fixture secret is >=32 bytes.
func TestRunCheckConfigRejectsJWTSecretSharedWithServiceSecrets(t *testing.T) {
	signing := strings.Repeat("s", 32)
	encryption := strings.Repeat("k", 32)
	cfg := service.Config{SigningSecret: signing, EncryptionKey: encryption, AllowDevSecrets: true}
	cases := []struct {
		name   string
		secret string
	}{
		{"equals-signing-secret", signing},
		{"equals-encryption-key", encryption},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := log.New(&buf, "", 0)
			authn := auth.Authenticator{JWTSecret: tc.secret, AllowLocalHS256: true}
			exit := runCheckConfig(logger, cfg, runtimeconfig.SigningArchive{}, authn)
			if exit != 1 {
				t.Fatalf("exit=%d, want 1; log: %q", exit, buf.String())
			}
			out := buf.String()
			if !strings.Contains(out, "invalid authentication configuration") || !strings.Contains(out, "distinct from the signing and encryption secrets") {
				t.Fatalf("log must carry the distinctness marker, got: %q", out)
			}
			if strings.Contains(out, "check_config=ok") {
				t.Fatalf("check_config=ok must not be printed, got: %q", out)
			}
		})
	}
}

// checkConfigOKLine returns the check_config=ok line from a runCheckConfig
// log buffer, or "" when the preflight never reached the ok line.
func checkConfigOKLine(out string) string {
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "check_config=ok") {
			return line
		}
	}
	return ""
}

// TestPrepareServer covers the pre-server startup path: a valid
// authentication config builds the HTTP server with tracing disabled for an
// empty OTLP endpoint; a zero-value authenticator and a flag-only dev-auth
// config both fail fast with the startup error text (REQ-TLS-5, AC-3).
func TestPrepareServer(t *testing.T) {
	svc, err := service.New(nil, service.Config{SigningSecret: "test-secret", EncryptionKey: "test-key", AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	valid := auth.Authenticator{JWTSecret: testJWTSecret, AllowLocalHS256: true}
	t.Run("valid config builds server", func(t *testing.T) {
		var buf bytes.Buffer
		server, tracer, err := prepareServer(log.New(&buf, "", 0), svc, valid, "", ":0")
		if err != nil {
			t.Fatal(err)
		}
		if server == nil || server.Handler == nil {
			t.Fatal("server must be built")
		}
		if tracer != nil {
			t.Fatal("empty OTLP endpoint must not create a tracer")
		}
		if buf.Len() != 0 {
			t.Fatalf("no tracing log expected, got: %q", buf.String())
		}
	})
	t.Run("invalid auth config fails", func(t *testing.T) {
		_, _, err := prepareServer(log.New(io.Discard, "", 0), svc, auth.Authenticator{}, "", ":0")
		if err == nil || !strings.Contains(err.Error(), "invalid authentication configuration") {
			t.Fatalf("err=%v, want invalid authentication configuration", err)
		}
	})
	t.Run("dev auth flag-only fails", func(t *testing.T) {
		t.Setenv(envDevAuth, "")
		devAuth := auth.Authenticator{JWTSecret: testJWTSecret, AllowLocalHS256: true, AllowDev: true}
		_, _, err := prepareServer(log.New(io.Discard, "", 0), svc, devAuth, "", ":0")
		if err == nil || !strings.Contains(err.Error(), "AUDIT_ALLOW_DEV_AUTH") {
			t.Fatalf("err=%v, want the dev-auth allowlist error", err)
		}
	})
	t.Run("jwt secret shared with signing secret fails", func(t *testing.T) {
		// SEC-2 startup parity: the same distinctness check that fails
		// -check-config must fail prepareServer, so CI cannot bless a
		// config the runtime would reject.
		signing := strings.Repeat("s", 32)
		sharedSvc, err := service.New(nil, service.Config{SigningSecret: signing, EncryptionKey: strings.Repeat("k", 32), AllowDevSecrets: true})
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = prepareServer(log.New(io.Discard, "", 0), sharedSvc, auth.Authenticator{JWTSecret: signing, AllowLocalHS256: true}, "", ":0")
		if err == nil || !strings.Contains(err.Error(), "distinct from the signing and encryption secrets") {
			t.Fatalf("err=%v, want the distinctness error", err)
		}
	})
}

// TestLogSecretWarnings covers both warning branches of the well-known
// default secret logger.
func TestLogSecretWarnings(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	logSecretWarnings(logger, service.Config{SigningSecret: "development-signing-key-change-me", EncryptionKey: "development-encryption-key-change-me"})
	out := buf.String()
	if !strings.Contains(out, "warning=signing_secret=well-known-default") || !strings.Contains(out, "warning=encryption_key=well-known-default") {
		t.Fatalf("warnings missing for well-known defaults, got: %q", out)
	}
	buf.Reset()
	logSecretWarnings(logger, service.Config{SigningSecret: "strong-secret", EncryptionKey: "stronger-key"})
	if buf.Len() != 0 {
		t.Fatalf("no warnings expected for strong secrets, got: %q", buf.String())
	}
}

// TestWireExternal covers the startup wiring helper: valid Vault/S3 configs
// attach the signer/archive and log the provider; invalid configs fail fast
// with the same actionable text startup would emit (REQ-TLS-4/5).
func TestWireExternal(t *testing.T) {
	svc := func(t *testing.T) *service.Service {
		t.Helper()
		s, err := service.New(nil, service.Config{SigningSecret: "test-secret", EncryptionKey: "test-key", AllowDevSecrets: true})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	t.Run("vault https attaches signer", func(t *testing.T) {
		s := svc(t)
		var buf bytes.Buffer
		logger := log.New(&buf, "", 0)
		external := runtimeconfig.SigningArchive{VaultAddr: "https://vault.example.com:8200", VaultToken: "t", VaultTransitKey: "audit-checkpoints"}
		if err := wireExternal(logger, s, external, "audit-checkpoints", "https://vault.example.com:8200", "", ""); err != nil {
			t.Fatal(err)
		}
		if s.Config.Signer == nil {
			t.Fatal("signer must be attached")
		}
		if !strings.Contains(buf.String(), "signer=vault-transit key=audit-checkpoints addr=https://vault.example.com:8200") {
			t.Fatalf("log must name the signer provider, got: %q", buf.String())
		}
	})
	t.Run("s3 attaches store", func(t *testing.T) {
		s := svc(t)
		var buf bytes.Buffer
		logger := log.New(&buf, "", 0)
		external := runtimeconfig.SigningArchive{S3Endpoint: "s3.example.com:9000", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s", ArchiveRetentionDays: 365}
		if err := wireExternal(logger, s, external, "", "", "worm", "s3.example.com:9000"); err != nil {
			t.Fatal(err)
		}
		if _, ok := s.Config.Archive.(*archive.S3Store); !ok {
			t.Fatalf("archive type=%T, want *archive.S3Store", s.Config.Archive)
		}
		if !strings.Contains(buf.String(), "archive=s3 bucket=worm endpoint=s3.example.com:9000") {
			t.Fatalf("log must name the archive provider, got: %q", buf.String())
		}
	})
	t.Run("vault http non-loopback fails fast", func(t *testing.T) {
		s := svc(t)
		external := runtimeconfig.SigningArchive{VaultAddr: "http://vault.example.com:8200", VaultToken: "t", VaultTransitKey: "audit-checkpoints"}
		err := wireExternal(log.New(io.Discard, "", 0), s, external, "", "", "", "")
		if err == nil || !strings.Contains(err.Error(), "signer:") || !strings.Contains(err.Error(), runtimeconfig.EnvAllowInsecureVaultLoopback) {
			t.Fatalf("err=%v, want signer fail-fast naming the opt-in", err)
		}
	})
	t.Run("s3 https without flag fails fast", func(t *testing.T) {
		s := svc(t)
		external := runtimeconfig.SigningArchive{S3Endpoint: "https://s3.example.com", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s"}
		err := wireExternal(log.New(io.Discard, "", 0), s, external, "", "", "", "")
		if err == nil || !strings.Contains(err.Error(), "archive:") || !strings.Contains(err.Error(), runtimeconfig.EnvS3UseSSL) {
			t.Fatalf("err=%v, want archive fail-fast naming AUDIT_S3_USE_SSL", err)
		}
	})
	t.Run("no external config leaves defaults", func(t *testing.T) {
		s := svc(t)
		var buf bytes.Buffer
		if err := wireExternal(log.New(&buf, "", 0), s, runtimeconfig.SigningArchive{}, "", "", "", ""); err != nil {
			t.Fatal(err)
		}
		// service.New supplies the HMAC signer and FileStore defaults; the
		// wiring helper must leave them untouched when nothing is configured.
		if _, ok := s.Config.Archive.(*archive.FileStore); !ok {
			t.Fatalf("archive type=%T, want service default *archive.FileStore", s.Config.Archive)
		}
		if s.Config.Signer == nil {
			t.Fatal("signer must remain the service default")
		}
		if buf.Len() != 0 {
			t.Fatalf("no provider log expected, got: %q", buf.String())
		}
	})
}
