package service

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// secretsBinaries are built once per test run from the module root, so the
// AC-2 integration checks exercise the real command wiring (flags, env
// names, exit codes) without any external store or network.
var (
	apiBin    string
	workerBin string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "snaplink-bins-")
	if err != nil {
		panic(err)
	}
	apiBin, err = buildBinary("audit-api", "./cmd/audit-api", dir)
	if err != nil {
		panic(err)
	}
	workerBin, err = buildBinary("audit-governance-worker", "./cmd/audit-governance-worker", dir)
	if err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func buildBinary(name, pkg, dir string) (string, error) {
	target := filepath.Join(dir, name)
	cmd := exec.Command("go", "build", "-o", target, pkg)
	cmd.Dir = filepath.Join("..", "..")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("go build %s: %w\n%s", pkg, err, out)
	}
	return target, nil
}

// withoutSecretsEnv strips the secret env vars so the child processes start
// from the same clean state regardless of the outer environment.
func withoutSecretsEnv() []string {
	var env []string
	for _, entry := range os.Environ() {
		name := strings.SplitN(entry, "=", 2)[0]
		if name == "AUDIT_SIGNING_SECRET" || name == "AUDIT_ENCRYPTION_KEY" || name == "AUDIT_ALLOW_DEV_SECRETS" || name == "AUDIT_ALLOW_DEV_AUTH" {
			continue
		}
		env = append(env, entry)
	}
	return env
}

func withStrongSecretsEnv() []string {
	return append(withoutSecretsEnv(),
		"AUDIT_SIGNING_SECRET=signing-"+strings.Repeat("s", 40),
		"AUDIT_ENCRYPTION_KEY=encryption-"+strings.Repeat("e", 40))
}

func TestAuditAPIFailsFastWithoutSecrets(t *testing.T) {
	cmd := exec.Command(apiBin, "-check-config")
	cmd.Env = withoutSecretsEnv()
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("audit-api -check-config without secrets must exit non-zero; output:\n%s", out)
	}
	if !strings.Contains(string(out), "AUDIT_SIGNING_SECRET") {
		t.Fatalf("startup failure must name AUDIT_SIGNING_SECRET; output:\n%s", out)
	}
	if !strings.Contains(string(out), "AUDIT_ALLOW_DEV_SECRETS") {
		t.Fatalf("startup failure must mention the dev opt-in; output:\n%s", out)
	}
}

func TestWorkerFailsFastWithoutSecrets(t *testing.T) {
	cmd := exec.Command(workerBin, "-check-config")
	cmd.Env = withoutSecretsEnv()
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("worker -check-config without secrets must exit non-zero; output:\n%s", out)
	}
	if !strings.Contains(string(out), "AUDIT_SIGNING_SECRET") {
		t.Fatalf("startup failure must name AUDIT_SIGNING_SECRET; output:\n%s", out)
	}
}

func TestCheckConfigAcceptsStrongSecrets(t *testing.T) {
	for name, bin := range map[string]string{"audit-api": apiBin, "worker": workerBin} {
		cmd := exec.Command(bin, "-check-config")
		cmd.Env = withStrongSecretsEnv()
		if name == "audit-api" {
			// The preflight validates auth config; the env allowlist is the
			// only way a dev-auth-enabled config passes (AC-3).
			cmd.Env = append(cmd.Env, "AUDIT_ALLOW_DEV_AUTH=true")
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s -check-config with strong secrets failed: %v\n%s", name, err, out)
		}
		if !strings.Contains(string(out), "check_config=ok") {
			t.Fatalf("%s -check-config output missing check_config=ok:\n%s", name, out)
		}
		if name == "audit-api" && !strings.Contains(string(out), "warning=development_auth_enabled") {
			t.Fatalf("allowlisted audit-api preflight must show the dev-auth warning:\n%s", out)
		}
	}
}

func TestCheckConfigDevModeOptIn(t *testing.T) {
	// Default flags (allow-dev-auth now defaults to false) plus no secrets
	// must still fail: the dev-secrets opt-in is independent (REQ-3).
	cmd := exec.Command(apiBin, "-check-config")
	cmd.Env = withoutSecretsEnv()
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("default dev-auth without dev-secrets must fail; output:\n%s", out)
	}
	// Explicit opt-in restores the legacy defaults with loud warnings.
	cmd = exec.Command(apiBin, "-check-config", "-allow-dev-secrets")
	cmd.Env = append(withoutSecretsEnv(), "AUDIT_ALLOW_DEV_AUTH=true")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("audit-api -check-config -allow-dev-secrets failed: %v\n%s", err, out)
	}
	for _, want := range []string{"warning=development_secrets_enabled", "warning=signing_secret=well-known-default", "warning=development_auth_enabled", "check_config=ok"} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}

func TestCheckConfigPrintsNoSecretValues(t *testing.T) {
	signing := "signing-very-secret-value-12345"
	encryption := "encryption-very-secret-value-67890"
	for name, bin := range map[string]string{"audit-api": apiBin, "worker": workerBin} {
		cmd := exec.Command(bin, "-check-config")
		cmd.Env = append(withoutSecretsEnv(), "AUDIT_SIGNING_SECRET="+signing, "AUDIT_ENCRYPTION_KEY="+encryption)
		if name == "audit-api" {
			cmd.Env = append(cmd.Env, "AUDIT_ALLOW_DEV_AUTH=true")
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s -check-config failed: %v\n%s", name, err, out)
		}
		text := string(out)
		if strings.Contains(text, signing) || strings.Contains(text, encryption) {
			t.Fatalf("%s -check-config output must never contain secret values:\n%s", name, text)
		}
		if !strings.Contains(text, "signing_secret_length=") || !strings.Contains(text, "encryption_key_length=") {
			t.Fatalf("%s -check-config output must report lengths only:\n%s", name, text)
		}
	}
}

// TestStrictBoolEnvRejectsMalformedValues pins AC-2: a malformed value for
// any of the four audit-api boolean env vars exits non-zero naming the
// variable, before flag.Parse, so there is no fail-open path (C2).
func TestStrictBoolEnvRejectsMalformedValues(t *testing.T) {
	for _, env := range []string{"AUDIT_ALLOW_LOCAL_HS256", "AUDIT_ALLOW_INSECURE_JWKS_LOOPBACK",
		"AUDIT_ALLOW_DEV_AUTH", "AUDIT_ALLOW_DEV_SECRETS"} {
		childEnv := withStrongSecretsEnv()
		if env != "AUDIT_ALLOW_DEV_AUTH" {
			// Keep the failure attributable to the malformed variable, not
			// to a missing dev-auth allowlist.
			childEnv = append(childEnv, "AUDIT_ALLOW_DEV_AUTH=true")
		}
		childEnv = append(childEnv, env+"=not-a-bool")
		cmd := exec.Command(apiBin, "-check-config")
		cmd.Env = childEnv
		out, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(out), env) {
			t.Fatalf("%s malformed: err=%v out=%q, want non-zero exit naming %s", env, err, out, env)
		}
	}
}

// TestStrictBoolEnvParseBoolLiteralsAndEmpty pins D1: the allowlist uses the
// same strict strconv.ParseBool path as the runtime flag default, so the
// truthy literals 1/TRUE/t behave identically to true; a present-but-empty
// value falls back to the (false) default instead of failing the parse and
// is then rejected by the preflight via the missing-trust-source path (F7).
func TestStrictBoolEnvParseBoolLiteralsAndEmpty(t *testing.T) {
	for _, literal := range []string{"1", "TRUE", "t"} {
		cmd := exec.Command(apiBin, "-check-config")
		cmd.Env = append(withStrongSecretsEnv(), "AUDIT_ALLOW_DEV_AUTH="+literal)
		out, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), "warning=development_auth_enabled") || !strings.Contains(string(out), "check_config=ok") {
			t.Fatalf("AUDIT_ALLOW_DEV_AUTH=%s must parse as true: err=%v out=%q", literal, err, out)
		}
	}
	cmd := exec.Command(apiBin, "-check-config")
	cmd.Env = append(withStrongSecretsEnv(), "AUDIT_ALLOW_DEV_AUTH=")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("present-but-empty must fall back to false and fail the zero-auth preflight; output:\n%s", out)
	}
	if strings.Contains(string(out), "not a valid boolean") {
		t.Fatalf("present-but-empty must not produce the parse-failure message; output:\n%s", out)
	}
	if !strings.Contains(string(out), "no JWT verification trust source is configured") {
		t.Fatalf("present-but-empty must fail via the missing-trust-source path; output:\n%s", out)
	}
}

// TestPreflightPrecedenceCells pins QA-3: the check-config dev-auth gate is
// an environment-only allowlist. The flag can never satisfy it (AC-3 key
// pin), so every cell with runtime dev auth enabled and no env allowlist
// exits non-zero, and the only passing cells have the env allowlist present.
func TestPreflightPrecedenceCells(t *testing.T) {
	const jwtSecret = "jwt-secret-0123456789abcdef" // >16 bytes, satisfies the service guard
	cells := []struct {
		name         string
		flags        []string
		env          map[string]string
		trustSource  bool
		wantExit     int
		wantContains []string
	}{
		{name: "defaults-no-trust-source", wantExit: 1,
			wantContains: []string{"no JWT verification trust source is configured"}},
		{name: "env-allowlisted-no-trust-source", env: map[string]string{"AUDIT_ALLOW_DEV_AUTH": "true"}, wantExit: 0,
			wantContains: []string{"warning=development_auth_enabled", "check_config=ok"}},
		{name: "flag-only-no-trust-source", flags: []string{"-allow-dev-auth=true"}, wantExit: 1,
			wantContains: []string{"auth=dev_auth_flag_not_allowlisted"}},
		{name: "flag-vs-env-false-no-trust-source", flags: []string{"-allow-dev-auth=true"}, env: map[string]string{"AUDIT_ALLOW_DEV_AUTH": "false"}, wantExit: 1,
			wantContains: []string{"auth=dev_auth_flag_not_allowlisted"}},
		{name: "flag-off-env-true-no-trust-source", flags: []string{"-allow-dev-auth=false"}, env: map[string]string{"AUDIT_ALLOW_DEV_AUTH": "true"}, wantExit: 1,
			wantContains: []string{"no JWT verification trust source is configured"}},
		{name: "flag-only-with-trust-source", flags: []string{"-allow-dev-auth=true"}, trustSource: true, wantExit: 1,
			wantContains: []string{"auth=dev_auth_flag_not_allowlisted"}},
		{name: "env-allowlisted-with-trust-source", env: map[string]string{"AUDIT_ALLOW_DEV_AUTH": "true"}, trustSource: true, wantExit: 0,
			wantContains: []string{"warning=development_auth_enabled", "check_config=ok"}},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			childEnv := withStrongSecretsEnv()
			for key, value := range cell.env {
				childEnv = append(childEnv, key+"="+value)
			}
			if cell.trustSource {
				childEnv = append(childEnv, "AUDIT_JWT_SECRET="+jwtSecret, "AUDIT_ALLOW_LOCAL_HS256=true")
			}
			cmd := exec.Command(apiBin, append([]string{"-check-config"}, cell.flags...)...)
			cmd.Env = childEnv
			out, err := cmd.CombinedOutput()
			gotExit := 0
			if err != nil {
				gotExit = cmd.ProcessState.ExitCode()
			}
			if gotExit != cell.wantExit {
				t.Fatalf("exit = %d, want %d; output:\n%s", gotExit, cell.wantExit, out)
			}
			for _, want := range cell.wantContains {
				if !strings.Contains(string(out), want) {
					t.Fatalf("output missing %q:\n%s", want, out)
				}
			}
		})
	}
}

// TestStartupFailsClosedWithoutTrustSource pins AC-4: a bare startup with
// dev auth at its new default (false) and no JWT trust source fails with the
// byte-exact preserved string and without the dev-auth warning.
func TestStartupFailsClosedWithoutTrustSource(t *testing.T) {
	cmd := exec.Command(apiBin, "-listen", "127.0.0.1:0",
		"-state", filepath.Join(t.TempDir(), "state.json"),
		"-archive", t.TempDir(), "-bootstrap-tenant", "qa")
	cmd.Env = append(withoutSecretsEnv(), "AUDIT_ALLOW_DEV_SECRETS=true")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("bare startup must fail closed without a trust source; output:\n%s", out)
	}
	if !strings.Contains(string(out), "no JWT verification trust source is configured") {
		t.Fatalf("startup must fail with the preserved string; output:\n%s", out)
	}
	if strings.Contains(string(out), "warning=development_auth_enabled") {
		t.Fatalf("fail-closed startup must not show the dev-auth warning; output:\n%s", out)
	}
}
