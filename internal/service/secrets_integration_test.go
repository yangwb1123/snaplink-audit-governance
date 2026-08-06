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
		if name == "AUDIT_SIGNING_SECRET" || name == "AUDIT_ENCRYPTION_KEY" || name == "AUDIT_ALLOW_DEV_SECRETS" {
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
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s -check-config with strong secrets failed: %v\n%s", name, err, out)
		}
		if !strings.Contains(string(out), "check_config=ok") {
			t.Fatalf("%s -check-config output missing check_config=ok:\n%s", name, out)
		}
	}
}

func TestCheckConfigDevModeOptIn(t *testing.T) {
	// Default flags (including -allow-dev-auth=true) plus no secrets must
	// still fail: the dev-secrets opt-in is independent (REQ-3).
	cmd := exec.Command(apiBin, "-check-config")
	cmd.Env = withoutSecretsEnv()
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("default dev-auth without dev-secrets must fail; output:\n%s", out)
	}
	// Explicit opt-in restores the legacy defaults with loud warnings.
	cmd = exec.Command(apiBin, "-check-config", "-allow-dev-secrets")
	cmd.Env = withoutSecretsEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("audit-api -check-config -allow-dev-secrets failed: %v\n%s", err, out)
	}
	for _, want := range []string{"warning=development_secrets_enabled", "warning=signing_secret=well-known-default", "check_config=ok"} {
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
