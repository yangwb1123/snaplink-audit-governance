//go:build linux

package grpcapi

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestTLSPEMProjectedMountAndConfiguredParentSymlink(t *testing.T) {
	root := t.TempDir()
	mount := filepath.Join(root, "secret")
	if err := os.Mkdir(mount, 0o700); err != nil {
		t.Fatal(err)
	}
	version := filepath.Join(mount, "version-one")
	if err := os.Mkdir(version, 0o700); err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM := generateTestCertificate(t)
	for name, data := range map[string][]byte{"tls.crt": certPEM, "tls.key": keyPEM, "ca.crt": certPEM} {
		if err := os.WriteFile(filepath.Join(version, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join("..data", name), filepath.Join(mount, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("version-one", filepath.Join(mount, "..data")); err != nil {
		t.Fatal(err)
	}
	configured := filepath.Join(root, "configured-secret")
	if err := os.Symlink(mount, configured); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		cert, key, ca := filepath.Join(configured, "tls.crt"), filepath.Join(configured, "tls.key"), filepath.Join(configured, "ca.crt")
		if creds, err := ServerCredentials(cert, key); err != nil || creds == nil {
			t.Fatalf("projected server credentials: %v", err)
		}
		if creds, err := MutualTLSCredentials(cert, key, ca); err != nil || creds == nil {
			t.Fatalf("projected mTLS credentials: %v", err)
		}
	}
	check()
	// Real atomic ..data replacement, not a timing-based rotation assumption.
	second := filepath.Join(mount, "version-two")
	if err := os.Mkdir(second, 0o700); err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM = generateTestCertificate(t)
	for name, data := range map[string][]byte{"tls.crt": certPEM, "tls.key": keyPEM, "ca.crt": certPEM} {
		if err := os.WriteFile(filepath.Join(second, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("version-two", filepath.Join(mount, "next")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(mount, "next"), filepath.Join(mount, "..data")); err != nil {
		t.Fatal(err)
	}
	check()
}

func TestTLSPEMRejectsRootEscapesSpecialAndOversizedFiles(t *testing.T) {
	for _, kind := range []string{"relative-escape", "absolute-escape", "loop", "fifo", "directory", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			mount := filepath.Join(root, "mount")
			if err := os.Mkdir(mount, 0o700); err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(root, "private-outside")
			if err := os.WriteFile(outside, []byte("DO-NOT-LOG-PRIVATE-KEY"), 0o600); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(mount, "input")
			var err error
			switch kind {
			case "relative-escape":
				err = os.Symlink("../private-outside", path)
			case "absolute-escape":
				err = os.Symlink(outside, path)
			case "loop":
				err = os.Symlink("input", path)
			case "fifo":
				err = unix.Mkfifo(path, 0o600)
			case "directory":
				err = os.Mkdir(path, 0o700)
			case "oversized":
				err = os.WriteFile(path, bytes.Repeat([]byte("x"), maxTLSPEMBytes+1), 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			// Start the timeout guard only AFTER the real special-file boundary
			// exists. A FIFO must not block waiting for a writer at open/read.
			type readResult struct {
				raw []byte
				err error
			}
			result := make(chan readResult, 1)
			go func() {
				raw, err := readTLSPEMFile(path)
				result <- readResult{raw: raw, err: err}
			}()
			select {
			case got := <-result:
				if got.raw != nil || got.err == nil || strings.Contains(got.err.Error(), "DO-NOT-LOG") {
					t.Fatalf("unsafe PEM accepted or leaked: %v", got.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("TLS PEM special-file read blocked")
			}
		})
	}
}

func TestTLSPEMBudgetAppliesToCertKeyAndClientCA(t *testing.T) {
	cert, key := writeTestTLSFiles(t)
	big := filepath.Join(t.TempDir(), "too-big.pem")
	if err := os.WriteFile(big, bytes.Repeat([]byte("x"), maxTLSPEMBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, run := range []func() error{
		func() error { _, err := ServerCredentials(big, key); return err },
		func() error { _, err := ServerCredentials(cert, big); return err },
		func() error { _, err := MutualTLSCredentials(cert, key, big); return err },
	} {
		if err := run(); err == nil || !strings.Contains(err.Error(), "at most") {
			t.Fatalf("PEM size was not checked before parsing: %v", err)
		}
	}
	atLimit := filepath.Join(t.TempDir(), "exact-limit")
	if err := os.WriteFile(atLimit, bytes.Repeat([]byte("x"), maxTLSPEMBytes), 0o600); err != nil {
		t.Fatal(err)
	}
	if raw, err := readTLSPEMFile(atLimit); err != nil || len(raw) != maxTLSPEMBytes {
		t.Fatalf("exact read budget rejected: len=%d err=%v", len(raw), err)
	}
	for _, path := range []string{"", "nul\x00path"} {
		if _, err := readTLSPEMFile(path); err == nil {
			t.Fatal("invalid configured PEM path accepted")
		}
	}
}
