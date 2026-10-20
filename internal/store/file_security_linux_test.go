//go:build linux

package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/fsutil"
)

func TestFileStoreRejectsSymlinkedStateLockAndLayout(t *testing.T) {
	for _, target := range []string{"state", "lock", "tenants", "ledger"} {
		t.Run(target, func(t *testing.T) {
			root, outside := t.TempDir(), t.TempDir()
			path := filepath.Join(root, "state.json")
			if target == "tenants" || target == "ledger" {
				st, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := st.Update(context.Background(), func(data *Snapshot) error {
					data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Active: true}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
				original := filepath.Join(root, target)
				if err := os.Rename(original, original+"-preserved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, original); err != nil {
					t.Fatal(err)
				}
			} else {
				victim := filepath.Join(outside, "private")
				if err := os.WriteFile(victim, []byte("do-not-touch"), 0o600); err != nil {
					t.Fatal(err)
				}
				link := path
				if target == "lock" {
					link += ".lock"
				}
				if err := os.Symlink(victim, link); err != nil {
					t.Fatal(err)
				}
			}
			if st, err := Open(path); !errors.Is(err, fsutil.ErrUnsafeFile) {
				if st != nil {
					_ = st.Close()
				}
				t.Fatalf("unsafe %s layout did not fail closed: %v", target, err)
			}
			if target == "state" || target == "lock" {
				raw, err := os.ReadFile(filepath.Join(outside, "private"))
				if err != nil || string(raw) != "do-not-touch" {
					t.Fatalf("modified outside file: %q %v", raw, err)
				}
			}
		})
	}
}

func TestFileSnapshotRefusesUnknownTempWithoutPublishing(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "state.json")
	backend, err := openFileBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	seed := NewSnapshot()
	seed.Tenants["keep"] = domain.Tenant{ID: "keep"}
	if err := backend.Save(context.Background(), seed); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".tmp", []byte("unknown-old-write-plan"), 0o600); err != nil {
		t.Fatal(err)
	}
	changed := NewSnapshot()
	changed.Tenants["wrong"] = domain.Tenant{ID: "wrong"}
	if err := backend.Save(context.Background(), changed); err == nil {
		t.Fatal("unknown temp was overwritten")
	}
	memory, err := backend.Load(context.Background())
	if err != nil || memory.Tenants["keep"].ID != "keep" || memory.Tenants["wrong"].ID != "" {
		t.Fatalf("failed save changed authority: %+v %v", memory, err)
	}
	raw, err := os.ReadFile(path + ".tmp")
	if err != nil || string(raw) != "unknown-old-write-plan" {
		t.Fatalf("unknown evidence removed/truncated: %q %v", raw, err)
	}
}

func TestFileLayoutCreatesOwnerOnlyFilesWithoutChmoddingLegacyLedger(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "private", "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Update(ctx, func(data *Snapshot) error {
		data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendAdminFact(ctx, domain.AdminAction{ID: "fact", TenantID: "tenant-a", Actor: "tester", Action: "read", CreatedAt: time.Now()}, 100, 100); err != nil {
		t.Fatal(err)
	}
	event := publicationTestEvent("tenant-a", "event")
	if err := st.ConfirmPublication(ctx, event); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	q := snapshotFixture(1, now)
	if err := st.SaveQuerySnapshot(ctx, q, now); err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(path)
	for _, file := range []string{path, path + ".lock", path + trailPathSuffix, tenantFilePath(root, "tenant-a"), ledgerFilePath(root, "tenant-a"), st.publicationPath(publicationFromReceipt(publicationTestReceipt(event))), st.querySnapshotPath("tenant-a", q.ID)} {
		info, err := os.Stat(file)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("new file not owner-only: %s %v %v", file, info, err)
		}
	}
	ledger := ledgerFilePath(root, "tenant-a")
	if err := os.Chmod(ledger, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := readLedgerFile(root, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(ledger)
	if info.Mode().Perm() != 0o640 {
		t.Fatal("legacy ledger was automatically chmod'd")
	}
}

func TestQuerySnapshotSymlinkCannotReadOrCollectOutsideMetadata(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC()
	q := snapshotFixture(1, now)
	if err := st.SaveQuerySnapshot(ctx, q, now); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(st.querySnapshotPath(q.TenantID, q.ID))
	if err := os.Rename(dir, dir+"-preserved"); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	raw, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(outside, q.ID+".json")
	if err := os.WriteFile(victim, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ReadQuerySnapshot(ctx, q.TenantID, q.ID, now); !errors.Is(err, fsutil.ErrUnsafeFile) {
		t.Fatalf("symlink read did not fail closed: %v", err)
	}
	later := q.ExpiresAt.Add(time.Second)
	if err := st.SaveQuerySnapshot(ctx, snapshotFixture(2, later), later); !errors.Is(err, fsutil.ErrUnsafeFile) {
		t.Fatalf("GC followed an outside directory: %v", err)
	}
	preserved, err := os.ReadFile(victim)
	if err != nil || string(preserved) != string(raw) {
		t.Fatalf("metadata GC touched another directory: %q %v", preserved, err)
	}
}
