package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestFileBackendPersistAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Update(func(data *Snapshot) error {
		data.Tenants["demo"] = domain.Tenant{ID: "demo", Name: "Demo", Active: true}
		data.AdminActions = append(data.AdminActions, domain.AdminAction{ID: "a1", Actor: "test", Action: "tenant.created", TargetType: "tenant", TargetID: "demo"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("state file not persisted: %v", err)
	}

	// 新实例必须从磁盘恢复。
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var tenant domain.Tenant
	if err := second.Read(func(data *Snapshot) error {
		var ok bool
		tenant, ok = data.Tenants["demo"]
		if !ok {
			t.Fatal("tenant missing after reload")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if tenant.Name != "Demo" {
		t.Fatalf("tenant name=%q", tenant.Name)
	}
	if len(second.MustSnapshot().AdminActions) != 1 {
		t.Fatal("admin actions not restored")
	}
}

func TestFileBackendUpdateRollbackOnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Update(func(data *Snapshot) error {
		data.Tenants["keep"] = domain.Tenant{ID: "keep", Name: "Keep", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// 闭包报错时，内存与磁盘都必须保持原状。
	err = st.Update(func(data *Snapshot) error {
		data.Tenants["ghost"] = domain.Tenant{ID: "ghost", Name: "Ghost", Active: true}
		return errors.New("boom")
	})
	if err == nil {
		t.Fatal("expected update error")
	}
	if err := st.Read(func(data *Snapshot) error {
		if _, ok := data.Tenants["ghost"]; ok {
			t.Fatal("failed update leaked into snapshot")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// 重开后 ghost 也不在磁盘上。
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Read(func(data *Snapshot) error {
		if _, ok := data.Tenants["ghost"]; ok {
			t.Fatal("failed update leaked to disk")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryStoreNeverPersists(t *testing.T) {
	st, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Update(func(data *Snapshot) error {
		data.Tenants["mem"] = domain.Tenant{ID: "mem", Name: "Mem", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	// 内存模式不写任何文件（路径为空时 Save 为 no-op）。
	reopened, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Read(func(data *Snapshot) error {
		if len(data.Tenants) != 0 {
			t.Fatal("memory store leaked between instances")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotNormalizeLegacyJSON(t *testing.T) {
	// 旧版本快照缺少新字段（admin_actions、restore_runs 等），读取后必须可用。
	legacy := `{"tenants":{"demo":{"id":"demo"}},"events":null,"admin_actions":null}`
	path := filepath.Join(t.TempDir(), "legacy.json")
	if err := os.WriteFile(path, []byte(legacy), 0o640); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Read(func(data *Snapshot) error {
		if data.AdminActions == nil || data.RestoreRuns == nil || data.Streams == nil {
			t.Fatal("normalize did not fill missing collections")
		}
		if _, ok := data.Tenants["demo"]; !ok {
			t.Fatal("legacy tenant lost")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCloneSnapshotIsDeep(t *testing.T) {
	data := NewSnapshot()
	data.Tenants["demo"] = domain.Tenant{ID: "demo", Name: "Demo", Active: true}
	clone, err := cloneSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	clone.Tenants["demo"] = domain.Tenant{ID: "demo", Name: "Mutated", Active: false}
	if data.Tenants["demo"].Name != "Demo" {
		t.Fatal("clone shares tenant map with source")
	}
}

func TestKeySeparatorRoundTrip(t *testing.T) {
	if TenantKey("t") != "t" {
		t.Fatal("tenant key must be identity")
	}
	for _, key := range []string{
		SourceKey("t", "s"),
		SchemaKey("t", "s", 1),
		EventKey("t", "e"),
		StreamKey("t", "stream"),
	} {
		if !strings.Contains(key, KeySeparator) {
			t.Fatalf("key %q missing separator", key)
		}
		if strings.Contains(key, "\x00") {
			t.Fatalf("key %q must not contain NUL (jsonb rejects \\u0000)", key)
		}
	}
}

func (s *Store) MustSnapshot() *Snapshot {
	snapshot, err := s.Snapshot()
	if err != nil {
		panic(err)
	}
	return snapshot
}
