package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

// TestFileBackendLockRejectsSecondLiveWriter is T1 (AC-1): while one store
// holds the lifetime flock on <path>.lock, a second Open on the same path
// fails fast with errors.Is(err, ErrStateFileLocked), and the error names
// the absolute path plus the holder identity (pid and executable base name)
// recorded in the lock file. The control leg pins the sentinel's
// distinctness: a decode error on a corrupt state file is not the sentinel,
// and the failed open releases the lock (F7 — a corrected open succeeds).
func TestFileBackendLockRejectsSecondLiveWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })

	second, err := Open(path)
	if err == nil {
		_ = second.Close()
		t.Fatal("second Open on the same path must fail while the first holds the lock")
	}
	if !errors.Is(err, ErrStateFileLocked) {
		t.Fatalf("error %v must wrap ErrStateFileLocked", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, path) {
		t.Fatalf("error must name the absolute state path, got: %q", msg)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{fmt.Sprintf("pid %d", os.Getpid()), filepath.Base(exe), "stop the other audit-api or audit-governance-worker"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error must contain holder identity %q, got: %q", want, msg)
		}
	}
	// The lock file carries the holder identity (truncated on acquisition).
	raw, err := os.ReadFile(path + ".lock")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), fmt.Sprintf("%d\t%s\t", os.Getpid(), filepath.Base(exe))) {
		t.Fatalf("lock file must record holder identity, got: %q", raw)
	}

	t.Run("decode error is not the sentinel and releases the lock", func(t *testing.T) {
		badPath := filepath.Join(t.TempDir(), "corrupt.json")
		if err := os.WriteFile(badPath, []byte("{not json"), 0o640); err != nil {
			t.Fatal(err)
		}
		_, err := Open(badPath)
		if err == nil {
			t.Fatal("Open on a corrupt state file must fail")
		}
		if errors.Is(err, ErrStateFileLocked) {
			t.Fatalf("decode error %v must not be ErrStateFileLocked", err)
		}
		// F7: the failed open released the lock — a corrected open succeeds.
		if err := os.WriteFile(badPath, []byte("{}"), 0o640); err != nil {
			t.Fatal(err)
		}
		fixed, err := Open(badPath)
		if err != nil {
			t.Fatalf("Open after fixing the state file must succeed (lock leaked?): %v", err)
		}
		_ = fixed.Close()
	})
}

// TestFileBackendLockReleasedOnCloseIsT2 (AC-1/REQ-3): Close releases the
// lock exactly — a new Open succeeds and observes the snapshot the first
// store committed; committed data is never clobbered by the reopen.
func TestFileBackendLockReleasedOnClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Update(func(data *Snapshot) error {
		data.Tenants["demo"] = domain.Tenant{ID: "demo", Name: "Demo", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	// Close must have released the flock.
	second, err := Open(path)
	if err != nil {
		t.Fatalf("reopen after Close must succeed: %v", err)
	}
	defer second.Close()
	if err := second.Read(func(data *Snapshot) error {
		if _, ok := data.Tenants["demo"]; !ok {
			t.Fatal("snapshot committed by the first store is not visible after reopen")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestFileBackendLockNotAcquiredForOtherModes is T3 (REQ-1): in-memory
// (Open("")) and PostgreSQL stores acquire no lock and Close tolerates the
// absence; no file is created for the in-memory mode. (The -check-config
// subprocess leg lives in cmd/audit-governance-worker/main_test.go — it
// never opens the store and must exit 0 while the lock is held.)
func TestFileBackendLockNotAcquiredForOtherModes(t *testing.T) {
	t.Run("in-memory store has no lock and creates no file", func(t *testing.T) {
		dir := t.TempDir()
		wd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Chdir(dir); err != nil {
			t.Fatal(err)
		}
		defer os.Chdir(wd)
		st, err := Open("")
		if err != nil {
			t.Fatal(err)
		}
		backend := st.backend.(*fileBackend)
		if backend.lock != nil {
			t.Fatal("in-memory backend must not hold a lock")
		}
		if err := st.Close(); err != nil {
			t.Fatalf("Close on a lock-less backend must be a no-op: %v", err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("in-memory mode must create no files, found: %v", entries)
		}
	})

	t.Run("postgres store acquires no file lock", func(t *testing.T) {
		db := sql.OpenDB(noopConnector{})
		st, err := OpenPostgres(db)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := st.backend.(*postgresBackend); !ok {
			t.Fatalf("OpenPostgres must build a postgresBackend, got %T", st.backend)
		}
		if err := st.Close(); err != nil {
			t.Fatalf("Close must release the db pool: %v", err)
		}
	})
}

// TestFileBackendLockNoSilentClobber is T5 (AC-3): with store A open, store
// B's Open is refused (the "refused" arm of the acceptance — a lifetime
// flock replaces per-Save conflict errors), and any Update on A after B's
// failed open commits and re-reads: no write is ever silently dropped.
func TestFileBackendLockNoSilentClobber(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err := Open(path); !errors.Is(err, ErrStateFileLocked) {
		t.Fatalf("second writer must be refused with ErrStateFileLocked, got %v", err)
	}
	if err := a.Update(func(data *Snapshot) error {
		data.Tenants["t"] = domain.Tenant{ID: "t", Name: "T", Active: true}
		data.Events[EventKey("t", "e1")] = domain.Event{TenantID: "t", EventID: "e1"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.Read(func(data *Snapshot) error {
		if _, ok := data.Tenants["t"]; !ok || len(data.Events) != 1 {
			t.Fatal("write after the refused peer open was silently dropped")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestFileBackendLockFlockHolderSimulation is T6 (AC-3 mechanism pin): the
// test process itself takes the flock on <path>.lock (simulating a live peer
// whose state file does not yet exist — the parent-dir MkdirAll makes the
// lock file reachable deterministically), then Open fails with
// ErrStateFileLocked; releasing the flock makes Open succeed.
func TestFileBackendLockFlockHolderSimulation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("state file must not exist yet for the simulation")
	}
	lockFile, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	_, err = Open(path)
	if !errors.Is(err, ErrStateFileLocked) {
		t.Fatalf("Open with an externally held flock must fail with ErrStateFileLocked, got %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("failed Open must not create the state file")
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := lockFile.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path)
	if err != nil {
		t.Fatalf("Open after releasing the external flock must succeed: %v", err)
	}
	_ = st.Close()
}

// TestFileBackendLockCrashSemantics is T7 (REQ-1 lifetime/release): closing
// the underlying lock fd without Close() simulates process death — the
// kernel releases the flock, so a subsequent Open succeeds immediately and
// no stale-lock artifact survives (the lockReplayState property).
func TestFileBackendLockCrashSemantics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	lockFile, ok := st.backend.(*fileBackend).lock.(*os.File)
	if !ok {
		t.Fatal("file backend must hold an *os.File lock")
	}
	// Simulate process death: the fd closes without Store.Close, releasing
	// the kernel flock. The test never calls st.Close() afterwards.
	if err := lockFile.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("Open after the holder fd closed must succeed (stale lock?): %v", err)
	}
	_ = reopened.Close()
}

// --- sql/driver stubs for the OpenPostgres no-lock leg (T3) ---

type noopConn struct{}

func (noopConn) Prepare(query string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (noopConn) Close() error                              { return nil }
func (noopConn) Begin() (driver.Tx, error)                 { return nil, errors.New("unused") }

type noopDriver struct{}

func (noopDriver) Open(name string) (driver.Conn, error) { return noopConn{}, nil }

type noopConnector struct{}

func (noopConnector) Connect(context.Context) (driver.Conn, error) { return noopConn{}, nil }
func (noopConnector) Driver() driver.Driver                        { return noopDriver{} }
