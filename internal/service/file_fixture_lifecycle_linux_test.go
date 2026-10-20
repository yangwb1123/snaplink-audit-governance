package service

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Inspect only descriptors for this fixture's exact owned state path. Never
// read/log file contents, keys, identities, paths or other processes' handles.
// Keep the Service reachable after child cleanup so GC finalizers cannot hide
// missing explicit teardown of the real flock/trail descriptors.
func ownedFixtureDescriptors(t *testing.T, path string) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal("could not inspect the test process descriptor metadata")
	}
	count := 0
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue // descriptor closed while enumerating
		}
		if err != nil {
			t.Fatal("could not inspect owned descriptor metadata")
		}
		target = strings.TrimSuffix(target, " (deleted)")
		if target == path+".lock" || target == path+".admin-trail.jsonl" {
			count++
		}
	}
	return count
}

func TestReceiptReadClosedFileTrailNeverServesOrReopens(t *testing.T) {
	svc := testService(t, true)
	seedTrailEvent(t, svc, "closed-receipt-source")
	path := filepath.Join(filepath.Dir(svc.Config.ArchiveDir), "state.json")
	objects := &checkpointArchive{Store: svc.Config.Archive}
	svc.Config.Archive = objects
	original, err := svc.GetReceipt(testCtx, "tenant-a", "auditor-1", "closed-receipt-source")
	if err != nil || original.Status != domain.StatusArchived {
		t.Fatal("could not persist the original exact receipt read fact")
	}
	trailPath := path + ".admin-trail.jsonl"
	raw, err := os.ReadFile(trailPath)
	if err != nil {
		t.Fatal("could not capture the durable receipt attempt fact")
	}
	inode, err := os.Stat(trailPath)
	if err != nil {
		t.Fatal("could not capture the actual trail inode")
	}
	if err := svc.Store.Close(); err != nil {
		t.Fatal("could not close the original file writer")
	}
	next, err := store.Open(path)
	if err != nil {
		t.Fatal("successor could not acquire the actual file lock")
	}
	defer next.Close()
	got, err := svc.GetReceipt(testCtx, "tenant-a", "auditor-1", "closed-receipt-source")
	if !errors.Is(err, os.ErrClosed) || got != (domain.EventReceipt{}) {
		t.Error("closed read self-audit served a receipt or reopened the old trail")
	}
	afterRaw, readErr := os.ReadFile(trailPath)
	afterInode, statErr := os.Stat(trailPath)
	if readErr != nil || statErr != nil || !bytes.Equal(raw, afterRaw) || !os.SameFile(inode, afterInode) || ownedFixtureDescriptors(t, path) != 1 {
		t.Error("withdrawn old writer changed durable read facts or retained/reopened a descriptor")
	}
	fresh := newServiceOn(t, next)
	stored, err := fresh.GetReceipt(testCtx, "tenant-a", "", "closed-receipt-source")
	facts, trailErr := next.ReadAdminTrail(testCtx)
	if err != nil || stored != original || trailErr != nil || len(facts) != 1 || objects.gets.Load() != 0 || objects.puts.Load() != 0 {
		t.Error("close lost exact durable receipt metadata/facts or performed source I/O")
	}
}

func TestFileFixtureCleanupReleasesDescriptorsBeforeNextTest(t *testing.T) {
	for _, kind := range []string{"service", "admin-trail"} {
		t.Run(kind, func(t *testing.T) {
			var svc *Service
			var path string
			beforeClose := 0
			root := t.TempDir()
			t.Run("early-return", func(t *testing.T) {
				if kind == "service" {
					svc = testService(t, true)
					path = filepath.Join(filepath.Dir(svc.Config.ArchiveDir), "state.json")
				} else {
					svc, path = adminTrailTestService(t, root, 100, 100)
				}
				seedTrailEvent(t, svc, "fixture-close-source")
				if _, err := svc.GetEvent(testCtx, "tenant-a", "auditor-1", "fixture-close-source"); err != nil {
					t.Fatal("could not persist the actual actor-bearing read")
				}
				if got := ownedFixtureDescriptors(t, path); got != 2 {
					t.Fatal("oracle did not observe both the actual flock and persistent trail")
				}
				// Registered after construction: custom IO/background cleanups
				// must still run BEFORE the fixture's final Store.Close.
				t.Cleanup(func() { beforeClose = ownedFixtureDescriptors(t, path) })
				return // no explicit Close: exercise the fixture's cleanup path
			})
			if svc == nil || path == "" {
				t.Fatal("child fixture did not finish its original preparation")
			}
			t.Cleanup(func() { _ = svc.Store.Close() })
			afterClose := ownedFixtureDescriptors(t, path)
			t.Logf("file_fixture_boundary before_close=%d after_cleanup=%d", beforeClose, afterClose)
			if beforeClose != 2 || afterClose != 0 {
				t.Error("fixture cleanup left actual file descriptors across the child-test boundary")
			}
			if _, err := svc.Store.ReadAdminTrail(testCtx); !errors.Is(err, os.ErrClosed) {
				t.Error("finished fixture retained an active trail backend")
			}
			if kind == "admin-trail" {
				next, err := store.Open(path)
				if err != nil {
					t.Fatal("finished fixture retained the actual file flock")
				}
				defer next.Close()
				facts, err := next.ReadAdminTrail(testCtx)
				if err != nil || len(facts) != 1 {
					t.Error("fixture teardown lost or duplicated its durable read fact")
				}
			}
		})
	}
}
