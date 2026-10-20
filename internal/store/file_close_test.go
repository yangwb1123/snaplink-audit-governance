package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/fsutil"
)

// Observe the actual trail descriptor before releasing the actual flock. No
// synthetic close replaces either OS operation; the optional error is added
// only after the real lock descriptor has been closed.
type fileCloseLock struct {
	file      *os.File
	trail     *os.File
	calls     int
	trailOpen bool
	failure   error
}

func (l *fileCloseLock) Close() error {
	l.calls++
	_, err := l.trail.Stat()
	l.trailOpen = !errors.Is(err, os.ErrClosed)
	return errors.Join(l.file.Close(), l.failure)
}

func fileCloseFixture(t *testing.T, cap int) (*Store, *fileBackend, *os.File, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal("could not open owned file-close fixture")
	}
	f := st.backend.(*fileBackend)
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Update(testCtx, func(data *Snapshot) error {
		data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Owned", Active: true}
		return nil
	}); err != nil {
		t.Fatal("could not persist original control and tenant markers")
	}
	for _, fact := range trailFacts(3) {
		if err := st.AppendAdminFact(testCtx, fact, 100, cap); err != nil {
			t.Fatal("could not persist original read facts")
		}
	}
	trail := f.trailFile
	if trail == nil {
		t.Fatal("fixture did not reach the actual persistent trail descriptor")
	}
	// Also clean up safely when this regression runs against the old Close.
	t.Cleanup(func() { _ = trail.Close() })
	return st, f, trail, path
}

func TestFileCloseReleasesTrailBeforeLockAndPreservesDurableFiles(t *testing.T) {
	for _, cap := range []int{0, 2} {
		name := "append"
		if cap != 0 {
			name = "compacted"
		}
		t.Run(name, func(t *testing.T) {
			st, f, trail, path := fileCloseFixture(t, cap)
			facts, err := st.ReadAdminTrail(testCtx)
			if err != nil || (cap == 0 && len(facts) != 3) || (cap != 0 && len(facts) != 1) {
				t.Fatal("fixture did not retain the original append/compaction facts")
			}
			paths := []string{path, tenantFilePath(filepath.Dir(path), "tenant-a"), ledgerFilePath(filepath.Dir(path), "tenant-a"), path + trailPathSuffix, path + ".tmp"}
			if err := os.WriteFile(path+".tmp", []byte("owned-unknown-close-temp"), 0o600); err != nil {
				t.Fatal("could not prepare an owned unknown temporary file")
			}
			before := map[string][]byte{}
			inodes := map[string]os.FileInfo{}
			for _, p := range paths {
				before[p], err = os.ReadFile(p)
				if err != nil {
					t.Fatal("could not capture owned durable bytes")
				}
				inodes[p], err = os.Stat(p)
				if err != nil {
					t.Fatal("could not capture owned inode")
				}
			}
			lock := &fileCloseLock{file: f.lock.(*os.File), trail: trail}
			f.lock = lock
			if err := st.Close(); err != nil {
				t.Error("healthy close reported a failure")
			}
			if _, err := trail.Stat(); !errors.Is(err, os.ErrClosed) || lock.calls != 1 || lock.trailOpen {
				t.Error("close retained its actual trail descriptor or released flock before closing it")
			}
			if f.trailFile != nil {
				t.Error("closed backend retained the trail handle")
			}
			if err := st.Close(); err != nil || lock.calls != 1 {
				t.Error("repeated close performed another OS close")
			}
			for _, p := range paths {
				raw, readErr := os.ReadFile(p)
				info, statErr := os.Stat(p)
				if readErr != nil || statErr != nil || !bytes.Equal(raw, before[p]) || !os.SameFile(info, inodes[p]) {
					t.Error("close rewrote, removed or inferred an unknown durable file")
				}
			}
			next, err := Open(path)
			if err != nil {
				t.Fatal("close failed to release actual flock for a new writer")
			}
			defer next.Close()
			got, err := next.ReadAdminTrail(testCtx)
			if err != nil || !reflect.DeepEqual(got, facts) {
				t.Fatal("reopen lost or duplicated durable read facts")
			}
			if err := st.AppendAdminFact(testCtx, trailFact("late"), 100, cap); !errors.Is(err, os.ErrClosed) {
				t.Error("closed trail writer appended or reopened after releasing its flock")
			}
			if _, err := st.ReadAdminTrail(testCtx); !errors.Is(err, os.ErrClosed) {
				t.Error("closed trail read did not fail closed")
			}
			after, err := next.ReadAdminTrail(testCtx)
			if err != nil || !reflect.DeepEqual(after, facts) {
				t.Error("old writer changed the successor's actual trail")
			}
		})
	}
}

func TestFileCloseKeepsBothErrorsAndNeverRetriesDescriptors(t *testing.T) {
	for _, trailFailure := range []bool{false, true} {
		for _, lockFailure := range []bool{false, true} {
			name := "healthy-trail/healthy-lock"
			if trailFailure {
				name = "closed-trail/healthy-lock"
			}
			if lockFailure {
				name += "-fault"
			}
			t.Run(name, func(t *testing.T) {
				st, f, trail, path := fileCloseFixture(t, 0)
				injected := errors.New("owned lock close outcome")
				lock := &fileCloseLock{file: f.lock.(*os.File), trail: trail}
				if lockFailure {
					lock.failure = injected
				}
				f.lock = lock
				if trailFailure {
					if err := trail.Close(); err != nil {
						t.Fatal("could not establish the actual closed-descriptor fault")
					}
				}
				err := st.Close()
				if errors.Is(err, os.ErrClosed) != trailFailure || errors.Is(err, injected) != lockFailure || lock.calls != 1 || lock.trailOpen {
					t.Error("close hid a real trail/lock failure, skipped lock release or inverted close order")
				}
				if again := st.Close(); again != err || lock.calls != 1 {
					t.Error("repeated close lost its first error or retried a consumed descriptor")
				}
				next, openErr := Open(path)
				if openErr != nil {
					t.Fatal("a trail close failure prevented actual flock release")
				}
				defer next.Close()
				facts, readErr := next.ReadAdminTrail(testCtx)
				if readErr != nil || len(facts) != 3 {
					t.Error("failed close was treated as rollback or durable read-fact loss")
				}
				if err := st.AppendAdminFact(testCtx, trailFact("late"), 100, 0); !errors.Is(err, os.ErrClosed) {
					t.Error("unknown close outcome allowed the old trail to reopen")
				}
			})
		}
	}
}

func TestFileCloseWaitsForInFlightTrailAppendBeforeUnlocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal("could not open owned in-flight close fixture")
	}
	f := st.backend.(*fileBackend)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.syncDir = func(path string) error {
		once.Do(func() { close(entered); <-release })
		return fsutil.SyncDir(path) // retain the real directory durability chain
	}
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	appendDone, closeDone := make(chan error, 1), make(chan error, 1)
	appendExited, closeExited := make(chan struct{}), make(chan struct{})
	ownedExits := []<-chan struct{}{appendExited}
	ctx, cancel := context.WithTimeout(testCtx, 3*time.Second)
	defer cancel()
	t.Cleanup(func() {
		unblock()
		for _, exited := range ownedExits {
			select {
			case <-exited:
			case <-time.After(3 * time.Second):
				t.Error("owned file-close goroutine did not exit")
			}
		}
		_ = st.Close()
	})
	go func() {
		defer close(appendExited)
		appendDone <- st.AppendAdminFact(ctx, trailFact("in-flight"), 100, 0)
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("append did not reach actual trail creation/directory synchronization")
	}
	ownedExits = append(ownedExits, closeExited)
	go func() { defer close(closeExited); closeDone <- st.Close() }()
	var closeErr error
	closedEarly := false
	select {
	case closeErr = <-closeDone:
		closedEarly = true
		t.Error("close returned and released flock while the actual trail append was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	challenger, challengeErr := Open(path)
	if challengeErr == nil {
		_ = challenger.Close()
		t.Error("another actual file writer acquired flock before the in-flight append exited")
	} else if !errors.Is(challengeErr, ErrStateFileLocked) {
		t.Error("actual flock observation failed for an unrelated reason")
	}
	unblock()
	select {
	case err := <-appendDone:
		if err != nil {
			t.Error("close interrupted an append that owned the persistence boundary")
		}
	case <-ctx.Done():
		t.Fatal("actual append failed to exit after release")
	}
	if !closedEarly {
		select {
		case closeErr = <-closeDone:
		case <-ctx.Done():
			t.Fatal("close did not finish after actual append exited")
		}
	}
	if closeErr != nil {
		t.Error("healthy in-flight close failed")
	}
	next, err := Open(path)
	if err != nil {
		t.Fatal("completed close retained its actual flock")
	}
	defer next.Close()
	facts, err := next.ReadAdminTrail(testCtx)
	if err != nil || len(facts) != 1 || facts[0].ID != "in-flight" {
		t.Error("close lost the actual durable in-flight fact")
	}
}

func TestFileCloseConcurrentCallersConsumeDescriptorsOnce(t *testing.T) {
	st, f, trail, _ := fileCloseFixture(t, 0)
	injected := errors.New("owned concurrent lock close outcome")
	lock := &fileCloseLock{file: f.lock.(*os.File), trail: trail, failure: injected}
	f.lock = lock
	const callers = 8
	start, results := make(chan struct{}), make(chan error, callers)
	var writers sync.WaitGroup
	for range callers {
		writers.Go(func() { <-start; results <- st.Close() })
	}
	close(start)
	writers.Wait()
	close(results)
	var first error
	for err := range results {
		if first == nil {
			first = err
		}
		if !errors.Is(err, injected) || err != first {
			t.Error("concurrent close lost or changed the original close outcome")
		}
	}
	if lock.calls != 1 || lock.trailOpen {
		t.Error("concurrent callers retried a consumed descriptor or released lock before trail close")
	}
}

func TestFileClosePreservesUnknownCompactionOutcome(t *testing.T) {
	st, f, _, path := fileCloseFixture(t, 0)
	injected := errors.New("owned post-rename directory sync failure")
	f.syncDir = func(string) error { return injected }
	appendErr := st.AppendAdminFact(testCtx, trailFact("unknown"), 100, 2)
	if !errors.Is(appendErr, injected) || !errors.Is(appendErr, fsutil.ErrDataDirectorySync) {
		t.Fatal("oracle did not reach the actual renamed-but-unconfirmed compaction boundary")
	}
	raw, err := os.ReadFile(path + trailPathSuffix)
	if err != nil {
		t.Fatal("could not capture the actual unknown-outcome trail bytes")
	}
	inode, err := os.Stat(path + trailPathSuffix)
	if err != nil {
		t.Fatal("could not capture the actual unknown-outcome trail inode")
	}
	if err := st.Close(); err != nil {
		t.Error("healthy descriptor close failed after unknown append")
	}
	// A healthy Close is not confirmation or rollback of the failed append.
	next, err := Open(path)
	if err != nil {
		t.Fatal("unknown append prevented subsequent actual flock acquisition")
	}
	defer next.Close()
	afterRaw, readErr := os.ReadFile(path + trailPathSuffix)
	afterInode, statErr := os.Stat(path + trailPathSuffix)
	facts, trailErr := next.ReadAdminTrail(testCtx)
	if readErr != nil || statErr != nil || !bytes.Equal(raw, afterRaw) || !os.SameFile(inode, afterInode) || trailErr != nil || len(facts) != 1 || facts[0].ID != "unknown" {
		t.Error("close rewrote or rolled back the unknown but actually visible compaction fact")
	}
	if err := st.AppendAdminFact(testCtx, trailFact("late"), 100, 2); !errors.Is(err, os.ErrClosed) {
		t.Error("unknown append allowed a closed writer to reopen")
	}
}

func TestFileCloseMemoryAndNeverOpenedTrail(t *testing.T) {
	for _, path := range []string{"", filepath.Join(t.TempDir(), "state.json")} {
		st, err := Open(path)
		if err != nil {
			t.Fatal("could not open unused close fixture")
		}
		if err := st.Close(); err != nil {
			t.Error("unused close failed")
		}
		if err := st.Close(); err != nil {
			t.Error("unused close was not idempotent")
		}
		if err := st.AppendAdminFact(testCtx, trailFact("late"), 100, 0); !errors.Is(err, os.ErrClosed) {
			t.Error("closed unused backend accepted a new read fact")
		}
		if path != "" {
			if _, err := os.Stat(path + trailPathSuffix); !errors.Is(err, os.ErrNotExist) {
				t.Error("closing an unused trail created new evidence")
			}
		}
	}
}
