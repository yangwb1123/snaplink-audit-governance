package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
)

// Test-only I/O preparation bound, unrelated to any production job budget.
const capacityArchiveWriters = 16

func capacityArchiveKey(index int) string {
	return fmt.Sprintf("events/tenant-a/source/%08d.json", index)
}

func prepareFileCapacityArchive(t *testing.T, dir string, count, payloadSize, writers int) int64 {
	t.Helper()
	objects := make([]archive.Store, writers)
	for i := range objects {
		objects[i] = &archive.FileStore{Dir: dir}
	}
	// Ready makes the shared root durable before independent handles write
	// disjoint keys. Every Put retains the production file/ancestor fsyncs.
	if err := objects[0].Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	payload := []byte(strings.Repeat("x", payloadSize))
	if err := prepareCapacityArchive(context.Background(), objects, count, payload); err != nil {
		t.Fatal(err)
	}
	verifyCapacityArchive(t, dir, count, payload)
	return directoryBytes(t, dir)
}

// The capacity oracle measures retained file bytes, not preparation latency.
// Each handle writes an exclusive partition using the real FileStore.Put; no
// file/ancestor sync is stubbed. Even a failed preparation joins every writer
// before its private directory can be measured or cleaned up.
func prepareCapacityArchive(parent context.Context, objects []archive.Store, count int, payload []byte) error {
	if len(objects) == 0 {
		return errors.New("capacity preparation has no archive writers")
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	failures := make(chan error, len(objects))
	var workers sync.WaitGroup
	for index, object := range objects {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := index; i < count; i += len(objects) {
				if ctx.Err() != nil {
					return
				}
				if err := object.Put(ctx, capacityArchiveKey(i), payload); err != nil {
					failures <- err
					cancel()
					return
				}
			}
		}()
	}
	workers.Wait()
	close(failures)
	var result error
	for err := range failures {
		result = errors.Join(result, err)
	}
	return errors.Join(result, ctx.Err())
}

func verifyCapacityArchive(t *testing.T, dir string, count int, payload []byte) {
	t.Helper()
	// A new production handle must read every original key from actual files;
	// neither successful Put counts nor aggregate sizes can hide a missing key.
	reopened := &archive.FileStore{Dir: dir}
	for i := 0; i < count; i++ {
		raw, err := reopened.Get(context.Background(), capacityArchiveKey(i))
		if err != nil || !bytes.Equal(raw, payload) {
			t.Fatal("capacity fixture did not retain every exact file object")
		}
	}
	if got := directoryBytes(t, dir); got != int64(count)*int64(len(payload)) {
		t.Fatal("capacity fixture contains missing or unexpected archive bytes")
	}
}

func TestFileHotColdCapacityPreparationPreservesMeasuredBytes(t *testing.T) {
	for _, size := range []int{4096, 8192} {
		root := t.TempDir()
		serial := measureFileHotColdSizeWithWriters(t, root, 20, size, 1)
		boundedArchive := prepareFileCapacityArchive(t, filepath.Join(root, "parallel-archive"), 20, size, capacityArchiveWriters)
		// Compare before/after sizes of the SAME durable metadata, not two
		// unrelated ledgers with variable-width wall-clock WrittenAt values.
		// Both archives are new real objects; neither is an idempotent retry.
		boundedState := directoryBytes(t, root) - serial.archive - boundedArchive
		if serial.state != boundedState || serial.archive != boundedArchive || boundedArchive != int64(20*size) {
			t.Fatal("bounded preparation changed actual retained state/archive measurements")
		}
	}
}

type capacityProbeArchive struct {
	archive.Store
	put func(context.Context, string, []byte) error
}

func (a *capacityProbeArchive) Put(ctx context.Context, key string, data []byte) error {
	return a.put(ctx, key, data)
}

func TestCapacityPreparationPartitionsEveryPutAndBoundsWriters(t *testing.T) {
	const writers, count = capacityArchiveWriters, 97
	payload := bytes.Repeat([]byte("x"), 4096)
	objects := make([]archive.Store, writers)
	var mu sync.Mutex
	active, peak := 0, 0
	calls, owners := map[string]int{}, map[string]int{}
	entered, release := make(chan struct{}, writers), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	for index := range objects {
		first := true // one serial partition per handle
		objects[index] = &capacityProbeArchive{put: func(ctx context.Context, key string, data []byte) error {
			if !bytes.Equal(data, payload) {
				return errors.New("capacity preparation changed object bytes")
			}
			mu.Lock()
			active++
			peak = max(peak, active)
			calls[key]++
			owners[key] = index
			mu.Unlock()
			defer func() { mu.Lock(); active--; mu.Unlock() }()
			if first {
				first = false
				entered <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}}
	}
	done, exited := make(chan error, 1), make(chan struct{})
	t.Cleanup(func() { unblock(); <-exited })
	go func() {
		defer close(exited)
		done <- prepareCapacityArchive(context.Background(), objects, count, payload)
	}()
	for range writers {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("capacity preparation did not reach each actual Put boundary")
		}
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal("capacity preparation failed")
	}
	mu.Lock()
	defer mu.Unlock()
	if peak != writers || active != 0 || len(calls) != count {
		t.Fatal("capacity preparation lost work, exceeded its writer bound or returned before joining")
	}
	for i := 0; i < count; i++ {
		key := capacityArchiveKey(i)
		if calls[key] != 1 || owners[key] != i%writers {
			t.Fatal("capacity preparation did not Put each original key exactly once in its exclusive partition")
		}
	}
}

func TestCapacityPreparationFailureCancelsAndJoinsInflightPut(t *testing.T) {
	injected := errors.New("injected capacity Put failure")
	entered := make(chan struct{}, 2)
	fail, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblockFailure := sync.OnceFunc(func() { close(fail) })
	unblockPut := sync.OnceFunc(func() { close(release) })
	objects := []archive.Store{
		&capacityProbeArchive{put: func(context.Context, string, []byte) error {
			entered <- struct{}{}
			<-fail
			return injected
		}},
		&capacityProbeArchive{put: func(ctx context.Context, _ string, _ []byte) error {
			entered <- struct{}{}
			<-ctx.Done()
			close(cancelled)
			<-release // model a Put that cannot immediately be interrupted
			return ctx.Err()
		}},
	}
	done, exited := make(chan error, 1), make(chan struct{})
	t.Cleanup(func() { unblockFailure(); unblockPut(); <-exited })
	go func() {
		defer close(exited)
		done <- prepareCapacityArchive(context.Background(), objects, 10, []byte("x"))
	}()
	for range objects {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("capacity failure test did not reach both actual Put boundaries")
		}
	}
	unblockFailure()
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("capacity failure did not cancel the other in-flight Put")
	}
	select {
	case <-done:
		t.Fatal("capacity preparation returned while a Put still owned its fixture")
	default:
	}
	unblockPut()
	if err := <-done; !errors.Is(err, injected) || !errors.Is(err, context.Canceled) {
		t.Fatal("capacity preparation hid an actual Put failure or cancellation")
	}
}
