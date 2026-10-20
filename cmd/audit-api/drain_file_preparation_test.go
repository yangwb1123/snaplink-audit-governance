package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/grpcapi"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const drainChildSigningSecret = "test-signing-secret-0123456789abcdefgh"
const drainChildEncryptionKey = "test-encryption-key-0123456789abcdefg"

func drainFileConfig(dir string) service.Config {
	return service.Config{ArchiveDir: dir, SigningSecret: drainChildSigningSecret, EncryptionKey: drainChildEncryptionKey}
}

// Complete the original authenticated new Write and real file/archive commits
// before launching the process whose 1s store budget is also its drain budget.
// The measured child still handles the exact request over real gRPC, but as an
// authenticated duplicate. Neither the live stream nor SIGTERM is simulated.
func prepareDrainFileState(statePath string, cfg service.Config, request *auditv1.WriteRequest) (_ *auditv1.WriteResponse, err error) {
	st, err := store.Open(statePath)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, st.Close()) }()
	svc, err := service.New(st, cfg)
	if err != nil {
		return nil, err
	}
	if err := bootstrap(testCtx, svc, "demo"); err != nil {
		return nil, err
	}
	if err := svc.Config.Archive.Ready(testCtx); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer()
	grpcapi.Register(server, svc, auth.Authenticator{AllowDev: true})
	served := make(chan struct{})
	go func() { defer close(served); _ = server.Serve(listener) }()
	defer func() { server.Stop(); <-served; _ = listener.Close() }()
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	// Retain the original RPC preparation guard, after all non-RPC setup.
	// In particular, do not give the future idle stream this expiry.
	ctx, cancel := context.WithTimeout(devAuthContext(), 30*time.Second)
	defer cancel()
	receipt, release, err := prepareDrainIngest(ctx, auditv1.NewIngestClient(connection), request, false)
	if err != nil {
		return nil, err
	}
	defer release()
	if receipt.GetEventId() != request.GetEvent().GetEventId() || receipt.GetTenantId() != "demo" || receipt.GetSequence() != 1 || receipt.GetHash() == "" || receipt.GetStatus() != domain.StatusArchived {
		return nil, errors.New("drain file preparation did not finish the original archived commit")
	}
	return receipt, nil
}

type drainFilePausedArchive struct {
	archive.Store
	entered chan struct{}
	release chan struct{}
}

func (a *drainFilePausedArchive) Put(ctx context.Context, key string, raw []byte) error {
	select {
	case a.entered <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-a.release:
		return a.Store.Put(ctx, key, raw)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestDrainFilePreparationWaitsForRealPutAndReleasesLock(t *testing.T) {
	root := t.TempDir()
	statePath, archiveDir := filepath.Join(root, "state.json"), filepath.Join(root, "archive")
	objects := &archive.FileStore{Dir: archiveDir}
	paused := &drainFilePausedArchive{Store: objects, entered: make(chan struct{}, 1), release: make(chan struct{})}
	cfg := drainFileConfig(archiveDir)
	cfg.Archive = paused
	request := &auditv1.WriteRequest{Event: drainTestEvent("drain-file-original"), WaitFor: domain.StatusLedgered}
	type outcome struct {
		receipt *auditv1.WriteResponse
		err     error
	}
	done, exited := make(chan outcome, 1), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(paused.release) })
	t.Cleanup(func() { unblock(); <-exited })
	go func() {
		defer close(exited)
		receipt, err := prepareDrainFileState(statePath, cfg, request)
		done <- outcome{receipt, err}
	}()
	select {
	case <-paused.entered:
	case result := <-done:
		t.Fatalf("drain preparation exited before real Put: %v", result.err)
	case <-time.After(30 * time.Second):
		t.Fatal("drain preparation never reached its real file Put")
	}
	select {
	case <-done:
		t.Fatal("drain preparation declared readiness before its real Put completed")
	default:
	}
	if probe, err := store.Open(statePath); err == nil {
		_ = probe.Close()
		t.Fatal("preparation released the file lock while its commit was still in flight")
	} else if !errors.Is(err, store.ErrStateFileLocked) {
		t.Fatal("preparation lock probe failed for an unrelated reason")
	}
	unblock()
	result := <-done
	if result.err != nil || result.receipt.GetDuplicate() {
		t.Fatal("drain preparation did not perform the original authenticated new Write")
	}
	st, err := store.OpenWithOptions(statePath, store.Options{OperationTimeout: childStoreOperationTimeout})
	if err != nil {
		t.Fatal("completed preparation did not release the real state lock")
	}
	defer st.Close()
	if err := st.Read(testCtx, func(data *store.Snapshot) error {
		key := store.EventKey("demo", request.Event.EventId)
		receipt := data.Receipts[key]
		if len(data.Receipts) != 1 || len(data.Events) != 0 || receipt.Status != domain.StatusArchived || receipt.Hash != result.receipt.GetHash() || receipt.Sequence != 1 || receipt.Duplicate || receipt.Conflict {
			return errors.New("preparation did not retain exactly one original committed receipt")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := service.New(st, drainFileConfig(archiveDir))
	if err != nil {
		t.Fatal(err)
	}
	event, err := reader.GetEvent(testCtx, "demo", "", request.Event.EventId)
	if err != nil || event.Hash != result.receipt.GetHash() || !event.OccurredAt.Equal(request.Event.OccurredAt.AsTime()) {
		t.Fatal("preparation did not preserve authenticated original file evidence")
	}
	if fmt.Sprint(event.Payload["value"]) != "1" {
		t.Fatal("preparation changed the original archived content")
	}
}
