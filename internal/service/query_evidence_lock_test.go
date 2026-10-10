package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type queryPausedArchive struct {
	archive.Store
	entered chan struct{}
	release chan struct{}
}

func (a *queryPausedArchive) Get(ctx context.Context, key string) ([]byte, error) {
	select {
	case a.entered <- struct{}{}:
	default:
	}
	select {
	case <-a.release:
		return a.Store.Get(ctx, key)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestQueryArchiveIOReleasesStoreLock(t *testing.T) {
	for _, surface := range []string{"query", "event"} {
		t.Run(surface, func(t *testing.T) {
			svc := testService(t, true)
			at := time.Unix(1700000010, 0).UTC()
			if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("lock-free-evidence", "op", at), domain.StatusArchived); err != nil {
				t.Fatal(err)
			}
			paused := &queryPausedArchive{Store: svc.Config.Archive, entered: make(chan struct{}, 1), release: make(chan struct{})}
			svc.Config.Archive = paused
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if surface == "event" {
					_, err := svc.GetEvent(ctx, "tenant-a", "reader", "lock-free-evidence")
					done <- err
				} else {
					_, err := svc.QueryEvents(ctx, "tenant-a", "reader", domain.Query{From: at.Add(-time.Second), To: at.Add(time.Second)})
					done <- err
				}
			}()
			select {
			case <-paused.entered:
			case <-ctx.Done():
				t.Fatal("archive boundary not entered")
			}
			// No preparation-time assumption: the signal proves Get owns the blocked
			// boundary. A concurrent control mutation must complete BEFORE release.
			updated := make(chan error, 1)
			go func() {
				updated <- svc.Store.UpdateControl(ctx, func(data *store.Snapshot) error {
					data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a"}
					return nil
				})
			}()
			select {
			case err := <-updated:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("archive IO retained Store.mu")
			}
			close(paused.release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(errors.New("query did not resume"))
			}
		})
	}
}
