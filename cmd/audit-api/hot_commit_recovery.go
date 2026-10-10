package main

import (
	"context"
	"log"
	"sort"
	"time"

	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// One registered tenant per pass, regardless of publisher configuration.
// This resumes journaled hot->cold writes, not delivery or business execution.
func runHotCommitRecoveryLoop(ctx context.Context, svc *service.Service, logger *log.Logger) {
	if svc == nil || svc.Store == nil {
		return
	}
	last := ""
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		var tenants []string
		err := svc.Store.ReadControl(ctx, func(data *store.Snapshot) error {
			for id := range data.Tenants {
				tenants = append(tenants, id)
			}
			return nil
		})
		if err == nil && len(tenants) > 0 {
			sort.Strings(tenants)
			index := sort.Search(len(tenants), func(i int) bool { return tenants[i] > last })
			if index == len(tenants) {
				index = 0
			}
			last = tenants[index]
			recovered, recoverErr := svc.RecoverHotTenantCommit(ctx, last)
			if recoverErr != nil {
				logger.Printf("hot commit recovery failed")
			} else if recovered > 0 {
				logger.Printf("hot commit recovered_records=%d", recovered)
			}
		} else if err != nil {
			logger.Printf("hot commit recovery catalog unavailable")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
