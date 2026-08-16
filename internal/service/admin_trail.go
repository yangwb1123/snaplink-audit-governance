package service

import (
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// DefaultMaxAdminActions caps Snapshot.AdminActions (mutation-path facts):
// drop-oldest, newest retained. The persisted snapshot therefore never grows
// past the cap regardless of how many control-plane mutations occur.
const DefaultMaxAdminActions = 10000

// DefaultMaxAdminTrailActions caps the separate read self-audit trail. Once
// the bound is exceeded the trail is compacted to its newest
// DefaultMaxAdminTrailActions facts (amortized O(1) per append on Postgres;
// the file backend rewrites the capped JSONL). 0 = unbounded append-only,
// an operator-explicit choice.
const DefaultMaxAdminTrailActions = 100000

// resolveAdminTrailConfig resolves and validates the admin-trail caps with
// distinct zero semantics (design F-6): MaxAdminActions <= 0 selects
// DefaultMaxAdminActions; MaxAdminTrailActions < 0 selects
// DefaultMaxAdminTrailActions while == 0 means unbounded (operator-explicit).
// A non-zero trail cap below the snapshot cap is a misconfiguration
// (ErrInvalid): the trail exists to be the deep read history, so it must be
// at least as deep as the mutation facts it complements. (The design's draft
// `>= 100` floor is dropped: it contradicts the acceptance fixtures that
// deliberately use small caps, e.g. MaxAdminActions=5; resolution to a
// positive default already rejects 0/negative operator input.)
func resolveAdminTrailConfig(cfg Config) (Config, error) {
	if cfg.MaxAdminActions <= 0 {
		cfg.MaxAdminActions = DefaultMaxAdminActions
	}
	if cfg.MaxAdminTrailActions < 0 {
		cfg.MaxAdminTrailActions = DefaultMaxAdminTrailActions
	}
	if cfg.MaxAdminTrailActions > 0 && cfg.MaxAdminTrailActions < cfg.MaxAdminActions {
		return cfg, fmt.Errorf("%w: MaxAdminTrailActions (%d) must be 0 (unbounded) or at least MaxAdminActions (%d)", domain.ErrInvalid, cfg.MaxAdminTrailActions, cfg.MaxAdminActions)
	}
	return cfg, nil
}

// appendAdminAction appends one mutation-path admin fact inside the caller's
// Store.Update closure (the fact commits atomically with the audited
// mutation) and enforces MaxAdminActions: once the snapshot slice exceeds
// the cap the oldest facts are dropped (O(1) slice re-header, newest
// retained), so the persisted document always satisfies the bound.
func (s *Service) appendAdminAction(data *store.Snapshot, action domain.AdminAction) {
	data.AdminActions = append(data.AdminActions, action)
	capN := s.Config.MaxAdminActions
	if capN > 0 && len(data.AdminActions) > capN {
		data.AdminActions = data.AdminActions[len(data.AdminActions)-capN:]
	}
}

// mergeAdminActions merges the snapshot mutation facts with the read
// self-audit trail into one newest-first view. The tenant/platform filter
// applies BEFORE the limit clamp, preserving today's filter-before-cap
// semantics. Order: newest-first by CreatedAt desc; equal CreatedAt ⇒ trail
// first, then reverse-append within each source (a later append wins ties —
// design D-4; no in-repo pin constrains cross-source merged ordering,
// verified F-8). limit is clamped to 1..100 by the caller.
func mergeAdminActions(snapshot, trail []domain.AdminAction, tenantID string, platform bool, limit int) []domain.AdminAction {
	result := make([]domain.AdminAction, 0, min(len(snapshot)+len(trail), limit))
	matches := func(action domain.AdminAction) bool {
		return (platform && tenantID == "") || action.TenantID == tenantID
	}
	si := len(snapshot) - 1 // walk the snapshot newest-first
	ti := 0                 // the trail is already newest-first
	for si >= 0 && ti < len(trail) {
		var next domain.AdminAction
		if !trail[ti].CreatedAt.Before(snapshot[si].CreatedAt) {
			next = trail[ti]
			ti++
		} else {
			next = snapshot[si]
			si--
		}
		if matches(next) {
			result = append(result, next)
			if len(result) == limit {
				return result
			}
		}
	}
	for ; si >= 0; si-- {
		if next := snapshot[si]; matches(next) {
			result = append(result, next)
			if len(result) == limit {
				return result
			}
		}
	}
	for ; ti < len(trail); ti++ {
		if next := trail[ti]; matches(next) {
			result = append(result, next)
			if len(result) == limit {
				return result
			}
		}
	}
	return result
}
