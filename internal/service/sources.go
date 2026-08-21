package service

import (
	"fmt"
	"sort"
	"strings"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func (s *Service) AddSource(actor string, source domain.SourceSystem) error {
	var err error
	source, err = normalizeSource(source)
	if err != nil {
		return err
	}
	if source.CreatedAt.IsZero() {
		source.CreatedAt = s.Now()
	}
	if !source.Active {
		source.Active = true
	}
	return s.Store.Update(func(data *store.Snapshot) error {
		if _, ok := data.Tenants[source.TenantID]; !ok {
			return fmt.Errorf("%w: tenant does not exist", domain.ErrNotFound)
		}
		key := store.SourceKey(source.TenantID, source.ID)
		if _, exists := data.Sources[key]; exists {
			return fmt.Errorf("%w: source already exists", domain.ErrConflict)
		}
		data.Sources[key] = source
		s.appendAdminAction(data, s.adminAction(source.TenantID, actor, domain.AdminActionSourceCreated, "source", source.ID, fmt.Sprintf("allowed_client_ids=%v", source.AllowedClientIDs)))
		return nil
	})
}

func (s *Service) UpdateSource(actor string, source domain.SourceSystem) (domain.SourceSystem, error) {
	var err error
	source, err = normalizeSource(source)
	if err != nil {
		return domain.SourceSystem{}, err
	}
	err = s.Store.Update(func(data *store.Snapshot) error {
		// Defense-in-depth (mirrors AddSource): the source key can only be
		// reached for an existing tenant; a source row without its tenant
		// record (hand-edited snapshot) must never be reachable by key
		// alone. With key-framing validation at every boundary this is
		// unreachable via the API, but fail-closed stays cheaper than a
		// collision.
		if _, ok := data.Tenants[source.TenantID]; !ok {
			return fmt.Errorf("%w: tenant does not exist", domain.ErrNotFound)
		}
		key := store.SourceKey(source.TenantID, source.ID)
		existing, ok := data.Sources[key]
		if !ok {
			return domain.ErrNotFound
		}
		source.CreatedAt = existing.CreatedAt
		data.Sources[key] = source
		s.appendAdminAction(data, s.adminAction(source.TenantID, actor, domain.AdminActionSourceUpdated, "source", source.ID, fmt.Sprintf("allowed_client_ids=%v active=%v", source.AllowedClientIDs, source.Active)))
		return nil
	})
	return source, err
}

func normalizeSource(source domain.SourceSystem) (domain.SourceSystem, error) {
	if source.TenantID == "" || source.ID == "" || source.Name == "" {
		return domain.SourceSystem{}, fmt.Errorf("%w: source tenant_id, id and name are required", domain.ErrInvalid)
	}
	// Key-framing charset rule: source.ID becomes the second component of
	// SourceKey and the source branch of Event.Stream-derived StreamKeys, so
	// an embedded KeySeparator (0x1F) would create multi-separator keys that
	// SplitTenantKey fail-closes on — invisible to VerifyIntegrity and
	// aggregate checkpointing. This single check covers AddSource (body ID)
	// and UpdateSource (path ID) before any store access.
	if err := domain.ValidKeyComponent("source id", source.ID); err != nil {
		return domain.SourceSystem{}, err
	}
	// FM-1 archive-component bound: source.ID is the source branch of
	// Event.Stream(), whose archive key component is percent-encoded (3×
	// expansion for non-safe bytes). A registration longer than
	// domain.MaxArchiveComponentBytes could never produce an archivable
	// stream, so it is rejected at the boundary instead of producing a
	// source every event referencing it would fail to archive.
	if err := domain.ValidArchiveComponentLength("source id", source.ID); err != nil {
		return domain.SourceSystem{}, err
	}
	source.AllowedClientIDs = append([]string(nil), source.AllowedClientIDs...)
	seen := make(map[string]struct{}, len(source.AllowedClientIDs))
	for _, clientID := range source.AllowedClientIDs {
		if strings.TrimSpace(clientID) == "" || clientID != strings.TrimSpace(clientID) {
			return domain.SourceSystem{}, fmt.Errorf("%w: allowed client IDs must be non-empty and trimmed", domain.ErrInvalid)
		}
		if _, duplicate := seen[clientID]; duplicate {
			return domain.SourceSystem{}, fmt.Errorf("%w: allowed client IDs must be unique", domain.ErrInvalid)
		}
		seen[clientID] = struct{}{}
	}
	sort.Strings(source.AllowedClientIDs)
	return source, nil
}

func (s *Service) ListSources(tenantID string) ([]domain.SourceSystem, error) {
	var result []domain.SourceSystem
	err := s.Store.ReadControl(func(data *store.Snapshot) error {
		for _, source := range data.Sources {
			if source.TenantID == tenantID {
				result = append(result, source)
			}
		}
		return nil
	})
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, err
}

func (s *Service) resolveIngestTenant(tenantHint, clientID, sourceID string) (string, error) {
	var resolved string
	err := s.Store.ReadControl(func(data *store.Snapshot) error {
		var resolveErr error
		resolved, resolveErr = resolveIngestTenantFromData(data, tenantHint, clientID, sourceID)
		return resolveErr
	})
	return resolved, err
}

func resolveIngestTenantFromData(data *store.Snapshot, tenantHint, clientID, sourceID string) (string, error) {
	if tenantHint != "" {
		if !sourceAccessAllowed(data, tenantHint, sourceID, clientID) {
			return "", sourceAccessError()
		}
		return tenantHint, nil
	}
	resolved := ""
	for _, source := range data.Sources {
		if !source.Active || source.ID != sourceID || !source.AllowsClient(clientID) {
			continue
		}
		if resolved != "" && resolved != source.TenantID {
			return "", sourceAccessError()
		}
		resolved = source.TenantID
	}
	if resolved == "" {
		return "", sourceAccessError()
	}
	return resolved, nil
}

func sourceAccessAllowed(data *store.Snapshot, tenantID, sourceID, clientID string) bool {
	source, ok := data.Sources[store.SourceKey(tenantID, sourceID)]
	return ok && source.Active && source.AllowsClient(clientID)
}

func sourceAccessError() error {
	return fmt.Errorf("%w: source_system is not allowed for authenticated client", domain.ErrForbidden)
}
