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
		data.AdminActions = append(data.AdminActions, s.adminAction(source.TenantID, actor, domain.AdminActionSourceCreated, "source", source.ID, fmt.Sprintf("allowed_client_ids=%v", source.AllowedClientIDs)))
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
		key := store.SourceKey(source.TenantID, source.ID)
		existing, ok := data.Sources[key]
		if !ok {
			return domain.ErrNotFound
		}
		source.CreatedAt = existing.CreatedAt
		data.Sources[key] = source
		data.AdminActions = append(data.AdminActions, s.adminAction(source.TenantID, actor, domain.AdminActionSourceUpdated, "source", source.ID, fmt.Sprintf("allowed_client_ids=%v active=%v", source.AllowedClientIDs, source.Active)))
		return nil
	})
	return source, err
}

func normalizeSource(source domain.SourceSystem) (domain.SourceSystem, error) {
	if source.TenantID == "" || source.ID == "" || source.Name == "" {
		return domain.SourceSystem{}, fmt.Errorf("%w: source tenant_id, id and name are required", domain.ErrInvalid)
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
	err := s.Store.Read(func(data *store.Snapshot) error {
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
	err := s.Store.Read(func(data *store.Snapshot) error {
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
