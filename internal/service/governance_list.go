package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// ListExports reads the control document only: a work list must not scan the
// ledger or fetch WORM objects. Summaries never bypass the Hold-gated detail
// and download APIs, and are deliberately free of evidence metadata.
func (s *Service) ListExports(ctx context.Context, tenantID, actor string, query domain.JobQuery) (domain.JobPage[domain.ExportSummary], error) {
	query, err := normalizeJobQuery(tenantID, actor, "exports", query)
	if err != nil {
		return domain.JobPage[domain.ExportSummary]{}, err
	}
	items := []domain.ExportSummary{}
	err = s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
		for _, job := range data.Exports {
			if job.TenantID != tenantID || !jobMatches(query, actor, job.Status, job.RequestedBy, job.Query.OperationID, job.CreatedAt) {
				continue
			}
			items = append(items, domain.ExportSummary{ID: job.ID, TenantID: job.TenantID, RequestedBy: job.RequestedBy, Status: job.Status, CreatedAt: job.CreatedAt, StartedAt: job.StartedAt, FinishedAt: job.FinishedAt})
		}
		return nil
	})
	if err != nil {
		return domain.JobPage[domain.ExportSummary]{}, err
	}
	return pageJobs(s, tenantID, actor, "exports", query, items, func(item domain.ExportSummary) jobPosition {
		return jobPosition{CreatedAt: item.CreatedAt, ID: item.ID}
	})
}

// ListRestores's actionable view excludes the requesting actor. The HTTP
// boundary additionally requires the existing decision permission; this
// selection is not authorization to decide and never replaces the atomic
// separation-of-duties check in transitionRestore.
func (s *Service) ListRestores(ctx context.Context, tenantID, actor string, query domain.JobQuery) (domain.JobPage[domain.RestoreRun], error) {
	query, err := normalizeJobQuery(tenantID, actor, "restores", query)
	if err != nil {
		return domain.JobPage[domain.RestoreRun]{}, err
	}
	items := []domain.RestoreRun{}
	err = s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
		for _, run := range data.RestoreRuns {
			if run.TenantID != tenantID || !jobMatches(query, actor, run.Status, run.CreatedBy, run.OperationID, run.CreatedAt) {
				continue
			}
			if query.View == "actionable" && (run.Status != domain.RestoreStatusPendingApproval || run.CreatedBy == "" || run.CreatedBy == actor) {
				continue
			}
			items = append(items, run)
		}
		return nil
	})
	if err != nil {
		return domain.JobPage[domain.RestoreRun]{}, err
	}
	return pageJobs(s, tenantID, actor, "restores", query, items, func(item domain.RestoreRun) jobPosition {
		return jobPosition{CreatedAt: item.CreatedAt, ID: item.ID}
	})
}

func normalizeJobQuery(tenantID, actor, kind string, query domain.JobQuery) (domain.JobQuery, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return query, err
	}
	if actor == "" {
		return query, fmt.Errorf("%w: actor is required", domain.ErrInvalid)
	}
	if query.PageSize < 0 || query.PageSize > domain.MaxPageSize {
		return query, fmt.Errorf("%w: invalid page_size", domain.ErrInvalid)
	}
	if query.PageSize == 0 {
		query.PageSize = domain.DefaultPageSize
	}
	if query.View == "" {
		query.View = "all"
	}
	if query.View != "all" && query.View != "mine" && !(kind == "restores" && query.View == "actionable") {
		return query, fmt.Errorf("%w: invalid job view", domain.ErrInvalid)
	}
	statuses := map[string]bool{"": true, "pending": true, "running": true, "completed": true, "failed": true}
	if kind == "restores" {
		statuses = map[string]bool{"": true, "pending_approval": true, "approved": true, "rejected": true}
	}
	if !statuses[query.Status] {
		return query, fmt.Errorf("%w: invalid job status", domain.ErrInvalid)
	}
	if !query.From.IsZero() && !query.To.IsZero() && !query.From.Before(query.To) {
		return query, fmt.Errorf("%w: from must be before to", domain.ErrInvalid)
	}
	query.From, query.To = query.From.UTC(), query.To.UTC()
	return query, nil
}

func jobMatches(query domain.JobQuery, actor, status, owner, operation string, at time.Time) bool {
	return (query.Status == "" || query.Status == status) &&
		(query.RequestedBy == "" || query.RequestedBy == owner) &&
		(query.OperationID == "" || query.OperationID == operation) &&
		(query.View != "mine" || owner == actor) &&
		(query.From.IsZero() || !at.Before(query.From)) &&
		(query.To.IsZero() || at.Before(query.To))
}

type jobPosition struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

type jobCursor struct {
	Version int         `json:"v"`
	Binding string      `json:"binding"`
	Last    jobPosition `json:"last"`
}

func jobBinding(tenantID, actor, kind string, query domain.JobQuery) string {
	query.Cursor, query.PageSize = "", 0
	encoded, _ := json.Marshal(struct {
		Tenant string          `json:"tenant"`
		Actor  string          `json:"actor"`
		Kind   string          `json:"kind"`
		Query  domain.JobQuery `json:"query"`
	}{tenantID, actor, kind, query})
	return domain.HashBytes(encoded)
}

func (s *Service) encodeJobCursor(binding string, last jobPosition) string {
	encoded, _ := json.Marshal(jobCursor{Version: 1, Binding: binding, Last: last})
	mac := hmac.New(sha256.New, []byte(s.Config.SigningSecret))
	_, _ = mac.Write([]byte("governance.cursor.v1\x00"))
	_, _ = mac.Write(encoded)
	return base64.RawURLEncoding.EncodeToString(encoded) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (s *Service) decodeJobCursor(raw, binding string) (jobPosition, error) {
	invalid := fmt.Errorf("%w: cursor does not match job query", domain.ErrInvalid)
	if len(raw) > 4096 {
		return jobPosition{}, invalid
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 2 {
		return jobPosition{}, invalid
	}
	encoded, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return jobPosition{}, invalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return jobPosition{}, invalid
	}
	mac := hmac.New(sha256.New, []byte(s.Config.SigningSecret))
	_, _ = mac.Write([]byte("governance.cursor.v1\x00"))
	_, _ = mac.Write(encoded)
	var cursor jobCursor
	if !hmac.Equal(signature, mac.Sum(nil)) || json.Unmarshal(encoded, &cursor) != nil || cursor.Version != 1 || cursor.Binding != binding || cursor.Last.ID == "" || cursor.Last.CreatedAt.IsZero() {
		return jobPosition{}, invalid
	}
	return cursor.Last, nil
}

func positionBefore(left, right jobPosition) bool {
	if !left.CreatedAt.Equal(right.CreatedAt) {
		return left.CreatedAt.Before(right.CreatedAt)
	}
	return left.ID < right.ID
}

func pageJobs[T any](s *Service, tenantID, actor, kind string, query domain.JobQuery, items []T, position func(T) jobPosition) (domain.JobPage[T], error) {
	binding := jobBinding(tenantID, actor, kind, query)
	var after jobPosition
	if query.Cursor != "" {
		var err error
		after, err = s.decodeJobCursor(query.Cursor, binding)
		if err != nil {
			return domain.JobPage[T]{}, err
		}
	}
	sort.Slice(items, func(i, j int) bool { return positionBefore(position(items[j]), position(items[i])) })
	result := domain.JobPage[T]{Items: []T{}, Count: len(items)}
	start := 0
	for start < len(items) && query.Cursor != "" && !positionBefore(position(items[start]), after) {
		start++
	}
	items = items[start:]
	if len(items) > query.PageSize {
		result.Items = items[:query.PageSize]
		result.NextCursor = s.encodeJobCursor(binding, position(result.Items[len(result.Items)-1]))
	} else {
		result.Items = items
	}
	return result, nil
}
