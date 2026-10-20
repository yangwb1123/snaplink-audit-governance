package domain

import "time"

// JobQuery selects a live governance work list, not an immutable evidence
// snapshot. Cursors are bound to the tenant, actor, list kind and filters.
type JobQuery struct {
	Status      string    `json:"status,omitempty"`
	RequestedBy string    `json:"requested_by,omitempty"`
	OperationID string    `json:"operation_id,omitempty"`
	View        string    `json:"view,omitempty"`
	From        time.Time `json:"from,omitempty"`
	To          time.Time `json:"to,omitempty"`
	Cursor      string    `json:"cursor,omitempty"`
	PageSize    int       `json:"page_size,omitempty"`
}

// ExportSummary exposes operational identity/status only. It deliberately
// omits evidence selection, object location, digest and event cardinality;
// selecting a row still goes through GetExport's authoritative Hold gate.
type ExportSummary struct {
	ID          string     `json:"id"`
	TenantID    string     `json:"tenant_id"`
	RequestedBy string     `json:"requested_by"`
	Status      string     `json:"status"`
	RetryOf     string     `json:"retry_of,omitempty"`
	FailureCode string     `json:"failure_code,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}

type JobPage[T any] struct {
	Items      []T    `json:"items"`
	Count      int    `json:"count"`
	NextCursor string `json:"next_cursor,omitempty"`
}
