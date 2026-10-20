package store

import "time"

// ExportLease is runtime control metadata, never evidence or a public job DTO.
// Each CAS claim gets a fresh owner fence. Store it with the job in the SAME
// control CAS; API responses must not publish this map or its owner token.
// A missing old map identifies legacy jobs, not a lease inferred from age.
type ExportLease struct {
	Version     int       `json:"version"`
	JobID       string    `json:"job_id"`
	TenantID    string    `json:"tenant_id"`
	Owner       string    `json:"owner"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
	ExpiresAt   time.Time `json:"expires_at"`
}
