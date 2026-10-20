package store

import "time"

// ExportCheckpoint is a private, authenticated execution artifact, not ledger
// truth or a public DTO. Version 1 resumes only the sealed-object finalization
// stage. No payload, ciphertext, raw filters, search digests or lease owner are
// copied here. Commit it with export.checkpointed in the same control CAS.
type ExportCheckpoint struct {
	Version      int       `json:"version"`
	SealVersion  int       `json:"seal_version"`
	JobID        string    `json:"job_id"`
	TenantID     string    `json:"tenant_id"`
	Binding      string    `json:"binding"`
	ObjectPath   string    `json:"object_path"`
	SealedDigest string    `json:"sealed_digest"`
	Digest       string    `json:"digest"`
	EventCount   int       `json:"event_count"`
	CreatedAt    time.Time `json:"created_at"`
	Signature    string    `json:"signature"`
}
