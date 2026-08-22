package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	StatusReceived    = "received"
	StatusAccepted    = "accepted"
	StatusLedgered    = "ledgered"
	StatusIndexed     = "indexed"
	StatusArchived    = "archived"
	StatusFailed      = "failed"
	MaxEventBytes     = 256 * 1024
	DefaultPageSize   = 100
	MaxPageSize       = 1000
	MaxTimelineEvents = 10000
	// MaxArchiveComponentBytes caps every identifier that becomes an archive
	// key component (tenant id, stream-derived components, event id). The
	// injective percent-encoding expands a non-safe byte 3×, so 85 bytes is
	// exactly the FileStore boundary: 85 × 3 = 255 = POSIX NAME_MAX, and 86
	// bytes (or 29 three-byte runes = 87 bytes) can never fit a path
	// component. Three maximal components still fit the S3 1024-byte key
	// budget (35 fixed framing bytes + 3 × 255 = 800). UUIDs are 36 bytes,
	// so contract-conformant identifiers are unaffected.
	MaxArchiveComponentBytes = 85

	// RestoreStatusPendingApproval means the restore run awaits a human
	// approver; approval and business execution are separate facts.
	RestoreStatusPendingApproval = "pending_approval"
	RestoreStatusApproved        = "approved"
	RestoreStatusRejected        = "rejected"
)

var (
	ErrNotFound       = errors.New("not found")
	ErrConflict       = errors.New("conflict")
	ErrUnauthorized   = errors.New("unauthorized")
	ErrForbidden      = errors.New("forbidden")
	ErrInvalid        = errors.New("invalid request")
	ErrQuotaExceeded  = errors.New("quota exceeded")
	ErrSchemaNotFound = errors.New("schema not found")
	// ErrTenantMismatch means the envelope carried a tenant_id that does not
	// match the tenant resolved server-side from the authenticated client.
	// Rejecting instead of silently re-labelling prevents a writer from
	// attributing an event to a tenant it has no authority for (422).
	ErrTenantMismatch = errors.New("tenant mismatch")
	// ErrOccurredAtOutOfRange marks an event whose occurred_at falls outside the
	// ledger horizon [MinOccurredAt, MaxOccurredAt] (ClickHouse DateTime64
	// projection range). It wraps ErrInvalid so existing programmatic ErrInvalid
	// handling keeps working, while HTTP mapping (422) and the projector's
	// permanent-error classification can distinguish the class.
	ErrOccurredAtOutOfRange = fmt.Errorf("%w: occurred_at out of range", ErrInvalid)
)

// MinOccurredAt / MaxOccurredAt bound the ledger horizon to the ClickHouse
// DateTime64 platform contract (documented range 1900-01-01 00:00:00 ..
// 2299-12-31 23:59:59.99999999; column precision 3 ⇒ .999 ceiling). The
// immutable ledger must never commit a fact the projection tier cannot index;
// every ingest surface funnels through ValidateBasic, so the bound is enforced
// before any durable commit.
var (
	MinOccurredAt = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	MaxOccurredAt = time.Date(2299, 12, 31, 23, 59, 59, 999_000_000, time.UTC)
)

// Event is the canonical audit fact accepted from a source system. Payload is
// intentionally an interface: the registered schema, not arbitrary JSON,
// defines the accepted shape.
type Event struct {
	EventID            string                 `json:"event_id"`
	TenantID           string                 `json:"tenant_id"`
	SourceSystem       string                 `json:"source_system"`
	EventType          string                 `json:"event_type"`
	SchemaID           string                 `json:"schema_id"`
	SchemaVersion      int                    `json:"schema_version"`
	OccurredAt         time.Time              `json:"occurred_at"`
	ReceivedAt         time.Time              `json:"received_at"`
	OperationID        string                 `json:"operation_id,omitempty"`
	CausationID        string                 `json:"causation_id,omitempty"`
	CorrelationID      string                 `json:"correlation_id,omitempty"`
	TraceID            string                 `json:"trace_id,omitempty"`
	SpanID             string                 `json:"span_id,omitempty"`
	Actor              Actor                  `json:"actor"`
	Targets            []Target               `json:"targets,omitempty"`
	AggregateType      string                 `json:"aggregate_type,omitempty"`
	AggregateID        string                 `json:"aggregate_id,omitempty"`
	AggregateVersion   int64                  `json:"aggregate_version,omitempty"`
	WorkflowInstanceID string                 `json:"workflow_instance_id,omitempty"`
	ExecutionRunID     string                 `json:"execution_run_id,omitempty"`
	Action             string                 `json:"action"`
	Outcome            string                 `json:"outcome"`
	Reason             string                 `json:"reason,omitempty"`
	ChangedFields      map[string]FieldChange `json:"changed_fields,omitempty"`
	Payload            map[string]any         `json:"payload,omitempty"`
	PayloadRef         string                 `json:"payload_ref,omitempty"`
	SourceDigest       string                 `json:"source_digest,omitempty"`
	DataClassification string                 `json:"data_classification"`
	RetentionClass     string                 `json:"retention_class"`
	IdempotencyKey     string                 `json:"idempotency_key"`
	StreamID           string                 `json:"stream_id,omitempty"`
	Sequence           int64                  `json:"sequence,omitempty"`
	PrevHash           string                 `json:"prev_hash,omitempty"`
	Hash               string                 `json:"hash,omitempty"`
	ServerVersion      string                 `json:"server_version,omitempty"`
}

type Actor struct {
	ID         string   `json:"id"`
	Type       string   `json:"type,omitempty"`
	Name       string   `json:"name,omitempty"`
	Department string   `json:"department,omitempty"`
	Roles      []string `json:"roles,omitempty"`
}

type Target struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
}

type FieldChange struct {
	Before any `json:"before,omitempty"`
	After  any `json:"after,omitempty"`
}

type EventReceipt struct {
	EventID  string `json:"event_id"`
	TenantID string `json:"tenant_id"`
	// IdempotencyKey is retained with the receipt so idempotency remains
	// enforceable after the event payload is evicted from the hot snapshot.
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Status         string    `json:"status"`
	AcceptedAt     time.Time `json:"accepted_at"`
	LedgeredAt     time.Time `json:"ledgered_at,omitempty"`
	IndexedAt      time.Time `json:"indexed_at,omitempty"`
	ArchivedAt     time.Time `json:"archived_at,omitempty"`
	StreamID       string    `json:"stream_id,omitempty"`
	Sequence       int64     `json:"sequence,omitempty"`
	Hash           string    `json:"hash,omitempty"`
	Duplicate      bool      `json:"duplicate,omitempty"`
	Conflict       bool      `json:"conflict,omitempty"`
	ErrorCode      string    `json:"error_code,omitempty"`
	ErrorMessage   string    `json:"error_message,omitempty"`
}

// DeadLetter records an event (or sealed segment) that will never archive:
// the archive key cannot fit the backend limits (FM-1: the 3× percent-
// encoding expansion of an over-limit identifier) or a byte-different object
// already occupies the key (F-2: tampering, a legacy lossy-framed object, or
// a cross-deployment collision). The receipt stays StatusIndexed (the
// durable truth: the event is not archived) and carries ErrorCode/
// ErrorMessage; ArchivePending excludes dead-lettered entries from the retry
// set, so one bad key can never wedge a tenant's backlog. Segments, which
// have no receipt, use a synthetic event id ("seg:<stream>:<first>-<last>").
type DeadLetter struct {
	TenantID     string    `json:"tenant_id"`
	EventID      string    `json:"event_id"`
	StreamID     string    `json:"stream_id,omitempty"`
	Sequence     int64     `json:"sequence,omitempty"`
	Reason       string    `json:"reason"`
	ErrorMessage string    `json:"error_message,omitempty"`
	At           time.Time `json:"at"`
}

type EventSchema struct {
	TenantID         string    `json:"tenant_id"`
	SchemaID         string    `json:"schema_id"`
	Version          int       `json:"version"`
	EventType        string    `json:"event_type"`
	RequiredFields   []string  `json:"required_fields,omitempty"`
	AllowedFields    []string  `json:"allowed_fields,omitempty"`
	EncryptedFields  []string  `json:"encrypted_fields,omitempty"`
	SearchableFields []string  `json:"searchable_fields,omitempty"`
	Classification   string    `json:"classification"`
	Active           bool      `json:"active"`
	CreatedAt        time.Time `json:"created_at"`
}

type SourceSystem struct {
	ID               string    `json:"id"`
	TenantID         string    `json:"tenant_id"`
	Name             string    `json:"name"`
	AllowedClientIDs []string  `json:"allowed_client_ids,omitempty"`
	Active           bool      `json:"active"`
	CreatedAt        time.Time `json:"created_at"`
}

// AllowsClient applies the fail-closed source binding. Existing source
// records without an explicit allow-list accept only a client whose signed
// client_id is exactly the source ID.
func (s SourceSystem) AllowsClient(clientID string) bool {
	if clientID == "" {
		return false
	}
	if len(s.AllowedClientIDs) == 0 {
		return clientID == s.ID
	}
	for _, allowed := range s.AllowedClientIDs {
		if clientID == allowed {
			return true
		}
	}
	return false
}

// IngestPrincipal contains only server-derived identity. Transports must not
// populate it from the event body or caller-controlled forwarding headers.
type IngestPrincipal struct {
	ClientID string
}

type Tenant struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	HomeRegion      string    `json:"home_region"`
	DataRegion      string    `json:"data_region"`
	Active          bool      `json:"active"`
	EventsPerSecond int       `json:"events_per_second"`
	Burst           int       `json:"burst"`
	CreatedAt       time.Time `json:"created_at"`
}

type RetentionPolicy struct {
	TenantID       string `json:"tenant_id"`
	HotDays        int    `json:"hot_days"`
	WarmDays       int    `json:"warm_days"`
	ArchiveDays    int    `json:"archive_days"`
	RetentionClass string `json:"retention_class"`
}

type RetentionReport struct {
	TenantID        string          `json:"tenant_id"`
	EvaluatedAt     time.Time       `json:"evaluated_at"`
	Policy          RetentionPolicy `json:"policy"`
	EligibleEvents  int             `json:"eligible_events"`
	ProtectedEvents int             `json:"protected_events"`
	HoldIDs         []string        `json:"hold_ids,omitempty"`
	Action          string          `json:"action"`
}

type LegalHold struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	Name       string     `json:"name"`
	Reason     string     `json:"reason"`
	Filter     Query      `json:"filter"`
	CreatedBy  string     `json:"created_by"`
	CreatedAt  time.Time  `json:"created_at"`
	ReleasedAt *time.Time `json:"released_at,omitempty"`
	ReleasedBy string     `json:"released_by,omitempty"`
}

type Query struct {
	From          time.Time `json:"from"`
	To            time.Time `json:"to"`
	EventType     string    `json:"event_type,omitempty"`
	SourceSystem  string    `json:"source_system,omitempty"`
	ActorID       string    `json:"actor_id,omitempty"`
	TargetID      string    `json:"target_id,omitempty"`
	AggregateType string    `json:"aggregate_type,omitempty"`
	AggregateID   string    `json:"aggregate_id,omitempty"`
	OperationID   string    `json:"operation_id,omitempty"`
	CausationID   string    `json:"causation_id,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	TraceID       string    `json:"trace_id,omitempty"`
	Outcome       string    `json:"outcome,omitempty"`
	PayloadField  string    `json:"payload_field,omitempty"`
	PayloadDigest string    `json:"payload_digest,omitempty"`
	StreamID      string    `json:"stream_id,omitempty"`
	Cursor        string    `json:"cursor,omitempty"`
	PageSize      int       `json:"page_size,omitempty"`
}

type QueryResult struct {
	Items      []Event `json:"items"`
	NextCursor string  `json:"next_cursor,omitempty"`
	Count      int     `json:"count"`
}

// EventFacets is the bounded compatibility projection used by the Snaplink
// Console filter UI. It deliberately exposes counts only, never event payload.
type EventFacets struct {
	Total     int            `json:"total"`
	Outcomes  map[string]int `json:"outcomes"`
	Types     map[string]int `json:"types"`
	Clients   map[string]int `json:"clients"`
	Providers map[string]int `json:"providers"`
}

type OperationSummary struct {
	OperationID string    `json:"operation_id"`
	TenantID    string    `json:"tenant_id"`
	EventCount  int       `json:"event_count"`
	FirstAt     time.Time `json:"first_at"`
	LastAt      time.Time `json:"last_at"`
	Outcomes    []string  `json:"outcomes"`
}

type Segment struct {
	TenantID      string    `json:"tenant_id"`
	StreamID      string    `json:"stream_id"`
	FirstSequence int64     `json:"first_sequence"`
	LastSequence  int64     `json:"last_sequence"`
	FirstPrevHash string    `json:"first_prev_hash"`
	LastHash      string    `json:"last_hash"`
	EventCount    int       `json:"event_count"`
	MerkleRoot    string    `json:"merkle_root"`
	ManifestHash  string    `json:"manifest_hash"`
	Signature     string    `json:"signature"`
	CreatedAt     time.Time `json:"created_at"`
}

type Checkpoint struct {
	ID         string    `json:"id"`
	TenantID   string    `json:"tenant_id"`
	StreamID   string    `json:"stream_id"`
	Sequence   int64     `json:"sequence"`
	MerkleRoot string    `json:"merkle_root"`
	Signature  string    `json:"signature"`
	Algorithm  string    `json:"algorithm"`
	CreatedAt  time.Time `json:"created_at"`
}

// AggregateCheckpoint is a periodic signed Merkle root over the latest
// checkpoint of every stream in a tenant (architecture plan section 10:
// periodically build a Merkle root over multiple segment roots).
type AggregateCheckpoint struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenant_id"`
	StreamCount int       `json:"stream_count"`
	Root        string    `json:"root"`
	Signature   string    `json:"signature"`
	Algorithm   string    `json:"algorithm"`
	CreatedAt   time.Time `json:"created_at"`
	StreamRoots []string  `json:"stream_roots,omitempty"`
}

type ExportJob struct {
	ID          string     `json:"id"`
	TenantID    string     `json:"tenant_id"`
	RequestedBy string     `json:"requested_by"`
	Query       Query      `json:"query"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	ObjectPath  string     `json:"object_path,omitempty"`
	Digest      string     `json:"digest,omitempty"`
	EventCount  int        `json:"event_count"`
	Error       string     `json:"error,omitempty"`
}

type ReplayResult struct {
	TenantID     string         `json:"tenant_id"`
	OperationID  string         `json:"operation_id,omitempty"`
	AggregateID  string         `json:"aggregate_id,omitempty"`
	EventCount   int            `json:"event_count"`
	State        map[string]any `json:"state"`
	LastSequence int64          `json:"last_sequence"`
}

type RestoreRequest struct {
	OperationID string `json:"operation_id"`
	Reason      string `json:"reason"`
}

type RestorePreview struct {
	ID               string         `json:"id"`
	TenantID         string         `json:"tenant_id"`
	OperationID      string         `json:"operation_id"`
	ProposedState    map[string]any `json:"proposed_state"`
	ExternalCalls    []string       `json:"external_calls"`
	RequiresApproval bool           `json:"requires_approval"`
}

type RestoreRun struct {
	ID          string     `json:"id"`
	TenantID    string     `json:"tenant_id"`
	OperationID string     `json:"operation_id"`
	Status      string     `json:"status"`
	Reason      string     `json:"reason"`
	CreatedBy   string     `json:"created_by,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	ApprovedBy  string     `json:"approved_by,omitempty"`
	ApprovedAt  *time.Time `json:"approved_at,omitempty"`
	RejectedBy  string     `json:"rejected_by,omitempty"`
	RejectedAt  *time.Time `json:"rejected_at,omitempty"`
}

// AdminAction is one append-only self-audit record. Every control-plane
// mutation (tenants, sources, schemas, retention policies, exports, legal
// holds, restore approvals) writes an action atomically with the change it
// audits.
type AdminAction struct {
	ID         string    `json:"id"`
	TenantID   string    `json:"tenant_id,omitempty"`
	Actor      string    `json:"actor"`
	Action     string    `json:"action"`
	TargetType string    `json:"target_type,omitempty"`
	TargetID   string    `json:"target_id,omitempty"`
	Detail     string    `json:"detail,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

const (
	AdminActionTenantCreated      = "tenant.created"
	AdminActionSourceCreated      = "source.created"
	AdminActionSourceUpdated      = "source.updated"
	AdminActionSchemaCreated      = "schema.created"
	AdminActionRetentionPolicySet = "retention_policy.set"
	AdminActionExportCreated      = "export.created"
	// AdminActionExportRecovered records a governance-worker recovery: an
	// export job stuck in "running" past the worker's stuck-age threshold was
	// failed by the worker pass (atomic with the status change it audits).
	AdminActionExportRecovered = "export.recovered"
	// AdminActionExportBlocked records a legal-hold gate denial: a create,
	// run-time or download attempt for an export whose query overlaps an
	// active legal hold. TargetID carries the blocking hold ID.
	AdminActionExportBlocked = "export.blocked"
	// AdminActionExportRejected records a download whose sealed object failed
	// integrity verification (binding authentication, digest format or digest
	// equality) in VerifyExportDownload. TargetID carries the job ID; the
	// fact carries no content, error internals or actor. The governance
	// trail must not be silent about integrity failures — an unrecorded
	// tamper attempt is exactly the attack class this fact exists to expose
	// (security review F-2).
	AdminActionExportRejected    = "export.download_rejected"
	AdminActionLegalHoldCreated  = "legal_hold.created"
	AdminActionLegalHoldReleased = "legal_hold.released"
	AdminActionRestoreCreated    = "restore.created"
	AdminActionRestoreApproved   = "restore.approved"
	AdminActionRestoreRejected   = "restore.rejected"
	AdminActionEventRead         = "audit.event.read"
	AdminActionEventExport       = "audit.event.export"
)

func (e Event) ValidateBasic() error {
	for name, value := range map[string]string{
		"event_id": e.EventID, "source_system": e.SourceSystem, "event_type": e.EventType,
		"schema_id": e.SchemaID, "action": e.Action, "outcome": e.Outcome,
		"data_classification": e.DataClassification, "retention_class": e.RetentionClass,
		"idempotency_key": e.IdempotencyKey,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalid, name)
		}
	}
	// Key-framing charset rule: these five identifiers become composite-key
	// components (EventKey/StreamKey via Event.Stream), so an embedded
	// KeySeparator (0x1F) would create multi-separator keys that
	// SplitTenantKey fail-closes on — permanently invisible to
	// VerifyIntegrity and aggregate checkpointing. The ordered slice keeps
	// error precedence deterministic; the three optional stream components
	// are validated only when non-empty (an absent optional component never
	// derives a key segment).
	for _, pair := range [][2]string{
		{"event id", e.EventID}, {"source system", e.SourceSystem},
	} {
		if err := ValidKeyComponent(pair[0], pair[1]); err != nil {
			return err
		}
	}
	// Archive key expansion bound (FM-1): event_id and source_system are
	// archive key components (event object file name and the source branch
	// of Event.Stream()). The injective percent-encoding expands non-safe
	// bytes 3×, so a component longer than MaxArchiveComponentBytes can
	// never fit a FileStore path component; rejecting at the boundary keeps
	// over-limit identifiers out of the ledger instead of producing a
	// permanently unarchivable receipt.
	for _, pair := range [][2]string{
		{"event id", e.EventID}, {"source system", e.SourceSystem},
	} {
		if err := ValidArchiveComponentLength(pair[0], pair[1]); err != nil {
			return err
		}
	}
	// Stream components: the aggregate frame is tenant + ":aggregate:" +
	// type + ":" + id, so a ':' in either aggregate component would make the
	// frame ambiguous — ("a","b:c") and ("a:b","c") derive the identical
	// stream key and silently merge two aggregates into one hash-chained
	// ledger stream. ValidStreamComponent layers exactly the ':' rejection
	// on the generic rule. operation id stays on the generic rule:
	// tenant:operation:<id> is a single-component frame, injective by
	// construction.
	for _, pair := range [][2]string{
		{"aggregate type", e.AggregateType}, {"aggregate id", e.AggregateID},
	} {
		if pair[1] != "" {
			if err := ValidStreamComponent(pair[0], pair[1]); err != nil {
				return err
			}
		}
	}
	for _, pair := range [][2]string{
		{"operation id", e.OperationID},
	} {
		if pair[1] != "" {
			if err := ValidKeyComponent(pair[0], pair[1]); err != nil {
				return err
			}
			if err := ValidArchiveComponentLength(pair[0], pair[1]); err != nil {
				return err
			}
		}
	}
	if e.SchemaVersion <= 0 {
		return fmt.Errorf("%w: schema_version must be positive", ErrInvalid)
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred_at is required", ErrInvalid)
	}
	// Zero-check stays first (a zero time.Time is below the floor and must keep
	// reporting "occurred_at is required"). Range is then judged on the UTC
	// instant, matching Store.Insert's event.OccurredAt.UTC() binding.
	if e.OccurredAt.Before(MinOccurredAt) || e.OccurredAt.After(MaxOccurredAt) {
		return fmt.Errorf("%w: occurred_at %v is outside the ledger horizon [%s, %s]",
			ErrOccurredAtOutOfRange, e.OccurredAt.UTC().Format(time.RFC3339Nano),
			MinOccurredAt.Format(time.RFC3339), MaxOccurredAt.Format(time.RFC3339))
	}
	if e.Actor.ID == "" {
		return fmt.Errorf("%w: actor.id is required", ErrInvalid)
	}
	if e.Payload == nil && e.PayloadRef == "" {
		return fmt.Errorf("%w: payload or payload_ref is required", ErrInvalid)
	}
	return nil
}

func (e Event) Stream() string {
	if e.StreamID != "" {
		return e.StreamID
	}
	if e.AggregateType != "" && e.AggregateID != "" {
		return e.TenantID + ":aggregate:" + e.AggregateType + ":" + e.AggregateID
	}
	if e.OperationID != "" {
		return e.TenantID + ":operation:" + e.OperationID
	}
	return e.TenantID + ":source:" + e.SourceSystem
}

func HashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
