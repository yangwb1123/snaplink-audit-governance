package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/fsutil"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

type Config struct {
	ServerVersion string
	ArchiveDir    string
	SigningSecret string
	EncryptionKey string
	SegmentSize   int
	MaxEventBytes int
	Now           func() time.Time
	// AllowDevSecrets explicitly opts into the well-known development
	// secrets (empty values, default fallback and SigningSecret ==
	// EncryptionKey reuse). It is independent of the auth-scoped
	// -allow-dev-auth flag and defaults to false: production must fail
	// fast on missing, default or shared secrets instead of running with
	// publicly known key material.
	AllowDevSecrets bool
	// Signer produces checkpoint signatures. Defaults to HMAC-SHA256 over
	// SigningSecret; a Vault Transit signer can be injected for KMS-backed
	// signatures where the private key never leaves Vault.
	Signer Signer
	// Archive persists WORM-compatible compliance objects. Defaults to the
	// local read-only directory; an S3 Object Lock store can be injected.
	Archive archive.Store
	// AggregateCheckpointRetention caps retained aggregate-checkpoint
	// history per tenant (drop-oldest; floor 1 — the most recent record is
	// always retained). Values <= 0 select the documented default
	// (DefaultAggregateCheckpointRetention). The worker reads the override
	// from AUDIT_AGGREGATE_CHECKPOINT_HISTORY.
	AggregateCheckpointRetention int
	// StuckExportAge applies only to legacy running jobs without leases,
	// measured from usable StartedAt, otherwise CreatedAt. New jobs use their
	// durable lease expiry instead. Values <= 0 select the documented
	// default (DefaultStuckExportAge). The worker reads the override from
	// AUDIT_GOVERNANCE_STUCK_EXPORT_AGE / -stuck-export-age.
	StuckExportAge time.Duration
	// MaxAdminActions caps Snapshot.AdminActions (mutation-path admin
	// facts): drop-oldest, newest retained, enforced at every append site.
	// Values <= 0 select the documented default (DefaultMaxAdminActions).
	// The API reads the override from AUDIT_ADMIN_ACTIONS_CAP /
	// -admin-actions-cap.
	MaxAdminActions int
	// MaxAdminTrailActions caps the separate read self-audit trail (read
	// facts never rewrite the snapshot). 0 = unbounded append-only
	// (operator-explicit); otherwise the trail is compacted to its newest
	// MaxAdminTrailActions facts. Negative values select the documented
	// default (DefaultMaxAdminTrailActions). The API reads the override
	// from AUDIT_ADMIN_TRAIL_CAP / -admin-trail-cap.
	MaxAdminTrailActions int
	// LedgeredPublisher optionally publishes the post-commit event to the
	// ledgered topic. A nil publisher preserves the local reference behavior.
	LedgeredPublisher LedgeredPublisher
	// QueryProjection is opt-in; nil keeps reference reads. Projection is only
	// a candidate index; returned evidence still comes from Ledger/WORM.
	QueryProjection QueryProjectionReader
	// Logf receives bounded publication diagnostics. It is nil-safe so service
	// tests and embedded callers do not need to provide a logger.
	Logf func(format string, args ...any)
}

// Signer creates and verifies checkpoint signatures. Implementations must be
// deterministic per input so Verify can re-check archived manifests. The
// context cancels in-flight external (Vault) requests; local signers ignore it.
type Signer interface {
	Sign(ctx context.Context, data []byte) (string, error)
	Verify(ctx context.Context, data []byte, signature string) (bool, error)
	Algorithm() string
}

// hmacSigner is the local development signature scheme.
type hmacSigner struct {
	secret string
}

func (h hmacSigner) Sign(_ context.Context, data []byte) (string, error) {
	mac := hmac.New(sha256.New, []byte(h.secret))
	_, _ = mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func (h hmacSigner) Verify(ctx context.Context, data []byte, signature string) (bool, error) {
	expected, err := h.Sign(ctx, data)
	if err != nil {
		return false, err
	}
	return hmac.Equal([]byte(expected), []byte(signature)), nil
}

func (h hmacSigner) Algorithm() string { return "HMAC-SHA256(dev-compatible)" }

type Service struct {
	Store   *store.Store
	Config  Config
	quotaMu sync.Mutex
	quotas  map[string]quotaWindow
	// Dispatcher reservations cover each executor's final control read and
	// atomic follow-up wake, not just terminal visibility. Running claims are
	// also checked against the shared durable control document across replicas.
	exportMu             sync.Mutex
	exportActive         map[string]string
	exportDispatching    bool
	exportLastTenant     string
	exportWakeGeneration uint64
	exportHeartbeat      func() (<-chan time.Time, func())
	// ledgeredGatesMu guards ledgeredGates. It is held only while a gate is
	// looked up, referenced or released — never across a publish or a store
	// call — so it cannot become the head-of-line block the per-event gate
	// exists to remove.
	ledgeredGatesMu sync.Mutex
	// ledgeredGates maps store.EventKey to the gate that serializes the
	// publishers of that one pending event. An entry exists only while some
	// publisher holds a reference, so the table is bounded by in-flight
	// publications rather than by the events a process has ever published.
	//
	// The invariant, narrowed in scope: at most one publisher per pending
	// event at a time. The previous process-wide ledgeredPublishMu also
	// serialized unrelated events, and FlushLedgeredOutbox held it for a whole
	// round (one snapshot read plus up to ledgeredOutboxBatchSize publishes
	// and acknowledgements), so a slow broker head-of-line blocked every
	// concurrent ingest by an amount bounded by nothing in the request path.
	ledgeredGates map[string]*ledgeredKeyGate
	// ledgeredFlushMu admits one flush round at a time per process. Two
	// overlapping rounds would read the same outbox snapshot and could each
	// publish the same key from their own stale copy — a duplicate the
	// per-event gate cannot prevent, because the gate is free once the first
	// round acknowledges the key. It is never shared with the request path.
	ledgeredFlushMu sync.Mutex
	// ledgeredPublishDeferrals counts post-commit publications the request
	// path declined because another publisher already owned the event's key
	// gate. The durable intent stays in Snapshot.LedgeredOutbox, so a
	// deferral delays the rebuildable projection feed by at most one flush
	// round; it can never lose the event and never changes the ingest answer.
	// Exposed as audit_ledgered_publish_deferred_total.
	ledgeredPublishDeferrals atomic.Uint64
	// ledgeredRequestAcks counts post-commit publications acknowledged since
	// process start. A flush round samples it before its snapshot read and
	// re-checks it before each key, so a key the request path already
	// delivered since the snapshot is skipped instead of published twice. The
	// flusher's own acknowledgements are deliberately not counted: its
	// snapshot already reflects them, and only one round runs at a time.
	ledgeredRequestAcks atomic.Uint64
	// ledgeredIntentRepairs counts committed intent re-creations by client
	// retries and active reconciliation. Receipt state is never delivery proof.
	ledgeredIntentRepairs atomic.Uint64
	// Active-reconciliation diagnostics are sampled outside /metrics and
	// deliberately unlabelled: records examined, bad evidence, failed passes.
	ledgeredReconcileScanned          atomic.Uint64
	ledgeredReconcileEvidenceFailures atomic.Uint64
	ledgeredReconcileErrors           atomic.Uint64
	hotCommitRecovered                atomic.Uint64
	hotCommitRecoveryFailures         atomic.Uint64
	// ledgeredPublishFailures counts post-ledger publication attempts this
	// process could not complete: a broker refusal (including the bounded
	// ledgeredPublishTimeout and any caller cancellation surfaced through it),
	// an event clone failure, or a delivered publication whose durable
	// acknowledgement could not be persisted. The intent always survives in
	// Snapshot.LedgeredOutbox, so the immutable ledger is unaffected — this is
	// projection staleness, not evidence loss. Deliberately unlabelled: the
	// three causes have three distinct fixes but already three distinct log
	// lines (ledgered publish failed / ledgered publish clone failed /
	// ledgered outbox acknowledgement failed), so a reason, broker or topic
	// label would multiply cardinality for no operator gain.
	// Exposed as audit_ledgered_publish_failures_total.
	ledgeredPublishFailures atomic.Uint64
	// ledgeredPublishSuccesses counts post-ledger publications this process
	// both delivered to the broker and removed from Snapshot.LedgeredOutbox,
	// over the ingest request path AND the background flush round. It is
	// deliberately not ledgeredRequestAcks: that counter excludes the
	// flusher's own acknowledgements because it doubles as the flush round's
	// staleness predicate (acksAtSnapshot), so exporting it here would report
	// ~0 exactly while the flusher drains a post-outage backlog.
	// Exposed as audit_ledgered_publish_success_total.
	ledgeredPublishSuccesses atomic.Uint64
	// ledgeredOutboxPending is the durable-intent count observed by the most
	// recent completed flush round, sampled out of band — never on the
	// /metrics request path, never while holding store.Store.mu, never issuing
	// a statement — and before the round's batch truncation, so the value is
	// the backlog and not this round's batch size.
	// Exposed as audit_ledgered_outbox_pending.
	ledgeredOutboxPending atomic.Int64
	// ledgeredOutboxSampled reports whether at least one flush round has
	// completed a successful snapshot read. It is what lets the renderer omit
	// the gauge instead of fabricating 0 before any sample exists.
	ledgeredOutboxSampled atomic.Bool
}

type quotaWindow struct {
	Started time.Time
	Count   int
}

type IntegrityResult struct {
	Valid        bool      `json:"valid"`
	TenantID     string    `json:"tenant_id"`
	StreamID     string    `json:"stream_id,omitempty"`
	EventCount   int       `json:"event_count"`
	SegmentCount int       `json:"segment_count"`
	CheckedAt    time.Time `json:"checked_at"`
	Errors       []string  `json:"errors,omitempty"`
}

// New validates and resolves the service configuration, then returns the
// service. It never dereferences st (the store is opened by the caller), so
// configuration-only callers such as -check-config can pass nil.
func New(st *store.Store, cfg Config) (*Service, error) {
	if cfg.ServerVersion == "" {
		cfg.ServerVersion = "audit-governance/dev"
	}
	if cfg.SegmentSize <= 0 {
		cfg.SegmentSize = 100
	}
	if cfg.MaxEventBytes <= 0 {
		cfg.MaxEventBytes = domain.MaxEventBytes
	}
	if cfg.AggregateCheckpointRetention <= 0 {
		cfg.AggregateCheckpointRetention = DefaultAggregateCheckpointRetention
	}
	if cfg.StuckExportAge <= 0 {
		cfg.StuckExportAge = DefaultStuckExportAge
	}
	resolved, err := resolveAdminTrailConfig(cfg)
	if err != nil {
		return nil, err
	}
	cfg = resolved
	if err := resolveSecrets(&cfg); err != nil {
		return nil, err
	}
	if cfg.Signer == nil {
		cfg.Signer = hmacSigner{secret: cfg.SigningSecret}
	}
	if cfg.Archive == nil {
		cfg.Archive = &archive.FileStore{Dir: cfg.ArchiveDir}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Service{Store: st, Config: cfg, quotas: map[string]quotaWindow{}}, nil
}

// resolveSecrets fills the development defaults when explicitly allowed and
// otherwise enforces the production fail-fast rules: missing secrets are
// rejected, well-known defaults are rejected, the signing and encryption
// secrets must be distinct and each configured value must meet the
// security.MinConfiguredSecretBytes byte minimum. The order matters: a
// missing secret is reported before a default/equality/length problem, and
// validation completes before any key material is derived (hmacSigner) or any
// external client is built.
func resolveSecrets(cfg *Config) error {
	if cfg.SigningSecret == "" {
		if !cfg.AllowDevSecrets {
			return fmt.Errorf("%w: %s is empty; set %s and %s to strong distinct values, or enable development mode with %s=true", ErrMissingSecret, "AUDIT_SIGNING_SECRET", "AUDIT_SIGNING_SECRET", "AUDIT_ENCRYPTION_KEY", "AUDIT_ALLOW_DEV_SECRETS")
		}
		cfg.SigningSecret = devSigningSecret
	}
	if cfg.EncryptionKey == "" {
		if !cfg.AllowDevSecrets {
			return fmt.Errorf("%w: %s is empty; set %s and %s to strong distinct values, or enable development mode with %s=true", ErrMissingSecret, "AUDIT_ENCRYPTION_KEY", "AUDIT_SIGNING_SECRET", "AUDIT_ENCRYPTION_KEY", "AUDIT_ALLOW_DEV_SECRETS")
		}
		cfg.EncryptionKey = cfg.SigningSecret
	}
	if cfg.AllowDevSecrets {
		return nil
	}
	if isKnownDefaultSecret(cfg.SigningSecret) {
		return fmt.Errorf("%w: %s is a well-known default value that must never be used outside development mode; set a strong secret or enable %s=true", ErrDefaultSecret, "AUDIT_SIGNING_SECRET", "AUDIT_ALLOW_DEV_SECRETS")
	}
	if isKnownDefaultSecret(cfg.EncryptionKey) {
		return fmt.Errorf("%w: %s is a well-known default value that must never be used outside development mode; set a strong secret or enable %s=true", ErrDefaultSecret, "AUDIT_ENCRYPTION_KEY", "AUDIT_ALLOW_DEV_SECRETS")
	}
	if cfg.SigningSecret == cfg.EncryptionKey {
		return fmt.Errorf("%w: %s and %s must be distinct values", ErrSharedSecret, "AUDIT_SIGNING_SECRET", "AUDIT_ENCRYPTION_KEY")
	}
	if err := validateSecretLength("AUDIT_SIGNING_SECRET", cfg.SigningSecret); err != nil {
		return err
	}
	return validateSecretLength("AUDIT_ENCRYPTION_KEY", cfg.EncryptionKey)
}

func validateSecretLength(envName, value string) error {
	if len([]byte(value)) < security.MinConfiguredSecretBytes {
		return fmt.Errorf("%w: %s must be at least %d bytes", ErrWeakSecret, envName, security.MinConfiguredSecretBytes)
	}
	return nil
}

func (s *Service) Now() time.Time { return s.Config.Now().UTC() }

// adminAction builds a self-audit record. Callers append it inside the same
// Store.Update closure as the audited mutation so the audit fact cannot be
// lost when the mutation commits.
func (s *Service) adminAction(tenantID, actor, action, targetType, targetID, detail string) domain.AdminAction {
	return domain.AdminAction{ID: newID("admin"), TenantID: tenantID, Actor: actor, Action: action, TargetType: targetType, TargetID: targetID, Detail: detail, CreatedAt: s.Now()}
}

func (s *Service) CreateTenant(ctx context.Context, actor string, tenant domain.Tenant) error {
	if strings.TrimSpace(tenant.ID) == "" || strings.TrimSpace(tenant.Name) == "" {
		return fmt.Errorf("%w: tenant id and name are required", domain.ErrInvalid)
	}
	// Key-framing validation before any mutation: a tenant ID containing the
	// key separator (or other control/whitespace/path characters) would make
	// its composite keys ambiguous with another tenant's keys
	// (StreamKey("a\x1fb","s") == StreamKey("a","b\x1fs")). Reject before
	// the snapshot closure so no tenant, admin action or conflict check runs
	// for invalid IDs.
	if err := store.ValidTenantID(tenant.ID); err != nil {
		return err
	}
	if tenant.CreatedAt.IsZero() {
		tenant.CreatedAt = s.Now()
	}
	if !tenant.Active {
		tenant.Active = true
	}
	return s.Store.Update(ctx, func(data *store.Snapshot) error {
		if _, exists := data.Tenants[tenant.ID]; exists {
			return fmt.Errorf("%w: tenant already exists", domain.ErrConflict)
		}
		data.Tenants[tenant.ID] = tenant
		s.appendAdminAction(data, s.adminAction(tenant.ID, actor, domain.AdminActionTenantCreated, "tenant", tenant.ID, ""))
		return nil
	})
}

func (s *Service) ListTenants(ctx context.Context) ([]domain.Tenant, error) {
	var result []domain.Tenant
	// Catalog observation owns control only. Keep capture, ordering and the
	// final withdrawal check inside one budget, outside any Store closure.
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		if err := s.Store.ReadControl(opCtx, func(data *store.Snapshot) error {
			for _, tenant := range data.Tenants {
				result = append(result, tenant)
			}
			return nil
		}); err != nil {
			return err
		}
		sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
		return opCtx.Err()
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) RegisterSchema(ctx context.Context, actor string, schema domain.EventSchema) error {
	if err := requireTenantScope(schema.TenantID); err != nil {
		return err
	}
	if schema.TenantID == "" {
		return fmt.Errorf("%w: schema tenant_id is required", domain.ErrInvalid)
	}
	if schema.SchemaID == "" || schema.Version <= 0 || schema.EventType == "" {
		return fmt.Errorf("%w: schema_id, version and event_type are required", domain.ErrInvalid)
	}
	// Key-framing charset rule: schema.SchemaID becomes a SchemaKey component,
	// so an embedded KeySeparator (0x1F) would create a multi-separator key
	// that SplitTenantKey fail-closes on (invisible to VerifyIntegrity's
	// schema lookups). Reject before the snapshot closure: no schema key and
	// no admin action for invalid IDs.
	if err := domain.ValidKeyComponent("schema id", schema.SchemaID); err != nil {
		return err
	}
	// Control-plane resource caps (arity + per-entry bytes) on the four field
	// lists. Like the key-framing check above, this runs before the snapshot
	// closure: an over-cap registration writes no Schemas key and no
	// schema.created admin action. The in-closure version-monotonicity and
	// required/allowed superset rules are untouched.
	if err := domain.ValidateSchemaCaps(schema); err != nil {
		return err
	}
	if schema.CreatedAt.IsZero() {
		schema.CreatedAt = s.Now()
	}
	if !schema.Active {
		schema.Active = true
	}
	// Definitions and their facts only change control. Re-run all evolution
	// checks on fresh control after a Save conflict; never restore a captured
	// whole snapshot over current export authority or replace v2 evidence.
	return s.Store.UpdateControl(ctx, func(data *store.Snapshot) error {
		if _, ok := data.Tenants[schema.TenantID]; !ok {
			return fmt.Errorf("%w: tenant does not exist", domain.ErrNotFound)
		}
		// Version-rollback guard: versions for a (tenant_id, schema_id) pair
		// must be strictly monotonic. Re-registering a version below the
		// highest registered version would silently re-admit payload shapes
		// a newer schema deliberately restricted, and because validateEvent
		// and reconstructAndDerive look schemas up version-exactly, the
		// weakening would survive VerifyIntegrity. The guard runs inside the
		// same control CAS closure as the write (atomic check-and-commit,
		// no TOCTOU window) and precedes the duplicate-key check so a
		// rollback re-register of an existing version reports ErrInvalid
		// instead of ErrConflict; re-registering the exact highest version
		// remains a duplicate conflict.
		var maxVersion int
		for _, candidate := range data.Schemas {
			if candidate.TenantID == schema.TenantID && candidate.SchemaID == schema.SchemaID && candidate.Version > maxVersion {
				maxVersion = candidate.Version
			}
		}
		if schema.Version < maxVersion {
			return fmt.Errorf("%w: schema version %d is lower than the highest registered version %d for schema %s; version rollback is not allowed", domain.ErrInvalid, schema.Version, maxVersion, schema.SchemaID)
		}
		key := store.SchemaKey(schema.TenantID, schema.SchemaID, schema.Version)
		if _, exists := data.Schemas[key]; exists {
			return fmt.Errorf("%w: schema already exists", domain.ErrConflict)
		}
		var previous *domain.EventSchema
		for _, candidate := range data.Schemas {
			if candidate.TenantID == schema.TenantID && candidate.SchemaID == schema.SchemaID && candidate.Version < schema.Version && (previous == nil || candidate.Version > previous.Version) {
				copyCandidate := candidate
				previous = &copyCandidate
			}
		}
		if previous != nil {
			allowed := map[string]bool{}
			for _, field := range schema.AllowedFields {
				allowed[field] = true
			}
			for _, field := range previous.RequiredFields {
				if !containsString(schema.RequiredFields, field) {
					return fmt.Errorf("%w: schema version removes required field %s", domain.ErrInvalid, field)
				}
			}
			if len(previous.AllowedFields) > 0 && len(schema.AllowedFields) > 0 {
				for _, field := range previous.AllowedFields {
					if !allowed[field] {
						return fmt.Errorf("%w: schema version removes allowed field %s", domain.ErrInvalid, field)
					}
				}
			}
		}
		data.Schemas[key] = schema
		s.appendAdminAction(data, s.adminAction(schema.TenantID, actor, domain.AdminActionSchemaCreated, "event_schema", schema.SchemaID, fmt.Sprintf("version %d", schema.Version)))
		return nil
	})
}

func (s *Service) ListSchemas(ctx context.Context, tenantID string) ([]domain.EventSchema, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return nil, err
	}
	var result []domain.EventSchema
	// Reuse the control clone's owned field slices without materializing
	// tenant evidence or restarting the budget before the final check.
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		if err := s.Store.ReadControl(opCtx, func(data *store.Snapshot) error {
			for _, schema := range data.Schemas {
				if schema.TenantID == tenantID {
					result = append(result, schema)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		sort.Slice(result, func(i, j int) bool {
			if result[i].SchemaID == result[j].SchemaID {
				return result[i].Version < result[j].Version
			}
			return result[i].SchemaID < result[j].SchemaID
		})
		return opCtx.Err()
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) SetRetentionPolicy(ctx context.Context, actor string, policy domain.RetentionPolicy) error {
	if err := requireTenantScope(policy.TenantID); err != nil {
		return err
	}
	if policy.TenantID == "" || strings.TrimSpace(policy.RetentionClass) == "" ||
		policy.ArchiveDays < 0 || policy.HotDays < 0 || policy.WarmDays < 0 {
		return fmt.Errorf("%w: invalid retention policy", domain.ErrInvalid)
	}
	// Configuration and its fact only change control. Recheck the tenant on
	// fresh control after every Save conflict instead of restoring an old
	// whole snapshot over current export terminal/lease/stage or Hold state.
	return s.Store.UpdateControl(ctx, func(data *store.Snapshot) error {
		if _, ok := data.Tenants[policy.TenantID]; !ok {
			return fmt.Errorf("%w: tenant does not exist", domain.ErrNotFound)
		}
		data.Policies[policy.TenantID] = policy
		s.appendAdminAction(data, s.adminAction(policy.TenantID, actor, domain.AdminActionRetentionPolicySet, "retention_policy", policy.RetentionClass, fmt.Sprintf("hot=%d warm=%d archive=%d", policy.HotDays, policy.WarmDays, policy.ArchiveDays)))
		return nil
	})
}

func (s *Service) GetRetentionPolicy(ctx context.Context, tenantID string) (domain.RetentionPolicy, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.RetentionPolicy{}, err
	}
	var result domain.RetentionPolicy
	// Configuration observation owns control only, not tenant evidence.
	// Preserve the original read budget through the post-capture withdrawal
	// check; a withdrawn read must not expose an otherwise valid policy.
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		if err := s.Store.ReadControl(opCtx, func(data *store.Snapshot) error {
			policy, ok := data.Policies[tenantID]
			if !ok {
				return domain.ErrNotFound
			}
			result = policy
			return nil
		}); err != nil {
			return err
		}
		return opCtx.Err()
	})
	if err != nil {
		return domain.RetentionPolicy{}, err
	}
	return result, nil
}

func (s *Service) EvaluateRetention(ctx context.Context, tenantID string, now time.Time) (domain.RetentionReport, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.RetentionReport{}, err
	}
	// The policy is control-plane configuration, so this observation stays
	// control-only. It deliberately keeps EvaluateRetention's original
	// withdrawal semantics: the worker's between-tenant abort contract still
	// attempts the in-flight tenant's full step sequence, so a cancelled
	// caller keeps its existing evaluation path instead of inheriting the
	// public GetRetentionPolicy pre-cancel gate. The second read below is
	// unchanged and still sees caller cancellation.
	var policy domain.RetentionPolicy
	if err := s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
		current, ok := data.Policies[tenantID]
		if !ok {
			return domain.ErrNotFound
		}
		policy = current
		return nil
	}); err != nil {
		return domain.RetentionReport{}, err
	}
	if now.IsZero() {
		now = s.Now()
	}
	report := domain.RetentionReport{TenantID: tenantID, EvaluatedAt: now.UTC(), Policy: policy, Action: "archive_only_immutable_ledger"}
	cutoff := now.Add(-time.Duration(policy.ArchiveDays) * 24 * time.Hour)
	var holds []domain.LegalHold
	if err := s.Store.Read(ctx, func(data *store.Snapshot) error {
		schemas := map[string]domain.EventSchema{}
		for key, schema := range data.Schemas {
			schemas[key] = schema
		}
		for _, hold := range data.LegalHolds {
			if hold.TenantID == tenantID && hold.ReleasedAt == nil {
				holds = append(holds, hold)
			}
		}
		for _, event := range data.Events {
			if event.TenantID != tenantID || event.RetentionClass != policy.RetentionClass ||
				policy.ArchiveDays <= 0 || event.OccurredAt.After(cutoff) {
				continue
			}
			protected := false
			for _, hold := range holds {
				if s.holdMatchesEvent(hold, event, schemas) {
					protected = true
					report.HoldIDs = appendUnique(report.HoldIDs, hold.ID)
				}
			}
			if protected {
				report.ProtectedEvents++
			} else {
				report.EligibleEvents++
			}
		}
		return nil
	}); err != nil {
		return domain.RetentionReport{}, err
	}
	sort.Strings(report.HoldIDs)
	return report, nil
}

func (s *Service) Ingest(ctx context.Context, tenantID string, principal domain.IngestPrincipal, event domain.Event, waitFor string) (domain.EventReceipt, error) {
	if waitFor != "" && waitFor != domain.StatusAccepted && waitFor != domain.StatusLedgered && waitFor != domain.StatusIndexed && waitFor != domain.StatusArchived {
		return domain.EventReceipt{}, fmt.Errorf("%w: unsupported wait_for value", domain.ErrInvalid)
	}
	// F1: a commit that is ALREADY durable is never rolled back by caller
	// cancellation. The ledger commit itself runs on ctx, so a context
	// cancelled before the commit aborts the ingest promptly and records
	// nothing; only the post-commit receipt transition runs on commitCtx,
	// because failing that write can never undo the ledgered event — it can
	// only leave the durable receipt at `ledgered`, which the worker
	// converges. Cancellation stays bounded either way, by the store's own
	// operation deadline. The store itself never strips cancellation:
	// detachment is this caller's decision, taken once, at the F1 boundary.
	commitCtx := context.WithoutCancel(ctx)
	clonedEvent, err := domain.CloneEvent(event)
	if err != nil {
		return domain.EventReceipt{}, err
	}
	event = clonedEvent
	if err := event.ValidateBasic(); err != nil {
		return domain.EventReceipt{}, err
	}
	tenantHint := tenantID
	// A supplied hint is already an effective tenant selector for the
	// application call. Validate it before consulting the store; the empty
	// hint remains the deliberate compatibility path for source-registration
	// resolution used by non-tenant-bound producers.
	if tenantHint != "" {
		if err := requireTenantScope(tenantHint); err != nil {
			return domain.EventReceipt{}, err
		}
	}
	resolvedTenant, err := s.resolveIngestTenant(ctx, tenantHint, principal.ClientID, event.SourceSystem)
	if err != nil {
		return domain.EventReceipt{}, err
	}
	tenantID = resolvedTenant
	if err := requireTenantScope(tenantID); err != nil {
		return domain.EventReceipt{}, err
	}
	// Tenant consistency (DS-08): the envelope may carry a tenant_id, but it
	// must be the tenant resolved server-side from the authenticated client
	// identity — never a different one. The resolved tenant is authoritative;
	// a non-empty mismatched envelope tenant is rejected (422) instead of
	// being silently re-labelled, so a writer cannot attribute events to a
	// tenant it has no authority for. An empty envelope tenant is derived
	// from the client/source registration below and stamped by the server.
	if event.TenantID != "" && event.TenantID != tenantID {
		return domain.EventReceipt{}, fmt.Errorf("%w: envelope tenant_id %q does not match the tenant resolved from the authenticated client (%q)", domain.ErrTenantMismatch, event.TenantID, tenantID)
	}
	// Stream consistency (this direction): the envelope may carry a stream_id,
	// but the ledger stream is server-derived from tenant + aggregate/operation/
	// source (Event.Stream). A client-supplied value is stripped here so it can
	// never participate in stream resolution, StreamKey, storage, hashing,
	// segment sealing or archive keys. stream_id is a read/query filter only.
	event.StreamID = ""
	if err := s.checkTenantAndQuota(ctx, tenantID); err != nil {
		return domain.EventReceipt{}, err
	}
	event.TenantID = tenantID
	event.ReceivedAt = s.Now()
	event.ServerVersion = s.Config.ServerVersion
	event.SourceDigest = ""
	sourceDigest, err := domain.EventContentDigest(event)
	if err != nil {
		return domain.EventReceipt{}, err
	}
	event.SourceDigest = sourceDigest
	schema, err := s.validateEvent(ctx, tenantID, principal, event)
	if err != nil {
		return domain.EventReceipt{}, err
	}
	if err := s.protectSensitiveFields(&event, schema); err != nil {
		return domain.EventReceipt{}, err
	}
	inputDigest := sourceDigest
	var receipt domain.EventReceipt
	// sealedSegments accumulates segments sealed by THIS successful Update
	// attempt. Store.Update may re-invoke the closure on a CAS conflict, and
	// a failed attempt's mutations are discarded — so the slice is truncated
	// at the start of every invocation, never carrying segments from a
	// superseded snapshot (phantom segments would be archived without ever
	// being part of the ledger).
	var sealedSegments []domain.Segment
	committedEvent := event
	if s.Store.HotCold(ctx) {
		committedEvent, receipt, sealedSegments, err = s.ingestTenant(ctx, tenantHint, principal, tenantID, event, inputDigest)
	} else {
		err = s.Store.Update(commitCtx, func(data *store.Snapshot) error {
			sealedSegments = sealedSegments[:0]
			commitTenant, accessErr := resolveIngestTenantFromData(data, tenantHint, principal.ClientID, event.SourceSystem)
			if accessErr != nil || commitTenant != tenantID {
				return sourceAccessError()
			}
			key := store.EventKey(tenantID, event.EventID)
			if existingReceipt, found, existingErr := s.checkExistingIngest(ctx, data, tenantID, event.EventID, inputDigest); found {
				receipt = existingReceipt
				return existingErr
			}
			// Idempotency keys are tenant-scoped and must not silently be reused
			// for another event. This mirrors the control-plane uniqueness
			// constraint and protects the file-backed reference implementation.
			if idempotencyKeyReused(data, tenantID, key, event.IdempotencyKey) {
				// The colliding (rejected) caller observes a freshly built,
				// self-describing conflict receipt. The pre-existing owner
				// event's stored receipt is intentionally left untouched (R1/R3):
				// we never read or write it here, so a successful ingest can
				// never be corrupted by a later idempotency-key collision.
				receipt = conflictReceipt(tenantID, event)
				return fmt.Errorf("%w: idempotency_key is already associated with another event", domain.ErrConflict)
			}

			now := event.ReceivedAt
			receipt = domain.EventReceipt{EventID: event.EventID, TenantID: tenantID, IdempotencyKey: event.IdempotencyKey, Status: domain.StatusAccepted, AcceptedAt: now}
			streamID := event.Stream()
			streamKey := store.StreamKey(tenantID, streamID)
			stream := data.Streams[streamKey]
			if stream.StreamID == "" {
				stream = store.StreamState{TenantID: tenantID, StreamID: streamID, NextSequence: 1}
			}
			event.StreamID = streamID
			event.Sequence = stream.NextSequence
			event.PrevHash = stream.HeadHash
			event.Hash, err = s.eventHash(event)
			if err != nil {
				return err
			}
			stream.NextSequence++
			stream.HeadHash = event.Hash
			stream.PendingHashes = append(stream.PendingHashes, event.Hash)
			stream.PendingEvents = append(stream.PendingEvents, key)
			if len(stream.PendingHashes) == 1 {
				stream.PendingPrevHash = event.PrevHash
			}
			data.Streams[streamKey] = stream
			data.Events[key] = event
			if s.Config.LedgeredPublisher != nil {
				if data.LedgeredOutbox == nil {
					data.LedgeredOutbox = map[string]domain.Event{}
				}
				data.LedgeredOutbox[key] = event
			}
			receipt.Status = domain.StatusLedgered
			receipt.LedgeredAt = now
			receipt.StreamID = event.StreamID
			receipt.Sequence = event.Sequence
			receipt.Hash = event.Hash
			data.Receipts[key] = receipt
			if len(stream.PendingHashes) >= s.Config.SegmentSize {
				segment, checkpoint, sealErr := s.sealSegment(ctx, stream, now)
				if sealErr != nil {
					return sealErr
				}
				sealedSegments = append(sealedSegments, segment)
				data.Segments[streamKey] = append(data.Segments[streamKey], segment)
				data.Checkpoints[streamKey] = append(data.Checkpoints[streamKey], checkpoint)
				stream.PendingHashes = nil
				stream.PendingEvents = nil
				stream.PendingPrevHash = ""
				data.Streams[streamKey] = stream
			}
			return nil
		})
		// The legacy snapshot closure derives the ledger coordinates in-place
		// on event. Preserve those committed coordinates for the post-commit
		// archive write; otherwise the archive key would be framed from the
		// pre-ledger zero-value stream and sequence.
		committedEvent = event
	}
	if err != nil {
		if errors.Is(err, domain.ErrConflict) {
			return receipt, err
		}
		return domain.EventReceipt{}, err
	}
	if receipt.Duplicate {
		// A duplicate must never create another ledger entry. However, a
		// caller explicitly waiting for archival cannot be told success when
		// the durable receipt is still ledgered/indexed. The archive worker can
		// converge the existing event; the caller retries the same idempotent
		// request rather than creating a new event.
		if waitFor == domain.StatusArchived && archive.Configured(s.Config.Archive) && receipt.Status != domain.StatusArchived {
			return receipt, fmt.Errorf("event %s is not archived; retry archival", receipt.EventID)
		}
		return receipt, nil
	}
	event = committedEvent
	s.publishLedgered(ctx, event)
	if archive.Configured(s.Config.Archive) {
		var archiveErrors []error
		if err := s.archiveEvent(ctx, event); err != nil {
			archiveErrors = append(archiveErrors, fmt.Errorf("archive event: %w", err))
		}
		for _, segment := range sealedSegments {
			if err := s.archiveSegment(ctx, segment); err != nil {
				archiveErrors = append(archiveErrors, fmt.Errorf("archive segment: %w", err))
			}
		}
		if len(archiveErrors) == 0 {
			updated, err := s.transitionReceipt(commitCtx, tenantID, event.EventID, domain.StatusArchived, true)
			if err != nil {
				// Durability boundary (store.go Update): the status write is
				// save-or-nothing, so a failed transition must not be reported.
				// Fail closed — receipt still carries the ledger-CAS-committed
				// state (StatusLedgered, zero IndexedAt/ArchivedAt), which is
				// the durable truth; the client recovers by idempotent retry
				// (503 contract). The local receipt is only mutated after the
				// transition durably commits.
				return receipt, err
			}
			receipt = updated
		} else {
			updated, err := s.transitionReceipt(commitCtx, tenantID, event.EventID, domain.StatusIndexed, false)
			if err != nil {
				return receipt, err
			}
			receipt = updated
			if waitFor == domain.StatusArchived {
				return receipt, errors.Join(archiveErrors...)
			}
		}
	} else {
		updated, err := s.transitionReceipt(commitCtx, tenantID, event.EventID, domain.StatusIndexed, false)
		if err != nil {
			return receipt, err
		}
		receipt = updated
	}
	if waitFor == "accepted" || waitFor == "" {
		response := receipt
		response.Status = domain.StatusAccepted
		response.LedgeredAt = time.Time{}
		response.IndexedAt = time.Time{}
		response.ArchivedAt = time.Time{}
		return response, nil
	}
	if waitFor == domain.StatusLedgered && receipt.Status != domain.StatusAccepted {
		return receipt, nil
	}
	if waitFor == domain.StatusIndexed && (receipt.Status == domain.StatusIndexed || receipt.Status == domain.StatusArchived) {
		return receipt, nil
	}
	if waitFor == domain.StatusArchived && receipt.Status == domain.StatusArchived {
		return receipt, nil
	}
	return receipt, nil
}

// conflictReceipt builds a self-describing conflict receipt for the colliding
// (rejected) ingest. It deliberately does NOT reference the pre-existing
// owner event's stored receipt: on an idempotency-key collision the owner's
// receipt must remain clean in the snapshot, and only the colliding caller
// observes a conflict (see direction idempotency-key-conflict-...).
func conflictReceipt(tenantID string, event domain.Event) domain.EventReceipt {
	return domain.EventReceipt{
		EventID:        event.EventID,
		TenantID:       tenantID,
		IdempotencyKey: event.IdempotencyKey,
		Conflict:       true,
		ErrorCode:      "idempotency_key_conflict",
		ErrorMessage:   "idempotency_key is already associated with another event",
	}
}

func (s *Service) GetReceipt(ctx context.Context, tenantID, actor, eventID string) (domain.EventReceipt, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.EventReceipt{}, err
	}
	var receipt domain.EventReceipt
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		// V2 uses the exact cold receipt index, not a global history snapshot.
		// Metadata reads do not require source Get or reinterpret stored status.
		if err := s.Store.ReadTenant(opCtx, tenantID, func(view *store.TenantView) error {
			value, found := view.Ledger.Receipt(store.EventKey(tenantID, eventID))
			if err := view.Ledger.Err(); err != nil {
				return err
			}
			if !found {
				return domain.ErrNotFound
			}
			if value.TenantID != tenantID || value.EventID != eventID {
				return fmt.Errorf("%w: receipt identity mismatch", domain.ErrConflict)
			}
			receipt = value
			return nil
		}); err != nil {
			return err
		}
		if err := opCtx.Err(); err != nil {
			return err
		}
		// Append-before-serve once under the lookup's original budget. A
		// durable attempt fact is not permission to serve after withdrawal.
		if err := s.recordReadAction(opCtx, tenantID, actor, domain.AdminActionEventRead, "event", eventID, "receipt"); err != nil {
			return err
		}
		return opCtx.Err()
	})
	if err != nil {
		return domain.EventReceipt{}, err
	}
	return receipt, nil
}

func (s *Service) GetEvent(ctx context.Context, tenantID, actor, eventID string) (domain.Event, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.Event{}, err
	}
	var event domain.Event
	err := s.Store.WithOperation(ctx, func(ctx context.Context) error {
		items, err := s.readQueryEvidence(ctx, tenantID, []store.EventReference{{EventID: eventID}})
		if err != nil {
			return err
		}
		event = items[0]
		if err := s.recordReadAction(ctx, tenantID, actor, domain.AdminActionEventRead, "event", eventID, ""); err != nil {
			return err
		}
		// A durable attempt fact cannot be retracted, but is not permission
		// to serve evidence after caller cancellation or the original deadline.
		return ctx.Err()
	})
	if err != nil {
		return domain.Event{}, err
	}
	return event, nil
}

// recordReadAction appends one self-audit record for a read-path fact. Reads
// have no paired mutation, so the record is appended to the separate read
// self-audit trail after the read succeeds; a failed append aborts the read
// (fail-closed: an un-auditable read must not be reported as served). Read
// facts never rewrite the control-plane snapshot (the direction's core
// defect); the trail is append-only with no Store.mu, so reads stop blocking
// ingests (design F-2).
func (s *Service) recordReadAction(ctx context.Context, tenantID, actor, action, targetType, targetID, detail string) error {
	if actor == "" {
		return nil
	}
	return s.Store.AppendAdminFact(ctx, s.adminAction(tenantID, actor, action, targetType, targetID, detail), s.Config.MaxAdminActions, s.Config.MaxAdminTrailActions)
}

func (s *Service) QueryEvents(ctx context.Context, tenantID, actor string, query domain.Query) (domain.QueryResult, error) {
	return s.queryEventsSnapshot(ctx, tenantID, actor, query)
}

func (s *Service) Operation(ctx context.Context, tenantID, actor, operationID string) (domain.OperationSummary, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.OperationSummary{}, err
	}
	if operationID == "" {
		return domain.OperationSummary{}, fmt.Errorf("%w: operation_id is required", domain.ErrInvalid)
	}
	var result domain.OperationSummary
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		// Capture owns its graph; Get and exact-schema/content/chain checks
		// run outside Store.mu, before membership or summary aggregation.
		events, err := s.readTimelineEvidence(opCtx, tenantID, func(event domain.Event) bool { return event.OperationID == operationID })
		if err != nil {
			return err
		}
		result = domain.OperationSummary{OperationID: operationID, TenantID: tenantID, EventCount: len(events), FirstAt: events[0].OccurredAt, LastAt: events[0].OccurredAt}
		set := map[string]bool{}
		for _, event := range events {
			if err := opCtx.Err(); err != nil {
				return err
			}
			if event.OccurredAt.Before(result.FirstAt) {
				result.FirstAt = event.OccurredAt
			}
			if event.OccurredAt.After(result.LastAt) {
				result.LastAt = event.OccurredAt
			}
			set[event.Outcome] = true
		}
		result.Outcomes = make([]string, 0, len(set))
		for outcome := range set {
			result.Outcomes = append(result.Outcomes, outcome)
		}
		sort.Strings(result.Outcomes)
		// Append-before-serve once, using the original operation deadline.
		// An unknown/late append is an attempt fact, never proof of delivery.
		if err := s.recordReadAction(opCtx, tenantID, actor, domain.AdminActionEventRead, "operation", operationID, "summary"); err != nil {
			return err
		}
		return opCtx.Err()
	})
	if err != nil {
		return domain.OperationSummary{}, err
	}
	return result, nil
}

func (s *Service) ReplayOperation(ctx context.Context, tenantID, actor, operationID string) (domain.ReplayResult, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.ReplayResult{}, err
	}
	var result domain.ReplayResult
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		// Authenticate committed evidence before selection or local folding.
		// Only owned snapshot capture holds Store.mu; source Get is lock-free.
		events, err := s.readTimelineEvidence(opCtx, tenantID, func(event domain.Event) bool { return event.OperationID == operationID })
		if err != nil {
			return err
		}
		if len(events) > domain.MaxTimelineEvents {
			return fmt.Errorf("%w: operation timeline exceeds %d events; use query or export", domain.ErrInvalid, domain.MaxTimelineEvents)
		}
		sortEvents(events)
		if err := opCtx.Err(); err != nil {
			return err
		}
		result = replay(events, tenantID, operationID, "")
		if err := opCtx.Err(); err != nil {
			return err
		}
		// One append-before-serve fact, never a delegated timeline fact.
		// Internal restore callers retain their explicit empty-actor policy.
		if err := s.recordReadAction(opCtx, tenantID, actor, domain.AdminActionEventRead, "operation", operationID, "replay"); err != nil {
			return err
		}
		return opCtx.Err()
	})
	if err != nil {
		return domain.ReplayResult{}, err
	}
	return result, nil
}

func (s *Service) ReplayAggregate(ctx context.Context, tenantID, actor, aggregateType, aggregateID string) (domain.ReplayResult, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.ReplayResult{}, err
	}
	var result domain.ReplayResult
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		events, err := s.aggregateTimelineNoAudit(opCtx, tenantID, aggregateType, aggregateID)
		if err != nil {
			return err
		}
		if err := opCtx.Err(); err != nil {
			return err
		}
		result = replay(events, tenantID, "", aggregateID)
		if err := opCtx.Err(); err != nil {
			return err
		}
		// D-ReplayAggregate: service-only compatibility read, exactly one
		// original fact, never an extra delegated timeline read fact.
		if err := s.recordReadAction(opCtx, tenantID, actor, domain.AdminActionEventRead, "aggregate", aggregateID, "replay"); err != nil {
			return err
		}
		return opCtx.Err()
	})
	if err != nil {
		return domain.ReplayResult{}, err
	}
	return result, nil
}

func (s *Service) checkTenantAndQuota(ctx context.Context, tenantID string) error {
	var tenant domain.Tenant
	if err := s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
		var ok bool
		tenant, ok = data.Tenants[tenantID]
		if !ok || !tenant.Active {
			return domain.ErrNotFound
		}
		return nil
	}); err != nil {
		return err
	}
	if tenant.EventsPerSecond <= 0 {
		return nil
	}
	now := s.Now()
	s.quotaMu.Lock()
	defer s.quotaMu.Unlock()
	window := s.quotas[tenantID]
	if window.Started.IsZero() || now.Sub(window.Started) >= time.Second {
		window = quotaWindow{Started: now}
	}
	limit := tenant.EventsPerSecond + tenant.Burst
	if limit <= 0 {
		limit = tenant.EventsPerSecond
	}
	if window.Count >= limit {
		s.quotas[tenantID] = window
		return domain.ErrQuotaExceeded
	}
	window.Count++
	s.quotas[tenantID] = window
	return nil
}

// safeName is a deprecated delegation shim over the injective, reversible
// fsutil.EncodeKeyComponent. The historical lossy sanitizer (every non-safe
// rune → '_', empty → "unnamed") collapsed distinct valid identifiers onto
// one WORM archive key — a cross-tenant archival denial — and is gone; all
// three production framing sites (archiveEvent, archiveSegment, export key)
// and the archive-key test helpers now route through the single encoding.
// Retained so existing tests that pin expected paths by calling safeName
// compile and stay correct. Do not add new call sites; use
// fsutil.EncodeKeyComponent directly.
func safeName(value string) string { return fsutil.EncodeKeyComponent(value) }

// ListAdminActions returns the append-only self-audit trail, newest first.
// A tenant-scoped caller only sees its own tenant's actions; a platform
// caller sees all actions (or one tenant with the optional filter).
func Marshal(value any) ([]byte, error) { return json.Marshal(value) }
