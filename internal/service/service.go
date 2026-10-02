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
	// StuckExportAge fails export jobs left "running" by a crashed or
	// restarted audit-api once they exceed this age, measured from CreatedAt
	// (the only pre-terminal timestamp). Values <= 0 select the documented
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
	// ledgeredIntentRepairs counts publication intents an interrupted tenant
	// ingest lost and an idempotent retry re-created. Each increment is direct
	// evidence that a tenant commit died between its durable cold append and
	// its control-plane write.
	ledgeredIntentRepairs atomic.Uint64
	// repairedMu guards repairedIntents, the bounded process-local set of keys
	// already repaired. It is deliberately not Store.mu: the repair predicate
	// runs inside an UpdateTenant closure, which already holds Store.mu.
	repairedMu      sync.Mutex
	repairedIntents map[string]struct{}
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
	return &Service{Store: st, Config: cfg, quotas: map[string]quotaWindow{}, repairedIntents: map[string]struct{}{}}, nil
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

func (s *Service) CreateTenant(actor string, tenant domain.Tenant) error {
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
	return s.Store.Update(func(data *store.Snapshot) error {
		if _, exists := data.Tenants[tenant.ID]; exists {
			return fmt.Errorf("%w: tenant already exists", domain.ErrConflict)
		}
		data.Tenants[tenant.ID] = tenant
		s.appendAdminAction(data, s.adminAction(tenant.ID, actor, domain.AdminActionTenantCreated, "tenant", tenant.ID, ""))
		return nil
	})
}

func (s *Service) ListTenants() ([]domain.Tenant, error) {
	var result []domain.Tenant
	err := s.Store.Read(func(data *store.Snapshot) error {
		for _, tenant := range data.Tenants {
			result = append(result, tenant)
		}
		return nil
	})
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, err
}

func (s *Service) RegisterSchema(actor string, schema domain.EventSchema) error {
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
	if schema.CreatedAt.IsZero() {
		schema.CreatedAt = s.Now()
	}
	if !schema.Active {
		schema.Active = true
	}
	return s.Store.Update(func(data *store.Snapshot) error {
		if _, ok := data.Tenants[schema.TenantID]; !ok {
			return fmt.Errorf("%w: tenant does not exist", domain.ErrNotFound)
		}
		// Version-rollback guard: versions for a (tenant_id, schema_id) pair
		// must be strictly monotonic. Re-registering a version below the
		// highest registered version would silently re-admit payload shapes
		// a newer schema deliberately restricted, and because validateEvent
		// and reconstructAndDerive look schemas up version-exactly, the
		// weakening would survive VerifyIntegrity. The guard runs inside the
		// same store.Update closure as the write (atomic check-and-commit,
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

func (s *Service) ListSchemas(tenantID string) ([]domain.EventSchema, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return nil, err
	}
	var result []domain.EventSchema
	err := s.Store.Read(func(data *store.Snapshot) error {
		for _, schema := range data.Schemas {
			if schema.TenantID == tenantID {
				result = append(result, schema)
			}
		}
		return nil
	})
	sort.Slice(result, func(i, j int) bool {
		if result[i].SchemaID == result[j].SchemaID {
			return result[i].Version < result[j].Version
		}
		return result[i].SchemaID < result[j].SchemaID
	})
	return result, err
}

func (s *Service) SetRetentionPolicy(actor string, policy domain.RetentionPolicy) error {
	if err := requireTenantScope(policy.TenantID); err != nil {
		return err
	}
	if policy.TenantID == "" || strings.TrimSpace(policy.RetentionClass) == "" ||
		policy.ArchiveDays < 0 || policy.HotDays < 0 || policy.WarmDays < 0 {
		return fmt.Errorf("%w: invalid retention policy", domain.ErrInvalid)
	}
	return s.Store.Update(func(data *store.Snapshot) error {
		if _, ok := data.Tenants[policy.TenantID]; !ok {
			return fmt.Errorf("%w: tenant does not exist", domain.ErrNotFound)
		}
		data.Policies[policy.TenantID] = policy
		s.appendAdminAction(data, s.adminAction(policy.TenantID, actor, domain.AdminActionRetentionPolicySet, "retention_policy", policy.RetentionClass, fmt.Sprintf("hot=%d warm=%d archive=%d", policy.HotDays, policy.WarmDays, policy.ArchiveDays)))
		return nil
	})
}

func (s *Service) GetRetentionPolicy(tenantID string) (domain.RetentionPolicy, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.RetentionPolicy{}, err
	}
	var result domain.RetentionPolicy
	err := s.Store.Read(func(data *store.Snapshot) error {
		policy, ok := data.Policies[tenantID]
		if !ok {
			return domain.ErrNotFound
		}
		result = policy
		return nil
	})
	return result, err
}

func (s *Service) EvaluateRetention(tenantID string, now time.Time) (domain.RetentionReport, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.RetentionReport{}, err
	}
	policy, err := s.GetRetentionPolicy(tenantID)
	if err != nil {
		return domain.RetentionReport{}, err
	}
	if now.IsZero() {
		now = s.Now()
	}
	report := domain.RetentionReport{TenantID: tenantID, EvaluatedAt: now.UTC(), Policy: policy, Action: "archive_only_immutable_ledger"}
	cutoff := now.Add(-time.Duration(policy.ArchiveDays) * 24 * time.Hour)
	var holds []domain.LegalHold
	if err := s.Store.Read(func(data *store.Snapshot) error {
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
	resolvedTenant, err := s.resolveIngestTenant(tenantHint, principal.ClientID, event.SourceSystem)
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
	if err := s.checkTenantAndQuota(tenantID); err != nil {
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
	schema, err := s.validateEvent(tenantID, principal, event)
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
	if s.Store.HotCold() {
		committedEvent, receipt, sealedSegments, err = s.ingestTenant(ctx, tenantHint, principal, tenantID, event, inputDigest)
	} else {
		err = s.Store.Update(func(data *store.Snapshot) error {
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
			updated, err := s.transitionReceipt(tenantID, event.EventID, domain.StatusArchived, true)
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
			updated, err := s.transitionReceipt(tenantID, event.EventID, domain.StatusIndexed, false)
			if err != nil {
				return receipt, err
			}
			receipt = updated
			if waitFor == domain.StatusArchived {
				return receipt, errors.Join(archiveErrors...)
			}
		}
	} else {
		updated, err := s.transitionReceipt(tenantID, event.EventID, domain.StatusIndexed, false)
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

func (s *Service) GetReceipt(tenantID, actor, eventID string) (domain.EventReceipt, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.EventReceipt{}, err
	}
	var receipt domain.EventReceipt
	err := s.Store.Read(func(data *store.Snapshot) error {
		value, ok := data.Receipts[store.EventKey(tenantID, eventID)]
		if !ok {
			return domain.ErrNotFound
		}
		receipt = value
		return nil
	})
	if err != nil {
		return domain.EventReceipt{}, err
	}
	// Read self-audit (F-06): a receipt lookup exposes event content, so it
	// appends one audit.event.read fact in the service layer (no transport
	// can bypass it); a failed append fails the read closed, mirroring
	// GetEvent/QueryEvents.
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "event", eventID, "receipt"); err != nil {
		return domain.EventReceipt{}, err
	}
	return receipt, nil
}

func (s *Service) GetEvent(tenantID, actor, eventID string) (domain.Event, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.Event{}, err
	}
	var event domain.Event
	err := s.Store.Read(func(data *store.Snapshot) error {
		var resolveErr error
		event, resolveErr = s.eventFromSnapshot(context.Background(), data, tenantID, eventID)
		return resolveErr
	})
	if err != nil {
		return domain.Event{}, err
	}
	// Read self-audit (F-06): every event read is a governance fact appended
	// to the append-only admin trail. The append happens in the service layer
	// so no transport can bypass it; a failed append fails the read closed.
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "event", eventID, ""); err != nil {
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
func (s *Service) recordReadAction(tenantID, actor, action, targetType, targetID, detail string) error {
	if actor == "" {
		return nil
	}
	return s.Store.AppendAdminFact(s.adminAction(tenantID, actor, action, targetType, targetID, detail), s.Config.MaxAdminActions, s.Config.MaxAdminTrailActions)
}

func (s *Service) QueryEvents(tenantID, actor string, query domain.Query) (domain.QueryResult, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.QueryResult{}, err
	}
	if query.From.IsZero() || query.To.IsZero() {
		return domain.QueryResult{}, fmt.Errorf("%w: from and to are required", domain.ErrInvalid)
	}
	if !query.From.Before(query.To) {
		return domain.QueryResult{}, fmt.Errorf("%w: from must be before to", domain.ErrInvalid)
	}
	if (query.PayloadField == "") != (query.PayloadDigest == "") {
		return domain.QueryResult{}, fmt.Errorf("%w: payload_field and payload_digest must be provided together", domain.ErrInvalid)
	}
	if query.PageSize <= 0 {
		query.PageSize = domain.DefaultPageSize
	}
	if query.PageSize > domain.MaxPageSize {
		return domain.QueryResult{}, fmt.Errorf("%w: page_size exceeds %d", domain.ErrInvalid, domain.MaxPageSize)
	}
	var events []domain.Event
	schemas := map[string]domain.EventSchema{}
	err := s.Store.Read(func(data *store.Snapshot) error {
		for key, schema := range data.Schemas {
			schemas[key] = schema
		}
		var resolveErr error
		events, resolveErr = s.eventsFromSnapshot(context.Background(), data, tenantID, func(event domain.Event) bool {
			return s.matches(event, query, schemas)
		})
		return resolveErr
	})
	if err != nil {
		return domain.QueryResult{}, err
	}
	sort.Slice(events, func(i, j int) bool { return compareEvents(events[i], events[j]) })
	if query.Cursor != "" {
		cur, err := domain.DecodeCursor(query.Cursor)
		if err != nil {
			return domain.QueryResult{}, err
		}
		if cur.Legacy {
			return domain.QueryResult{}, fmt.Errorf("%w: cursor predates chronological ordering; re-run the query", domain.ErrInvalid)
		}
		filtered := events[:0]
		for _, event := range events {
			if event.OccurredAt.After(cur.OccurredAt) ||
				(event.OccurredAt.Equal(cur.OccurredAt) &&
					(event.Sequence > cur.Sequence ||
						(event.Sequence == cur.Sequence && event.EventID > cur.EventID))) {
				filtered = append(filtered, event)
			}
		}
		events = filtered
	}
	result := domain.QueryResult{Count: len(events)}
	if len(events) > query.PageSize {
		last := events[query.PageSize-1]
		result.Items = events[:query.PageSize]
		result.NextCursor = domain.EncodeCursor(domain.Cursor{OccurredAt: last.OccurredAt, Sequence: last.Sequence, EventID: last.EventID})
	} else {
		result.Items = events
	}
	// Read self-audit (F-06): the query itself is a governance fact. The
	// append happens in the service layer (every transport shares this
	// method); a failed append fails the query closed.
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "query", "", fmt.Sprintf("from=%s to=%s", query.From.Format(time.RFC3339), query.To.Format(time.RFC3339))); err != nil {
		return domain.QueryResult{}, err
	}
	return result, nil
}

func (s *Service) Operation(tenantID, actor, operationID string) (domain.OperationSummary, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.OperationSummary{}, err
	}
	if operationID == "" {
		return domain.OperationSummary{}, fmt.Errorf("%w: operation_id is required", domain.ErrInvalid)
	}
	events, err := s.eventsFor(tenantID, func(event domain.Event) bool { return event.OperationID == operationID })
	if err != nil {
		return domain.OperationSummary{}, err
	}
	if len(events) == 0 {
		return domain.OperationSummary{}, domain.ErrNotFound
	}
	sortEvents(events)
	set := map[string]bool{}
	for _, event := range events {
		set[event.Outcome] = true
	}
	outcomes := make([]string, 0, len(set))
	for outcome := range set {
		outcomes = append(outcomes, outcome)
	}
	sort.Strings(outcomes)
	// Read self-audit (F-06): an operation summary exposes event content, so
	// it appends one audit.event.read fact in the service layer (no transport
	// can bypass it), fail-closed like GetEvent/QueryEvents. Encoding matches
	// the sibling operation facts: (operation, id, timeline|replay|summary).
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "operation", operationID, "summary"); err != nil {
		return domain.OperationSummary{}, err
	}
	return domain.OperationSummary{OperationID: operationID, TenantID: tenantID, EventCount: len(events), FirstAt: events[0].OccurredAt, LastAt: events[len(events)-1].OccurredAt, Outcomes: outcomes}, nil
}

func (s *Service) ReplayOperation(tenantID, actor, operationID string) (domain.ReplayResult, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.ReplayResult{}, err
	}
	events, err := s.operationTimelineNoAudit(tenantID, operationID)
	if err != nil {
		return domain.ReplayResult{}, err
	}
	result := replay(events, tenantID, operationID, "")
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "operation", operationID, "replay"); err != nil {
		return domain.ReplayResult{}, err
	}
	return result, nil
}

func (s *Service) ReplayAggregate(tenantID, actor, aggregateType, aggregateID string) (domain.ReplayResult, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.ReplayResult{}, err
	}
	events, err := s.aggregateTimelineNoAudit(tenantID, aggregateType, aggregateID)
	if err != nil {
		return domain.ReplayResult{}, err
	}
	result := replay(events, tenantID, "", aggregateID)
	// D-ReplayAggregate: zero production callers today; appends exactly one
	// fact so the "every event-content read leaves exactly one fact"
	// invariant holds for this method too (pinned by T-D1).
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "aggregate", aggregateID, "replay"); err != nil {
		return domain.ReplayResult{}, err
	}
	return result, nil
}

func (s *Service) checkTenantAndQuota(tenantID string) error {
	var tenant domain.Tenant
	if err := s.Store.ReadControl(func(data *store.Snapshot) error {
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
