package service

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
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
// rejected, well-known defaults are rejected and the signing and encryption
// secrets must be distinct. The order matters: a missing secret is reported
// before a default/equality problem, and validation completes before any
// key material is derived (hmacSigner) or any external client is built.
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
		data.AdminActions = append(data.AdminActions, s.adminAction(tenant.ID, actor, domain.AdminActionTenantCreated, "tenant", tenant.ID, ""))
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
		data.AdminActions = append(data.AdminActions, s.adminAction(schema.TenantID, actor, domain.AdminActionSchemaCreated, "event_schema", schema.SchemaID, fmt.Sprintf("version %d", schema.Version)))
		return nil
	})
}

func (s *Service) ListSchemas(tenantID string) ([]domain.EventSchema, error) {
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
	if policy.TenantID == "" || strings.TrimSpace(policy.RetentionClass) == "" ||
		policy.ArchiveDays < 0 || policy.HotDays < 0 || policy.WarmDays < 0 {
		return fmt.Errorf("%w: invalid retention policy", domain.ErrInvalid)
	}
	return s.Store.Update(func(data *store.Snapshot) error {
		if _, ok := data.Tenants[policy.TenantID]; !ok {
			return fmt.Errorf("%w: tenant does not exist", domain.ErrNotFound)
		}
		data.Policies[policy.TenantID] = policy
		data.AdminActions = append(data.AdminActions, s.adminAction(policy.TenantID, actor, domain.AdminActionRetentionPolicySet, "retention_policy", policy.RetentionClass, fmt.Sprintf("hot=%d warm=%d archive=%d", policy.HotDays, policy.WarmDays, policy.ArchiveDays)))
		return nil
	})
}

func (s *Service) GetRetentionPolicy(tenantID string) (domain.RetentionPolicy, error) {
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
	if err := event.ValidateBasic(); err != nil {
		return domain.EventReceipt{}, err
	}
	tenantHint := tenantID
	resolvedTenant, err := s.resolveIngestTenant(tenantHint, principal.ClientID, event.SourceSystem)
	if err != nil {
		return domain.EventReceipt{}, err
	}
	tenantID = resolvedTenant
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
	if event.Payload != nil {
		copyPayload, cloneErr := clonePayload(event.Payload)
		if cloneErr != nil {
			return domain.EventReceipt{}, cloneErr
		}
		event.Payload = copyPayload
	}
	event.SourceDigest = ""
	sourceDigest, err := domain.EventDigest(event)
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
	err = s.Store.Update(func(data *store.Snapshot) error {
		sealedSegments = sealedSegments[:0]
		commitTenant, accessErr := resolveIngestTenantFromData(data, tenantHint, principal.ClientID, event.SourceSystem)
		if accessErr != nil || commitTenant != tenantID {
			return sourceAccessError()
		}
		key := store.EventKey(tenantID, event.EventID)
		if existing, ok := data.Events[key]; ok {
			existingDigest, digestErr := s.reconstructAndDerive(existing, data.Schemas)
			if digestErr != nil {
				// Fallback: legacy stored-digest comparison. Reconstruction needs
				// the event's exact schema version and the current encryption
				// key; when either is unavailable (deregistered schema, rotated
				// key) we preserve historical idempotency behavior instead of
				// breaking re-ingest.
				existingDigest, digestErr = domain.EventDigest(existing)
				if digestErr != nil {
					return digestErr
				}
			}
			receipt = data.Receipts[key]
			if existingDigest == inputDigest {
				receipt.Duplicate = true
				data.Receipts[key] = receipt
				return nil
			}
			receipt.Conflict = true
			receipt.ErrorCode = "event_id_content_conflict"
			receipt.ErrorMessage = "event_id already exists with different canonical content"
			data.Receipts[key] = receipt
			return fmt.Errorf("%w: event_id content differs", domain.ErrConflict)
		}
		// Idempotency keys are tenant-scoped and must not silently be reused
		// for another event. This mirrors the control-plane uniqueness
		// constraint and protects the file-backed reference implementation.
		for existingKey, existing := range data.Events {
			if existing.TenantID != tenantID || existing.IdempotencyKey != event.IdempotencyKey || existingKey == key {
				continue
			}
			receipt = data.Receipts[existingKey]
			receipt.Conflict = true
			receipt.ErrorCode = "idempotency_key_conflict"
			receipt.ErrorMessage = "idempotency_key is already associated with another event"
			data.Receipts[existingKey] = receipt
			return fmt.Errorf("%w: idempotency_key is already associated with another event", domain.ErrConflict)
		}

		now := event.ReceivedAt
		receipt = domain.EventReceipt{EventID: event.EventID, TenantID: tenantID, Status: domain.StatusAccepted, AcceptedAt: now}
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
	if err != nil {
		if errors.Is(err, domain.ErrConflict) {
			return receipt, err
		}
		return domain.EventReceipt{}, err
	}
	if receipt.Duplicate {
		return receipt, nil
	}
	if archive.Configured(s.Config.Archive) {
		archived := s.archiveEvent(event) == nil
		for _, segment := range sealedSegments {
			if s.archiveSegment(segment) != nil {
				archived = false
			}
		}
		if archived {
			_ = s.Store.Update(func(data *store.Snapshot) error {
				key := store.EventKey(tenantID, event.EventID)
				r := data.Receipts[key]
				r.IndexedAt = s.Now()
				r.Status = domain.StatusArchived
				r.ArchivedAt = s.Now()
				data.Receipts[key] = r
				return nil
			})
			receipt.Status = domain.StatusArchived
			receipt.IndexedAt = s.Now()
			receipt.ArchivedAt = s.Now()
		} else {
			_ = s.Store.Update(func(data *store.Snapshot) error {
				key := store.EventKey(tenantID, event.EventID)
				r := data.Receipts[key]
				r.Status = domain.StatusIndexed
				r.IndexedAt = s.Now()
				data.Receipts[key] = r
				return nil
			})
			receipt.Status = domain.StatusIndexed
			receipt.IndexedAt = s.Now()
		}
	} else {
		_ = s.Store.Update(func(data *store.Snapshot) error {
			key := store.EventKey(tenantID, event.EventID)
			r := data.Receipts[key]
			r.Status = domain.StatusIndexed
			r.IndexedAt = s.Now()
			data.Receipts[key] = r
			return nil
		})
		receipt.Status = domain.StatusIndexed
		receipt.IndexedAt = s.Now()
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

func (s *Service) GetReceipt(tenantID, actor, eventID string) (domain.EventReceipt, error) {
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
	var event domain.Event
	err := s.Store.Read(func(data *store.Snapshot) error {
		value, ok := data.Events[store.EventKey(tenantID, eventID)]
		if !ok {
			return domain.ErrNotFound
		}
		event = value
		return nil
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
// have no paired mutation, so the record is appended in its own Store.Update
// after the read succeeds; a failed append aborts the read (fail-closed:
// an un-auditable read must not be reported as served).
func (s *Service) recordReadAction(tenantID, actor, action, targetType, targetID, detail string) error {
	if actor == "" {
		return nil
	}
	return s.Store.Update(func(data *store.Snapshot) error {
		data.AdminActions = append(data.AdminActions, s.adminAction(tenantID, actor, action, targetType, targetID, detail))
		return nil
	})
}

func (s *Service) QueryEvents(tenantID, actor string, query domain.Query) (domain.QueryResult, error) {
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
		for _, event := range data.Events {
			if event.TenantID == tenantID && s.matches(event, query, schemas) {
				events = append(events, event)
			}
		}
		return nil
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

// operationTimelineNoAudit is the un-audited core shared by OperationTimeline
// and ReplayOperation, so a single replay call appends exactly one fact
// (FR-3: no delegation path may append twice).
func (s *Service) operationTimelineNoAudit(tenantID, operationID string) ([]domain.Event, error) {
	events, err := s.eventsFor(tenantID, func(event domain.Event) bool { return event.OperationID == operationID })
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, domain.ErrNotFound
	}
	sortEvents(events)
	return events, nil
}

// aggregateTimelineNoAudit mirrors operationTimelineNoAudit for aggregates
// (aggregate-version ordering preserved verbatim).
func (s *Service) aggregateTimelineNoAudit(tenantID, aggregateType, aggregateID string) ([]domain.Event, error) {
	events, err := s.eventsFor(tenantID, func(event domain.Event) bool {
		return event.AggregateType == aggregateType && event.AggregateID == aggregateID
	})
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, domain.ErrNotFound
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].AggregateVersion == events[j].AggregateVersion {
			return events[i].EventID < events[j].EventID
		}
		return events[i].AggregateVersion < events[j].AggregateVersion
	})
	return events, nil
}

func (s *Service) OperationTimeline(tenantID, actor, operationID string) ([]domain.Event, error) {
	events, err := s.operationTimelineNoAudit(tenantID, operationID)
	if err != nil {
		return nil, err
	}
	// Read self-audit (F-06): append-before-serve, fail-closed — a timeline
	// read that cannot leave its governance fact is not served.
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "operation", operationID, "timeline"); err != nil {
		return nil, err
	}
	return events, nil
}

func (s *Service) AggregateTimeline(tenantID, actor, aggregateType, aggregateID string) ([]domain.Event, error) {
	events, err := s.aggregateTimelineNoAudit(tenantID, aggregateType, aggregateID)
	if err != nil {
		return nil, err
	}
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "aggregate", aggregateID, aggregateType); err != nil {
		return nil, err
	}
	return events, nil
}

func (s *Service) ReplayOperation(tenantID, actor, operationID string) (domain.ReplayResult, error) {
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
	if err := s.Store.Read(func(data *store.Snapshot) error {
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

func (s *Service) validateEvent(tenantID string, principal domain.IngestPrincipal, event domain.Event) (domain.EventSchema, error) {
	var schema domain.EventSchema
	err := s.Store.Read(func(data *store.Snapshot) error {
		if !sourceAccessAllowed(data, tenantID, event.SourceSystem, principal.ClientID) {
			return sourceAccessError()
		}
		var ok bool
		schema, ok = data.Schemas[store.SchemaKey(tenantID, event.SchemaID, event.SchemaVersion)]
		if !ok || !schema.Active {
			return domain.ErrSchemaNotFound
		}
		return nil
	})
	if err != nil {
		return domain.EventSchema{}, err
	}
	if schema.EventType != event.EventType {
		return domain.EventSchema{}, fmt.Errorf("%w: event_type does not match schema", domain.ErrInvalid)
	}
	if schema.Classification != "" && event.DataClassification != schema.Classification {
		return domain.EventSchema{}, fmt.Errorf("%w: data_classification does not match schema", domain.ErrInvalid)
	}
	if err := rejectSensitive(event.Payload); err != nil {
		return domain.EventSchema{}, err
	}
	// Reserved namespace (this direction): top-level payload keys ending in
	// "__search_digest" are not user-allocatable. The stored SourceDigest
	// authenticates the pre-protection payload; a producer-planted colliding
	// key would be overwritten by protectSensitiveFields at rest and deleted
	// by reconstructAndDerive on verification, making the derived digest
	// permanently unequal to the stored one (stream invalid forever). Reject
	// before protectSensitiveFields / Store.Update so nothing persists.
	if err := rejectReservedSearchDigestNamespace(event.Payload); err != nil {
		return domain.EventSchema{}, err
	}
	if encodedChanges, marshalErr := json.Marshal(event.ChangedFields); marshalErr == nil {
		var decodedChanges any
		if json.Unmarshal(encodedChanges, &decodedChanges) == nil {
			if err := rejectSensitive(decodedChanges); err != nil {
				return domain.EventSchema{}, err
			}
		}
	}
	encoded, err := domain.CanonicalJSON(event.Payload)
	if err != nil {
		return domain.EventSchema{}, err
	}
	if len(encoded) > s.Config.MaxEventBytes {
		return domain.EventSchema{}, fmt.Errorf("%w: payload exceeds %d bytes", domain.ErrInvalid, s.Config.MaxEventBytes)
	}
	for _, field := range schema.RequiredFields {
		if !hasField(event, field) {
			return domain.EventSchema{}, fmt.Errorf("%w: schema field %s is required", domain.ErrInvalid, field)
		}
	}
	if len(schema.AllowedFields) > 0 {
		allowed := map[string]bool{}
		for _, field := range schema.AllowedFields {
			allowed[field] = true
		}
		for field := range event.Payload {
			if !allowed[field] {
				return domain.EventSchema{}, fmt.Errorf("%w: payload field %s is not in schema", domain.ErrInvalid, field)
			}
		}
	}
	return schema, nil
}

func (s *Service) protectSensitiveFields(event *domain.Event, schema domain.EventSchema) error {
	if len(schema.EncryptedFields) == 0 && len(schema.SearchableFields) == 0 {
		return nil
	}
	if event.Payload == nil {
		return nil
	}
	// Preserve the caller's canonical value while applying both protections.
	// A searchable digest must be derived from the original value, not from
	// the encrypted representation written back to the payload.
	originals := map[string]any{}
	for _, field := range schema.EncryptedFields {
		if value, ok := event.Payload[field]; ok {
			originals[field] = value
		}
	}
	for _, field := range schema.SearchableFields {
		if value, ok := event.Payload[field]; ok {
			originals[field] = value
		}
	}
	for _, field := range schema.EncryptedFields {
		value, ok := originals[field]
		if !ok {
			continue
		}
		encoded, err := security.EncryptJSON(value, s.Config.EncryptionKey, event.TenantID+"/"+field+"/"+event.EventID)
		if err != nil {
			return err
		}
		event.Payload[field] = encoded
	}
	for _, field := range schema.SearchableFields {
		value, ok := originals[field]
		if !ok {
			continue
		}
		// Tenant/field-bound digest: the same plaintext in another tenant or
		// under another field name yields a different digest, so stored
		// digests can no longer be used to correlate records across tenants
		// (threat-model boundary D). The key name stays field+"__search_digest"
		// and the format is self-describing ("sd2:" prefix) so old clients
		// and legacy stored events remain distinguishable.
		digest, err := security.SearchDigestBound(value, s.Config.EncryptionKey, event.TenantID, field)
		if err != nil {
			return err
		}
		event.Payload[field+security.SearchDigestSuffix] = digest
	}
	return nil
}

// verifyContentDigest reports a per-event verification error, or nil. It
// authenticates the stored content by reconstructing the pre-protection
// payload (the exact inverse of protectSensitiveFields) and comparing a
// freshly derived digest against the stored SourceDigest. An empty
// SourceDigest fails closed: an audit ledger must not silently pass events
// whose content is unauthenticated.
func (s *Service) verifyContentDigest(event domain.Event, schemas map[string]domain.EventSchema) error {
	if event.SourceDigest == "" {
		return fmt.Errorf("stream %s sequence %d missing source_digest", event.StreamID, event.Sequence)
	}
	derived, err := s.reconstructAndDerive(event, schemas)
	if err != nil {
		return fmt.Errorf("stream %s sequence %d content digest check failed: %w", event.StreamID, event.Sequence, err)
	}
	if derived != event.SourceDigest {
		if key, ok := firstReservedSearchDigestKey(event.Payload); ok {
			// Neutral diagnostic for any content mismatch on an event whose
			// stored payload carries a top-level reserved-namespace key. This
			// covers both the legacy namespace-collision shape (producer-
			// planted digest key with SourceDigest over the payload including
			// it) and ordinary tampering of an event that legitimately carries
			// a stored digest key; in both shapes the mismatch cannot be
			// attributed to ordinary tampering of a non-digest payload, and
			// the wording stays factually accurate — the digest WAS
			// reconstructed (that is how the mismatch was detected); it simply
			// does not match. Validity outcome is unchanged (invalid) — only
			// the wording differs.
			return fmt.Errorf("stream %s sequence %d content verification failed: stored payload carries top-level key %q in the reserved search_digest namespace (mismatch cannot be attributed to ordinary tampering)", event.StreamID, event.Sequence, key)
		}
		return fmt.Errorf("stream %s sequence %d content digest mismatch", event.StreamID, event.Sequence)
	}
	return nil
}

// reconstructAndDerive undoes protectSensitiveFields (removing search
// digests and decrypting encrypted fields with the same tenant/field/eventID
// AAD binding used at ingest), then derives the content digest from the
// reconstructed payload. It never mutates event.Payload: the stored map is
// shared by reference with the store snapshot, so it is deep-copied first.
// The schema definition the event was ingested under is looked up
// version-exactly.
//
// Number-encoding consistency note: reconstruction must canonicalize numbers
// exactly like the ingest-time digest path. Both sides decode through
// json.Number (clonePayload here and DecryptJSON), so int64 values > 2^53
// keep their exact digits and VerifyIntegrity agrees with the stored digest
// (pinned by TestVerifyIntegrityLargeIntSensitiveField).
func (s *Service) reconstructAndDerive(event domain.Event, schemas map[string]domain.EventSchema) (string, error) {
	schema, ok := schemas[store.SchemaKey(event.TenantID, event.SchemaID, event.SchemaVersion)]
	if !ok {
		return "", fmt.Errorf("schema %s v%d not found", event.SchemaID, event.SchemaVersion)
	}
	payload, err := clonePayload(event.Payload)
	if err != nil {
		return "", err
	}
	for _, field := range schema.SearchableFields {
		delete(payload, field+security.SearchDigestSuffix)
	}
	for _, field := range schema.EncryptedFields {
		encoded, ok := payload[field].(string)
		if !ok {
			continue
		}
		original, err := security.DecryptJSON(encoded, s.Config.EncryptionKey, event.TenantID+"/"+field+"/"+event.EventID)
		if err != nil {
			return "", fmt.Errorf("cannot decrypt field %s: %w", field, err)
		}
		payload[field] = original
	}
	reconstructed := event
	reconstructed.Payload = payload
	return domain.EventContentDigest(reconstructed)
}

func (s *Service) eventHash(event domain.Event) (string, error) {
	digest, err := domain.EventDigest(event)
	if err != nil {
		return "", err
	}
	data, err := domain.CanonicalJSON(map[string]any{"digest": digest, "prev_hash": event.PrevHash, "sequence": event.Sequence, "stream_id": event.StreamID})
	if err != nil {
		return "", err
	}
	return domain.HashBytes(data), nil
}

func (s *Service) sealSegment(ctx context.Context, stream store.StreamState, now time.Time) (domain.Segment, domain.Checkpoint, error) {
	hashes := append([]string(nil), stream.PendingHashes...)
	root := merkleRoot(hashes)
	manifest := fmt.Sprintf("%s:%d:%d:%s:%s", stream.TenantID, stream.NextSequence-int64(len(hashes)), stream.NextSequence-1, stream.HeadHash, root)
	manifestHash := domain.HashBytes([]byte(manifest))
	signature, err := s.Config.Signer.Sign(ctx, []byte(manifestHash))
	if err != nil {
		return domain.Segment{}, domain.Checkpoint{}, fmt.Errorf("sign segment: %w", err)
	}
	segment := domain.Segment{TenantID: stream.TenantID, StreamID: stream.StreamID, FirstSequence: stream.NextSequence - int64(len(hashes)), LastSequence: stream.NextSequence - 1, FirstPrevHash: stream.PendingPrevHash, LastHash: stream.HeadHash, EventCount: len(hashes), MerkleRoot: root, ManifestHash: manifestHash, Signature: signature, CreatedAt: now}
	checkpoint := domain.Checkpoint{ID: newID("checkpoint"), TenantID: stream.TenantID, StreamID: stream.StreamID, Sequence: stream.NextSequence - 1, MerkleRoot: root, Signature: signature, Algorithm: s.Config.Signer.Algorithm(), CreatedAt: now}
	return segment, checkpoint, nil
}

func (s *Service) archiveEvent(event domain.Event) error {
	data, err := domain.CanonicalJSON(event)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("events/%s/%s/%020d-%s.json", safeName(event.TenantID), safeName(event.StreamID), event.Sequence, safeName(event.EventID))
	return s.Config.Archive.Put(context.Background(), key, data)
}

func (s *Service) archiveSegment(segment domain.Segment) error {
	data, err := domain.CanonicalJSON(segment)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("segments/%s/%s/%020d-%020d.manifest.json", safeName(segment.TenantID), safeName(segment.StreamID), segment.FirstSequence, segment.LastSequence)
	return s.Config.Archive.Put(context.Background(), key, data)
}

func (s *Service) eventsFor(tenantID string, predicate func(domain.Event) bool) ([]domain.Event, error) {
	var events []domain.Event
	err := s.Store.Read(func(data *store.Snapshot) error {
		for _, event := range data.Events {
			if event.TenantID == tenantID && predicate(event) {
				events = append(events, event)
			}
		}
		return nil
	})
	return events, err
}

func (s *Service) matches(event domain.Event, query domain.Query, schemas map[string]domain.EventSchema) bool {
	if event.OccurredAt.Before(query.From) || !event.OccurredAt.Before(query.To) {
		return false
	}
	if query.EventType != "" && event.EventType != query.EventType {
		return false
	}
	if query.SourceSystem != "" && event.SourceSystem != query.SourceSystem {
		return false
	}
	if query.ActorID != "" && event.Actor.ID != query.ActorID {
		return false
	}
	if query.TargetID != "" && !hasTarget(event, query.TargetID) {
		return false
	}
	if query.AggregateType != "" && event.AggregateType != query.AggregateType {
		return false
	}
	if query.AggregateID != "" && event.AggregateID != query.AggregateID {
		return false
	}
	if query.OperationID != "" && event.OperationID != query.OperationID {
		return false
	}
	if query.CausationID != "" && event.CausationID != query.CausationID {
		return false
	}
	if query.CorrelationID != "" && event.CorrelationID != query.CorrelationID {
		return false
	}
	if query.TraceID != "" && event.TraceID != query.TraceID {
		return false
	}
	if query.Outcome != "" && event.Outcome != query.Outcome {
		return false
	}
	if query.PayloadField != "" {
		if !s.digestMatches(event, query, schemas) {
			return false
		}
	}
	if query.StreamID != "" && event.StreamID != query.StreamID {
		return false
	}
	return true
}

// digestMatches reports whether the stored event satisfies the query's
// (payload_field, payload_digest) filter. Both digest formats are accepted:
//   - same format (v1 unbound or v2 bound on both sides) → direct equality;
//   - mixed formats → the query-format digest is re-derived from the stored
//     plaintext and compared. The fallback fails closed when the plaintext
//     is unavailable (encrypted field, missing value, unknown schema
//     version), so a cross-format query can never match through a
//     ciphertext or an absent value.
//
// The fallback keeps old v1 clients able to search events ingested under
// the new bound format and new v2 clients able to search legacy events.
func (s *Service) digestMatches(event domain.Event, query domain.Query, schemas map[string]domain.EventSchema) bool {
	stored, ok := event.Payload[query.PayloadField+security.SearchDigestSuffix].(string)
	if !ok {
		return false
	}
	if security.IsBoundSearchDigest(stored) == security.IsBoundSearchDigest(query.PayloadDigest) {
		return stored == query.PayloadDigest
	}
	schema, ok := schemas[store.SchemaKey(event.TenantID, event.SchemaID, event.SchemaVersion)]
	if !ok || containsString(schema.EncryptedFields, query.PayloadField) {
		return false
	}
	value, ok := event.Payload[query.PayloadField]
	if !ok {
		return false
	}
	var derived string
	var err error
	if security.IsBoundSearchDigest(query.PayloadDigest) {
		derived, err = security.SearchDigestBound(value, s.Config.EncryptionKey, event.TenantID, query.PayloadField)
	} else {
		derived, err = security.SearchDigest(value, s.Config.EncryptionKey)
	}
	if err != nil {
		return false
	}
	return derived == query.PayloadDigest
}

func (s *Service) holdMatchesEvent(hold domain.LegalHold, event domain.Event, schemas map[string]domain.EventSchema) bool {
	filter := hold.Filter
	if filter.From.IsZero() && filter.To.IsZero() {
		return true
	}
	if filter.From.IsZero() || filter.To.IsZero() {
		return false
	}
	return s.matches(event, filter, schemas)
}

func appendUnique(values []string, wanted string) []string {
	for _, value := range values {
		if value == wanted {
			return values
		}
	}
	return append(values, wanted)
}

// compareEvents reports whether a precedes b in the chronological total
// order (OccurredAt, Sequence, EventID). event_id is unique per tenant
// (store.EventKey dedupe), so the tuple never ties: the order is strict.
func compareEvents(a, b domain.Event) bool {
	if a.OccurredAt.Equal(b.OccurredAt) {
		if a.Sequence == b.Sequence {
			return a.EventID < b.EventID
		}
		return a.Sequence < b.Sequence
	}
	return a.OccurredAt.Before(b.OccurredAt)
}

func sortEvents(events []domain.Event) {
	sort.Slice(events, func(i, j int) bool { return compareEvents(events[i], events[j]) })
}

func replay(events []domain.Event, tenantID, operationID, aggregateID string) domain.ReplayResult {
	state := map[string]any{}
	var last int64
	for _, event := range events {
		for field, change := range event.ChangedFields {
			if change.After == nil {
				delete(state, field)
			} else {
				state[field] = change.After
			}
		}
		last = event.Sequence
	}
	return domain.ReplayResult{TenantID: tenantID, OperationID: operationID, AggregateID: aggregateID, EventCount: len(events), State: state, LastSequence: last}
}

func hasField(event domain.Event, field string) bool {
	switch field {
	case "event_id":
		return event.EventID != ""
	case "source_system":
		return event.SourceSystem != ""
	case "event_type":
		return event.EventType != ""
	case "operation_id":
		return event.OperationID != ""
	case "aggregate_id":
		return event.AggregateID != ""
	case "actor.id":
		return event.Actor.ID != ""
	default:
		_, ok := event.Payload[field]
		return ok
	}
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func hasTarget(event domain.Event, targetID string) bool {
	for _, target := range event.Targets {
		if target.ID == targetID {
			return true
		}
	}
	return false
}

func rejectSensitive(value any) error {
	var scan func(any, string) error
	scan = func(current any, path string) error {
		switch v := current.(type) {
		case map[string]any:
			for key, child := range v {
				lower := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
				for _, forbidden := range []string{"password", "token", "private_key", "device_credential", "card_number", "secret"} {
					if strings.Contains(lower, forbidden) {
						return fmt.Errorf("%w: sensitive field %s is forbidden", domain.ErrInvalid, path+key)
					}
				}
				if err := scan(child, path+key+"."); err != nil {
					return err
				}
			}
		case []any:
			for i, child := range v {
				if err := scan(child, fmt.Sprintf("%s%d.", path, i)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return scan(value, "")
}

func clonePayload(payload map[string]any) (map[string]any, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var copyPayload map[string]any
	// UseNumber: a float64 re-decode would collapse int64 values > 2^53
	// before digest derivation, corrupting the canonical digest.
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&copyPayload); err != nil {
		return nil, err
	}
	return copyPayload, nil
}

func merkleRoot(hashes []string) string {
	if len(hashes) == 0 {
		return domain.HashBytes(nil)
	}
	level := append([]string(nil), hashes...)
	for len(level) > 1 {
		next := make([]string, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			left, right := level[i], level[i]
			if i+1 < len(level) {
				right = level[i+1]
			}
			next = append(next, domain.HashBytes([]byte(left+right)))
		}
		level = next
	}
	return level[0]
}

func newID(prefix string) string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(buf)
}

func safeName(value string) string {
	var b strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "unnamed"
	}
	return b.String()
}

// ListAdminActions returns the append-only self-audit trail, newest first.
// A tenant-scoped caller only sees its own tenant's actions; a platform
// caller sees all actions (or one tenant with the optional filter).
func Marshal(value any) ([]byte, error) { return json.Marshal(value) }
