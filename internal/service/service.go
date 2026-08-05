package service

import (
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
	// Signer produces checkpoint signatures. Defaults to HMAC-SHA256 over
	// SigningSecret; a Vault Transit signer can be injected for KMS-backed
	// signatures where the private key never leaves Vault.
	Signer Signer
	// Archive persists WORM-compatible compliance objects. Defaults to the
	// local read-only directory; an S3 Object Lock store can be injected.
	Archive archive.Store
}

// Signer creates and verifies checkpoint signatures. Implementations must be
// deterministic per input so Verify can re-check archived manifests.
type Signer interface {
	Sign(data []byte) (string, error)
	Verify(data []byte, signature string) (bool, error)
	Algorithm() string
}

// hmacSigner is the local development signature scheme.
type hmacSigner struct {
	secret string
}

func (h hmacSigner) Sign(data []byte) (string, error) {
	mac := hmac.New(sha256.New, []byte(h.secret))
	_, _ = mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func (h hmacSigner) Verify(data []byte, signature string) (bool, error) {
	expected, err := h.Sign(data)
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

func New(st *store.Store, cfg Config) *Service {
	if cfg.ServerVersion == "" {
		cfg.ServerVersion = "audit-governance/dev"
	}
	if cfg.SegmentSize <= 0 {
		cfg.SegmentSize = 100
	}
	if cfg.MaxEventBytes <= 0 {
		cfg.MaxEventBytes = domain.MaxEventBytes
	}
	if cfg.SigningSecret == "" {
		cfg.SigningSecret = "development-signing-key-change-me"
	}
	if cfg.EncryptionKey == "" {
		cfg.EncryptionKey = cfg.SigningSecret
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
	return &Service{Store: st, Config: cfg, quotas: map[string]quotaWindow{}}
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
				if holdMatchesEvent(hold, event) {
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

func (s *Service) Ingest(tenantID string, principal domain.IngestPrincipal, event domain.Event, waitFor string) (domain.EventReceipt, error) {
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
	var sealedSegments []domain.Segment
	err = s.Store.Update(func(data *store.Snapshot) error {
		commitTenant, accessErr := resolveIngestTenantFromData(data, tenantHint, principal.ClientID, event.SourceSystem)
		if accessErr != nil || commitTenant != tenantID {
			return sourceAccessError()
		}
		key := store.EventKey(tenantID, event.EventID)
		if existing, ok := data.Events[key]; ok {
			existingDigest, digestErr := domain.EventDigest(existing)
			if digestErr != nil {
				return digestErr
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
			segment, checkpoint, sealErr := s.sealSegment(stream, now)
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
	if s.Config.ArchiveDir != "" {
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

func (s *Service) GetReceipt(tenantID, eventID string) (domain.EventReceipt, error) {
	var receipt domain.EventReceipt
	err := s.Store.Read(func(data *store.Snapshot) error {
		value, ok := data.Receipts[store.EventKey(tenantID, eventID)]
		if !ok {
			return domain.ErrNotFound
		}
		receipt = value
		return nil
	})
	return receipt, err
}

func (s *Service) GetEvent(tenantID, eventID string) (domain.Event, error) {
	var event domain.Event
	err := s.Store.Read(func(data *store.Snapshot) error {
		value, ok := data.Events[store.EventKey(tenantID, eventID)]
		if !ok {
			return domain.ErrNotFound
		}
		event = value
		return nil
	})
	return event, err
}

func (s *Service) QueryEvents(tenantID string, query domain.Query) (domain.QueryResult, error) {
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
	err := s.Store.Read(func(data *store.Snapshot) error {
		for _, event := range data.Events {
			if event.TenantID == tenantID && matches(event, query) {
				events = append(events, event)
			}
		}
		return nil
	})
	if err != nil {
		return domain.QueryResult{}, err
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].Sequence == events[j].Sequence {
			return events[i].EventID < events[j].EventID
		}
		return events[i].Sequence < events[j].Sequence
	})
	if query.Cursor != "" {
		sequence, eventID, err := domain.DecodeCursor(query.Cursor)
		if err != nil {
			return domain.QueryResult{}, err
		}
		filtered := events[:0]
		for _, event := range events {
			if event.Sequence > sequence || (event.Sequence == sequence && event.EventID > eventID) {
				filtered = append(filtered, event)
			}
		}
		events = filtered
	}
	result := domain.QueryResult{Count: len(events)}
	if len(events) > query.PageSize {
		last := events[query.PageSize-1]
		result.Items = events[:query.PageSize]
		result.NextCursor = domain.EncodeCursor(last.Sequence, last.EventID)
	} else {
		result.Items = events
	}
	return result, nil
}

func (s *Service) Operation(tenantID, operationID string) (domain.OperationSummary, error) {
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
	return domain.OperationSummary{OperationID: operationID, TenantID: tenantID, EventCount: len(events), FirstAt: events[0].OccurredAt, LastAt: events[len(events)-1].OccurredAt, Outcomes: outcomes}, nil
}

func (s *Service) OperationTimeline(tenantID, operationID string) ([]domain.Event, error) {
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

func (s *Service) AggregateTimeline(tenantID, aggregateType, aggregateID string) ([]domain.Event, error) {
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

func (s *Service) ReplayOperation(tenantID, operationID string) (domain.ReplayResult, error) {
	events, err := s.OperationTimeline(tenantID, operationID)
	if err != nil {
		return domain.ReplayResult{}, err
	}
	return replay(events, tenantID, operationID, ""), nil
}

func (s *Service) ReplayAggregate(tenantID, aggregateType, aggregateID string) (domain.ReplayResult, error) {
	events, err := s.AggregateTimeline(tenantID, aggregateType, aggregateID)
	if err != nil {
		return domain.ReplayResult{}, err
	}
	return replay(events, tenantID, "", aggregateID), nil
}

func (s *Service) CreateExport(tenantID, requestedBy string, query domain.Query) (domain.ExportJob, error) {
	if _, err := s.QueryEvents(tenantID, query); err != nil {
		return domain.ExportJob{}, err
	}
	job := domain.ExportJob{ID: newID("export"), TenantID: tenantID, RequestedBy: requestedBy, Query: query, Status: "pending", CreatedAt: s.Now()}
	if err := s.Store.Update(func(data *store.Snapshot) error {
		data.Exports[job.ID] = job
		data.AdminActions = append(data.AdminActions, s.adminAction(tenantID, requestedBy, domain.AdminActionExportCreated, "export", job.ID, fmt.Sprintf("from=%s to=%s", query.From.Format(time.RFC3339), query.To.Format(time.RFC3339))))
		return nil
	}); err != nil {
		return domain.ExportJob{}, err
	}
	go s.runExport(job.ID)
	return job, nil
}

func (s *Service) GetExport(tenantID, jobID string) (domain.ExportJob, error) {
	var job domain.ExportJob
	err := s.Store.Read(func(data *store.Snapshot) error {
		value, ok := data.Exports[jobID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		job = value
		return nil
	})
	return job, err
}

func (s *Service) CreateLegalHold(hold domain.LegalHold) (domain.LegalHold, error) {
	if hold.TenantID == "" || hold.Name == "" || hold.Reason == "" {
		return domain.LegalHold{}, fmt.Errorf("%w: tenant_id, name and reason are required", domain.ErrInvalid)
	}
	if !hold.Filter.From.IsZero() || !hold.Filter.To.IsZero() {
		if hold.Filter.From.IsZero() || hold.Filter.To.IsZero() || !hold.Filter.From.Before(hold.Filter.To) {
			return domain.LegalHold{}, fmt.Errorf("%w: legal hold filter requires from before to", domain.ErrInvalid)
		}
	}
	if (hold.Filter.PayloadField == "") != (hold.Filter.PayloadDigest == "") {
		return domain.LegalHold{}, fmt.Errorf("%w: payload_field and payload_digest must be provided together", domain.ErrInvalid)
	}
	if hold.ID == "" {
		hold.ID = newID("hold")
	}
	if hold.CreatedAt.IsZero() {
		hold.CreatedAt = s.Now()
	}
	if err := s.Store.Update(func(data *store.Snapshot) error {
		if _, ok := data.Tenants[hold.TenantID]; !ok {
			return domain.ErrNotFound
		}
		if _, exists := data.LegalHolds[hold.ID]; exists {
			return fmt.Errorf("%w: legal hold already exists", domain.ErrConflict)
		}
		data.LegalHolds[hold.ID] = hold
		data.AdminActions = append(data.AdminActions, s.adminAction(hold.TenantID, hold.CreatedBy, domain.AdminActionLegalHoldCreated, "legal_hold", hold.ID, hold.Reason))
		return nil
	}); err != nil {
		return domain.LegalHold{}, err
	}
	return hold, nil
}

func (s *Service) ListLegalHolds(tenantID string) ([]domain.LegalHold, error) {
	var result []domain.LegalHold
	err := s.Store.Read(func(data *store.Snapshot) error {
		for _, hold := range data.LegalHolds {
			if hold.TenantID == tenantID {
				result = append(result, hold)
			}
		}
		return nil
	})
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result, err
}

func (s *Service) ReleaseLegalHold(tenantID, holdID, releasedBy string) (domain.LegalHold, error) {
	var hold domain.LegalHold
	if err := s.Store.Update(func(data *store.Snapshot) error {
		value, ok := data.LegalHolds[holdID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		if value.ReleasedAt != nil {
			hold = value
			return nil
		}
		now := s.Now()
		value.ReleasedAt = &now
		value.ReleasedBy = releasedBy
		data.LegalHolds[holdID] = value
		hold = value
		data.AdminActions = append(data.AdminActions, s.adminAction(value.TenantID, releasedBy, domain.AdminActionLegalHoldReleased, "legal_hold", holdID, ""))
		return nil
	}); err != nil {
		return domain.LegalHold{}, err
	}
	return hold, nil
}

func (s *Service) VerifyIntegrity(tenantID, streamID string) (IntegrityResult, error) {
	result := IntegrityResult{Valid: true, TenantID: tenantID, StreamID: streamID, CheckedAt: s.Now()}
	var events []domain.Event
	var segments []domain.Segment
	err := s.Store.Read(func(data *store.Snapshot) error {
		for _, event := range data.Events {
			if event.TenantID == tenantID && (streamID == "" || event.StreamID == streamID) {
				events = append(events, event)
			}
		}
		for key, values := range data.Segments {
			if strings.HasPrefix(key, tenantID+store.KeySeparator) && (streamID == "" || strings.HasSuffix(key, store.KeySeparator+streamID)) {
				segments = append(segments, values...)
			}
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	result.EventCount = len(events)
	result.SegmentCount = len(segments)
	// Events from different streams have independent sequence spaces.  They
	// must be checked stream-by-stream; ordering the combined result by event
	// time can otherwise make a valid chain look corrupted.
	byStream := map[string][]domain.Event{}
	for _, event := range events {
		byStream[event.StreamID] = append(byStream[event.StreamID], event)
	}
	for currentStream, streamEvents := range byStream {
		sort.Slice(streamEvents, func(i, j int) bool { return streamEvents[i].Sequence < streamEvents[j].Sequence })
		head := ""
		for _, event := range streamEvents {
			if event.PrevHash != head {
				result.Valid = false
				result.Errors = append(result.Errors, fmt.Sprintf("stream %s sequence %d prev_hash mismatch", currentStream, event.Sequence))
			}
			expected, hashErr := s.eventHash(event)
			if hashErr != nil || expected != event.Hash {
				result.Valid = false
				result.Errors = append(result.Errors, fmt.Sprintf("stream %s sequence %d hash mismatch", currentStream, event.Sequence))
			}
			head = event.Hash
		}
	}
	for _, segment := range segments {
		segmentHashes := make([]string, 0, segment.EventCount)
		streamEvents := byStream[segment.StreamID]
		for _, event := range streamEvents {
			if event.StreamID == segment.StreamID && event.Sequence >= segment.FirstSequence && event.Sequence <= segment.LastSequence {
				segmentHashes = append(segmentHashes, event.Hash)
			}
		}
		root := merkleRoot(segmentHashes)
		firstPrevHash := ""
		lastHash := ""
		if len(streamEvents) > 0 {
			for _, event := range streamEvents {
				if event.Sequence == segment.FirstSequence {
					firstPrevHash = event.PrevHash
				}
				if event.Sequence == segment.LastSequence {
					lastHash = event.Hash
				}
			}
		}
		contiguous := segment.LastSequence >= segment.FirstSequence && segment.EventCount == int(segment.LastSequence-segment.FirstSequence+1)
		if root != segment.MerkleRoot || len(segmentHashes) != segment.EventCount || firstPrevHash != segment.FirstPrevHash || lastHash != segment.LastHash || !contiguous {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("stream %s segment %d-%d merkle root mismatch", segment.StreamID, segment.FirstSequence, segment.LastSequence))
		}
		manifest := fmt.Sprintf("%s:%d:%d:%s:%s", segment.TenantID, segment.FirstSequence, segment.LastSequence, segment.LastHash, segment.MerkleRoot)
		manifestHash := domain.HashBytes([]byte(manifest))
		valid, verifyErr := s.Config.Signer.Verify([]byte(manifestHash), segment.Signature)
		if verifyErr != nil || manifestHash != segment.ManifestHash || !valid {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("stream %s segment %d-%d signature mismatch", segment.StreamID, segment.FirstSequence, segment.LastSequence))
		}
	}
	return result, nil
}

// SealPendingSegments creates a checkpoint for each non-empty partial stream.
// The ingest path seals full segments; the governance worker calls this method
// periodically so low-volume streams also receive a durable checkpoint.
func (s *Service) SealPendingSegments(tenantID string) error {
	now := s.Now()
	return s.Store.Update(func(data *store.Snapshot) error {
		for key, stream := range data.Streams {
			if stream.TenantID != tenantID || len(stream.PendingHashes) == 0 {
				continue
			}
			segment, checkpoint, err := s.sealSegment(stream, now)
			if err != nil {
				return err
			}
			data.Segments[key] = append(data.Segments[key], segment)
			data.Checkpoints[key] = append(data.Checkpoints[key], checkpoint)
			stream.PendingHashes = nil
			stream.PendingEvents = nil
			stream.PendingPrevHash = ""
			data.Streams[key] = stream
		}
		return nil
	})
}

// ArchivePending retries local WORM-compatible archive writes for events that
// were ledgered/indexed before the archive destination became available.
// It is intentionally idempotent: archive files are created with O_EXCL and
// an existing file is treated as already archived.
func (s *Service) ArchivePending(tenantID string) (int, error) {
	if s.Config.ArchiveDir == "" {
		return 0, fmt.Errorf("%w: archive directory is not configured", domain.ErrInvalid)
	}
	var events []domain.Event
	var segments []domain.Segment
	if err := s.Store.Read(func(data *store.Snapshot) error {
		for key, event := range data.Events {
			if event.TenantID != tenantID {
				continue
			}
			receipt := data.Receipts[key]
			if receipt.Status != domain.StatusArchived {
				events = append(events, event)
			}
		}
		for key, values := range data.Segments {
			if key == store.StreamKey(tenantID, "") || strings.HasPrefix(key, tenantID+store.KeySeparator) {
				segments = append(segments, values...)
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	for _, segment := range segments {
		if err := s.archiveSegment(segment); err != nil {
			return 0, err
		}
	}
	archived := 0
	for _, event := range events {
		if err := s.archiveEvent(event); err != nil {
			return archived, err
		}
		if err := s.Store.Update(func(data *store.Snapshot) error {
			key := store.EventKey(tenantID, event.EventID)
			receipt, ok := data.Receipts[key]
			if !ok {
				return domain.ErrNotFound
			}
			now := s.Now()
			receipt.Status = domain.StatusArchived
			receipt.IndexedAt = now
			receipt.ArchivedAt = now
			data.Receipts[key] = receipt
			return nil
		}); err != nil {
			return archived, err
		}
		archived++
	}
	return archived, nil
}

func (s *Service) PreviewRestore(tenantID string, request domain.RestoreRequest) (domain.RestorePreview, error) {
	result, err := s.ReplayOperation(tenantID, request.OperationID)
	if err != nil {
		return domain.RestorePreview{}, err
	}
	return domain.RestorePreview{ID: newID("restore-preview"), TenantID: tenantID, OperationID: request.OperationID, ProposedState: result.State, ExternalCalls: []string{}, RequiresApproval: true}, nil
}

func (s *Service) CreateRestore(tenantID string, request domain.RestoreRequest, requestedBy string) (domain.RestoreRun, error) {
	if request.OperationID == "" || request.Reason == "" {
		return domain.RestoreRun{}, fmt.Errorf("%w: operation_id and reason are required", domain.ErrInvalid)
	}
	if _, err := s.ReplayOperation(tenantID, request.OperationID); err != nil {
		return domain.RestoreRun{}, err
	}
	run := domain.RestoreRun{ID: newID("restore"), TenantID: tenantID, OperationID: request.OperationID, Status: domain.RestoreStatusPendingApproval, Reason: request.Reason, CreatedBy: requestedBy, CreatedAt: s.Now()}
	if err := s.Store.Update(func(data *store.Snapshot) error {
		data.RestoreRuns[run.ID] = run
		data.AdminActions = append(data.AdminActions, s.adminAction(tenantID, requestedBy, domain.AdminActionRestoreCreated, "restore", run.ID, request.OperationID))
		return nil
	}); err != nil {
		return domain.RestoreRun{}, err
	}
	return run, nil
}

// ApproveRestore moves a pending restore run to approved. Approval only
// records the decision fact; executing the compensating business calls is a
// separate, later stage and is never triggered here.
func (s *Service) ApproveRestore(tenantID, runID, approvedBy string) (domain.RestoreRun, error) {
	return s.transitionRestore(tenantID, runID, approvedBy, domain.RestoreStatusApproved)
}

// RejectRestore moves a pending restore run to rejected.
func (s *Service) RejectRestore(tenantID, runID, rejectedBy string) (domain.RestoreRun, error) {
	return s.transitionRestore(tenantID, runID, rejectedBy, domain.RestoreStatusRejected)
}

func (s *Service) transitionRestore(tenantID, runID, actor, target string) (domain.RestoreRun, error) {
	var run domain.RestoreRun
	err := s.Store.Update(func(data *store.Snapshot) error {
		value, ok := data.RestoreRuns[runID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		if value.Status != domain.RestoreStatusPendingApproval {
			return fmt.Errorf("%w: restore run is not pending approval", domain.ErrConflict)
		}
		now := s.Now()
		if target == domain.RestoreStatusApproved {
			value.Status = domain.RestoreStatusApproved
			value.ApprovedBy = actor
			value.ApprovedAt = &now
			data.AdminActions = append(data.AdminActions, s.adminAction(value.TenantID, actor, domain.AdminActionRestoreApproved, "restore", runID, value.OperationID))
		} else {
			value.Status = domain.RestoreStatusRejected
			value.RejectedBy = actor
			value.RejectedAt = &now
			data.AdminActions = append(data.AdminActions, s.adminAction(value.TenantID, actor, domain.AdminActionRestoreRejected, "restore", runID, value.OperationID))
		}
		data.RestoreRuns[runID] = value
		run = value
		return nil
	})
	return run, err
}

func (s *Service) GetRestore(tenantID, runID string) (domain.RestoreRun, error) {
	var run domain.RestoreRun
	err := s.Store.Read(func(data *store.Snapshot) error {
		value, ok := data.RestoreRuns[runID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		run = value
		return nil
	})
	return run, err
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
		digest, err := security.SearchDigest(value, s.Config.EncryptionKey)
		if err != nil {
			return err
		}
		event.Payload[field+"__search_digest"] = digest
	}
	return nil
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

func (s *Service) sealSegment(stream store.StreamState, now time.Time) (domain.Segment, domain.Checkpoint, error) {
	hashes := append([]string(nil), stream.PendingHashes...)
	root := merkleRoot(hashes)
	manifest := fmt.Sprintf("%s:%d:%d:%s:%s", stream.TenantID, stream.NextSequence-int64(len(hashes)), stream.NextSequence-1, stream.HeadHash, root)
	manifestHash := domain.HashBytes([]byte(manifest))
	signature, err := s.Config.Signer.Sign([]byte(manifestHash))
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

func (s *Service) runExport(jobID string) {
	var job domain.ExportJob
	if err := s.Store.Read(func(data *store.Snapshot) error {
		var ok bool
		job, ok = data.Exports[jobID]
		if !ok {
			return domain.ErrNotFound
		}
		return nil
	}); err != nil {
		return
	}
	_ = s.Store.Update(func(data *store.Snapshot) error {
		value := data.Exports[jobID]
		value.Status = "running"
		data.Exports[jobID] = value
		return nil
	})
	events, err := s.eventsFor(job.TenantID, func(event domain.Event) bool { return matches(event, job.Query) })
	if err != nil {
		s.finishExport(jobID, "failed", "", "", 0, err.Error())
		return
	}
	sortEvents(events)
	var bytesWritten []byte
	for _, event := range events {
		line, marshalErr := domain.CanonicalJSON(event)
		if marshalErr != nil {
			s.finishExport(jobID, "failed", "", "", 0, marshalErr.Error())
			return
		}
		bytesWritten = append(bytesWritten, line...)
		bytesWritten = append(bytesWritten, '\n')
	}
	key := fmt.Sprintf("exports/%s.jsonl", safeName(jobID))
	if err := s.Config.Archive.Put(context.Background(), key, bytesWritten); err != nil {
		s.finishExport(jobID, "failed", "", "", 0, err.Error())
		return
	}
	s.finishExport(jobID, "completed", key, domain.HashBytes(bytesWritten), len(events), "")
}

func (s *Service) finishExport(jobID, status, path, digest string, count int, failure string) {
	_ = s.Store.Update(func(data *store.Snapshot) error {
		job := data.Exports[jobID]
		now := s.Now()
		job.Status, job.FinishedAt, job.ObjectPath, job.Digest, job.EventCount, job.Error = status, &now, path, digest, count, failure
		data.Exports[jobID] = job
		return nil
	})
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

func matches(event domain.Event, query domain.Query) bool {
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
		if query.PayloadDigest == "" || event.Payload[query.PayloadField+"__search_digest"] != query.PayloadDigest {
			return false
		}
	}
	if query.StreamID != "" && event.StreamID != query.StreamID {
		return false
	}
	return true
}

func holdMatchesEvent(hold domain.LegalHold, event domain.Event) bool {
	filter := hold.Filter
	if filter.From.IsZero() && filter.To.IsZero() {
		return true
	}
	if filter.From.IsZero() || filter.To.IsZero() {
		return false
	}
	return matches(event, filter)
}

func appendUnique(values []string, wanted string) []string {
	for _, value := range values {
		if value == wanted {
			return values
		}
	}
	return append(values, wanted)
}

func sortEvents(events []domain.Event) {
	sort.Slice(events, func(i, j int) bool {
		if events[i].OccurredAt.Equal(events[j].OccurredAt) {
			if events[i].Sequence == events[j].Sequence {
				return events[i].EventID < events[j].EventID
			}
			return events[i].Sequence < events[j].Sequence
		}
		return events[i].OccurredAt.Before(events[j].OccurredAt)
	})
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
	if err := json.Unmarshal(encoded, &copyPayload); err != nil {
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
func (s *Service) ListAdminActions(tenantID string, platform bool, limit int) ([]domain.AdminAction, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	var result []domain.AdminAction
	err := s.Store.Read(func(data *store.Snapshot) error {
		for i := len(data.AdminActions) - 1; i >= 0 && len(result) < limit; i-- {
			action := data.AdminActions[i]
			if platform || action.TenantID == tenantID {
				result = append(result, action)
			}
		}
		return nil
	})
	return result, err
}

// Marshal is kept here as a small compile-time guard that the domain model
// remains JSON serializable for the file store and export path.
func Marshal(value any) ([]byte, error) { return json.Marshal(value) }
