package service

// Ingest validation and the bytes that back an audit fact: schema and
// source-binding validation, sensitive-field protection, canonical content
// digest derivation, the per-event hash, and the archive writes for events
// and sealed segments. Ingest (service.go) decides the order; the rules live
// here so the ledger's tamper-evidence rules can be read in one place.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

func (s *Service) validateEvent(ctx context.Context, tenantID string, principal domain.IngestPrincipal, event domain.Event) (domain.EventSchema, error) {
	var schema domain.EventSchema
	err := s.Store.ReadControl(ctx, func(data *store.Snapshot) error {
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
	reconstructedDigestEvent, err := s.reconstructEventForDigest(event, schemas)
	if err != nil {
		return fmt.Errorf("stream %s sequence %d content digest check failed: %w", event.StreamID, event.Sequence, err)
	}
	encoding, err := domain.MatchEventContentDigest(reconstructedDigestEvent, event.SourceDigest)
	if err != nil {
		return fmt.Errorf("stream %s sequence %d content digest check failed: %w", event.StreamID, event.Sequence, err)
	}
	if encoding == domain.DigestEncodingNone {
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
// Number-encoding consistency note: reconstruction must preserve numbers
// exactly like the ingest-time digest path. Both sides decode through
// json.Number (clonePayload here and DecryptJSON), so int64 values > 2^53
// keep their exact digits and VerifyIntegrity agrees with the stored digest.
func (s *Service) reconstructAndDerive(event domain.Event, schemas map[string]domain.EventSchema) (string, error) {
	reconstructed, err := s.reconstructEventForDigest(event, schemas)
	if err != nil {
		return "", err
	}
	return domain.EventContentDigest(reconstructed)
}

func (s *Service) reconstructEventForDigest(event domain.Event, schemas map[string]domain.EventSchema) (domain.Event, error) {
	schema, ok := schemas[store.SchemaKey(event.TenantID, event.SchemaID, event.SchemaVersion)]
	if !ok {
		return domain.Event{}, fmt.Errorf("schema %s v%d not found", event.SchemaID, event.SchemaVersion)
	}
	reconstructed, err := domain.CloneEvent(event)
	if err != nil {
		return domain.Event{}, err
	}
	for _, field := range schema.SearchableFields {
		delete(reconstructed.Payload, field+security.SearchDigestSuffix)
	}
	for _, field := range schema.EncryptedFields {
		encoded, ok := reconstructed.Payload[field].(string)
		if !ok {
			continue
		}
		original, err := security.DecryptJSON(encoded, s.Config.EncryptionKey, event.TenantID+"/"+field+"/"+event.EventID)
		if err != nil {
			return domain.Event{}, fmt.Errorf("cannot decrypt field %s: %w", field, err)
		}
		reconstructed.Payload[field] = original
	}
	return reconstructed, nil
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

// archivePutTimeout bounds every archive object write (worker passes and API
// ingest commits) so a hung or black-holed S3 endpoint cannot block a pass or
// an ingest commit indefinitely. Each object receives a fresh deadline, and
// an earlier caller deadline, such as a signal-cancelled pass, still wins.
const archivePutTimeout = 5 * time.Second

func (s *Service) archiveEvent(ctx context.Context, event domain.Event) error {
	data, err := domain.CanonicalJSON(event)
	if err != nil {
		return err
	}
	key := eventArchiveKey(event.TenantID, event.StreamID, event.Sequence, event.EventID)
	// The archive store's pre-flight checkKeyLength rejects an over-limit
	// composite with the typed archive.ErrArchiveKeyTooLong before any
	// filesystem mutation or network call (FM-1); ArchivePending dead-letters
	// that class instead of aborting the pass. The ctx threads the worker pass
	// deadline (and the ingest boundary's WithoutCancel ctx) into the S3 round
	// trips, while this per-object deadline bounds each round trip.
	putCtx, cancel := context.WithTimeout(ctx, archivePutTimeout)
	defer cancel()
	return s.Config.Archive.Put(putCtx, key, data)
}

func (s *Service) archiveSegment(ctx context.Context, segment domain.Segment) error {
	data, err := domain.CanonicalJSON(segment)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("segments/%s/%s/%020d-%020d.manifest.json", safeName(segment.TenantID), safeName(segment.StreamID), segment.FirstSequence, segment.LastSequence)
	putCtx, cancel := context.WithTimeout(ctx, archivePutTimeout)
	defer cancel()
	return s.Config.Archive.Put(putCtx, key, data)
}
