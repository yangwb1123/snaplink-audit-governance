package service

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// QueryProjectionReader is an explicitly configured candidate index. There is
// no availability fallback: unavailable/conflicting projection fails closed.
// Sensitive digest filters deliberately use the ledger adapter instead.
type QueryProjectionReader interface {
	SelectQueryReferences(context.Context, string, domain.Query) ([]store.EventReference, error)
	CheckQueryStorage(context.Context) error
}

type querySnapshotCursor struct {
	Version int    `json:"version"`
	ID      string `json:"id"`
	Binding string `json:"binding"`
	Offset  int    `json:"offset"`
}

func (s *Service) queryMAC(label string, value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	} // callers use only fixed, JSON-safe types
	mac := hmac.New(sha256.New, []byte(s.Config.SigningSecret))
	_, _ = mac.Write([]byte(label + "\x00"))
	_, _ = mac.Write(raw)
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *Service) queryBinding(tenantID, actor, source string, q domain.Query) string {
	q.Cursor = ""
	q.From, q.To = q.From.UTC(), q.To.UTC()
	return s.queryMAC("query.scope.v1", struct {
		Tenant, Actor, Source string
		Query                 domain.Query
	}{tenantID, actor, source, q})
}

func (s *Service) signQuerySnapshot(q store.QuerySnapshot) string {
	q.Signature = ""
	return s.queryMAC("query.membership.v1", q)
}

func (s *Service) encodeQueryCursor(q store.QuerySnapshot, offset int) string {
	cursor := querySnapshotCursor{Version: 1, ID: q.ID, Binding: q.Binding, Offset: offset}
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw) + "." + s.queryMAC("query.cursor.v1", cursor)
}

func (s *Service) decodeQueryCursor(raw, binding string) (querySnapshotCursor, error) {
	invalid := fmt.Errorf("%w: cursor does not match fixed query; restart query", domain.ErrInvalid)
	if len(raw) > 4096 {
		return querySnapshotCursor{}, invalid
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 2 {
		return querySnapshotCursor{}, fmt.Errorf("%w: cursor predates chronological ordering or fixed snapshots; restart query", domain.ErrInvalid)
	}
	encoded, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return querySnapshotCursor{}, invalid
	}
	var cursor querySnapshotCursor
	if json.Unmarshal(encoded, &cursor) != nil || cursor.Version != 1 || cursor.Binding != binding || cursor.Offset <= 0 || !hmac.Equal([]byte(parts[1]), []byte(s.queryMAC("query.cursor.v1", cursor))) {
		return querySnapshotCursor{}, invalid
	}
	return cursor, nil
}

func validateEventQuery(q *domain.Query) error {
	if q.PageSize <= 0 {
		q.PageSize = domain.DefaultPageSize
	}
	return validateConsoleReadQuery(*q)
}

// queryEventsSnapshot fixes membership once; subsequent pages resolve only the
// selected coordinates. No occurred_at high-water mark or SQL scan position is
// used as a snapshot: late occurrences/commits appear only in a NEW query.
func (s *Service) queryEventsSnapshot(ctx context.Context, tenantID, actor string, query domain.Query) (domain.QueryResult, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.QueryResult{}, err
	}
	if err := validateEventQuery(&query); err != nil {
		return domain.QueryResult{}, err
	}
	var result domain.QueryResult
	err := s.Store.WithOperation(ctx, func(ctx context.Context) error {
		source := "ledger"
		if s.Config.QueryProjection != nil && query.PayloadField == "" {
			source = "clickhouse"
		}
		binding := s.queryBinding(tenantID, actor, source, query)
		var snapshot store.QuerySnapshot
		offset := 0
		var events []domain.Event
		if query.Cursor != "" {
			cursor, err := s.decodeQueryCursor(query.Cursor, binding)
			if err != nil {
				return err
			}
			snapshot, err = s.Store.ReadQuerySnapshot(ctx, tenantID, cursor.ID, s.Now())
			if err != nil {
				return err
			}
			if snapshot.Binding != binding || snapshot.Source != source || !hmac.Equal([]byte(snapshot.Signature), []byte(s.signQuerySnapshot(snapshot))) || cursor.Offset >= len(snapshot.Events) {
				return store.ErrQuerySnapshotUnavailable
			}
			offset = cursor.Offset
			end := min(offset+query.PageSize, len(snapshot.Events))
			events, err = s.readQueryEvidence(ctx, tenantID, snapshot.Events[offset:end])
			if err != nil {
				return err
			}
		} else {
			var references []store.EventReference
			var err error
			if source == "clickhouse" {
				references, err = s.Config.QueryProjection.SelectQueryReferences(ctx, tenantID, query)
				if err == nil {
					err = s.verifyQueryReferences(ctx, tenantID, references)
				}
				if err != nil {
					return err
				}
				sort.Slice(references, func(i, j int) bool {
					a, b := references[i], references[j]
					if !a.OccurredAt.Equal(b.OccurredAt) {
						return a.OccurredAt.Before(b.OccurredAt)
					}
					if a.Sequence != b.Sequence {
						return a.Sequence < b.Sequence
					}
					return a.EventID < b.EventID
				})
				events, err = s.readQueryEvidence(ctx, tenantID, references[:min(len(references), query.PageSize)])
			} else {
				events, err = s.referenceQueryEvents(ctx, tenantID, query)
				if err == nil {
					sortEvents(events)
					for _, event := range events {
						references = append(references, eventReference(event))
					}
				}
			}
			if err != nil {
				return err
			}
			if len(references) > store.MaxQuerySnapshotEvents {
				return fmt.Errorf("%w: query result budget exceeded; narrow filters", domain.ErrQuotaExceeded)
			}
			now := s.Now().UTC()
			snapshot = store.QuerySnapshot{TenantID: tenantID, Binding: binding, Source: source, CreatedAt: now, ExpiresAt: now.Add(store.QuerySnapshotLifetime), Events: references}
			if len(references) > query.PageSize {
				var id [16]byte
				if _, err := rand.Read(id[:]); err != nil {
					return err
				}
				snapshot.ID = hex.EncodeToString(id[:])
				snapshot.Signature = s.signQuerySnapshot(snapshot)
				if err := s.Store.SaveQuerySnapshot(ctx, snapshot, now); err != nil {
					return err
				}
			}
			events = events[:min(len(events), query.PageSize)]
		}
		if source == "clickhouse" {
			for _, event := range events {
				if !s.matches(event, query, nil) {
					return fmt.Errorf("%w: projected filter differs from ledger evidence", domain.ErrConflict)
				}
			}
		}
		// Every page is separately audited, including retries of the same cursor.
		// Failure emits neither events nor cursor; membership is not authorization.
		if err := s.recordReadAction(ctx, tenantID, actor, domain.AdminActionEventRead, "query", "", fmt.Sprintf("from=%s to=%s", query.From.UTC().Format("2006-01-02T15:04:05Z07:00"), query.To.UTC().Format("2006-01-02T15:04:05Z07:00"))); err != nil {
			return err
		}
		result = domain.QueryResult{Items: events, Count: len(snapshot.Events), Snapshot: &domain.QuerySnapshotInfo{ObservedAt: snapshot.CreatedAt, ExpiresAt: snapshot.ExpiresAt, Source: source, Completeness: "not_asserted"}}
		if offset+len(events) < len(snapshot.Events) {
			result.NextCursor = s.encodeQueryCursor(snapshot, offset+len(events))
		}
		return nil
	})
	if err != nil {
		return domain.QueryResult{}, err
	}
	return result, nil
}

func eventReference(event domain.Event) store.EventReference {
	return store.EventReference{OccurredAt: event.OccurredAt, EventID: event.EventID, StreamID: event.StreamID, Sequence: event.Sequence, Hash: event.Hash}
}
