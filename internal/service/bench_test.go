package service

import (
	"fmt"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// benchService builds the same fixture as testService but for benchmarks.
func benchService(b *testing.B) *Service {
	b.Helper()
	st, err := store.Open("")
	if err != nil {
		b.Fatal(err)
	}
	svc, err := New(st, Config{SegmentSize: 100, Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		b.Fatal(err)
	}
	if err := svc.CreateTenant("bench", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		b.Fatal(err)
	}
	if err := svc.AddSource("bench", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		b.Fatal(err)
	}
	if err := svc.RegisterSchema("bench", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		b.Fatal(err)
	}
	return svc
}

// BenchmarkIngest measures single-event ingestion: schema validation,
// canonical encoding, sensitive-field scan, hashing and stream linkage.
func BenchmarkIngest(b *testing.B) {
	svc := benchService(b)
	base := time.Unix(1_700_000_010, 0).UTC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			event := domain.Event{EventID: fmt.Sprintf("bench-%d", i), SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: base, Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: fmt.Sprintf("idem-%d", i), Payload: map[string]any{"resource": "invoice", "value": 10}}
			if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, ""); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkQuery measures a filtered time-range query over a warm ledger.
func BenchmarkQuery(b *testing.B) {
	svc := benchService(b)
	base := time.Unix(1_700_000_010, 0).UTC()
	for i := 0; i < 1000; i++ {
		event := domain.Event{EventID: fmt.Sprintf("q-%d", i), SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: base.Add(time.Duration(i) * time.Second), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: fmt.Sprintf("q-idem-%d", i), Payload: map[string]any{"resource": "invoice", "value": i}}
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, ""); err != nil {
			b.Fatal(err)
		}
	}
	query := domain.Query{From: base, To: base.Add(2000 * time.Second), EventType: "audit.event", PageSize: 100}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := svc.QueryEvents("tenant-a", "bench", query); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEventDigest measures canonical encoding and hashing alone.
func BenchmarkEventDigest(b *testing.B) {
	event := domain.Event{EventID: "digest-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "idem", Payload: map[string]any{"resource": "invoice", "value": 10}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := domain.EventDigest(event); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueryLargeLedger measures filtered queries over a 5,000-event
// ledger. The snapshot is built in one update (not through per-event ingest,
// whose O(n^2) read-modify-write cost is documented in BENCHMARKS.md); the
// query path itself remains O(n) per filter.
func BenchmarkQueryLargeLedger(b *testing.B) {
	svc := benchService(b)
	base := time.Unix(1_700_000_010, 0).UTC()
	now := svc.Now()
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		for i := 0; i < 5000; i++ {
			event := domain.Event{EventID: fmt.Sprintf("big-%d", i), TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: base.Add(time.Duration(i) * time.Second), ReceivedAt: now, Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: fmt.Sprintf("big-idem-%d", i), Payload: map[string]any{"resource": "invoice", "value": i}, StreamID: "tenant-a:source:crm", Sequence: int64(i + 1), Hash: fmt.Sprintf("hash-%d", i)}
			key := store.EventKey("tenant-a", event.EventID)
			data.Events[key] = event
			data.Receipts[key] = domain.EventReceipt{EventID: event.EventID, TenantID: "tenant-a", Status: domain.StatusLedgered, AcceptedAt: now, StreamID: event.StreamID, Sequence: event.Sequence, Hash: event.Hash}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	query := domain.Query{From: base, To: base.Add(6000 * time.Second), EventType: "audit.event", PageSize: 100}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := svc.QueryEvents("tenant-a", "bench", query); err != nil {
			b.Fatal(err)
		}
	}
}
