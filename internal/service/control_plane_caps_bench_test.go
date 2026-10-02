package service

// Per-ingest cost of the control-plane lists the caps bound. allowed_client_ids
// is scanned by SourceSystem.AllowsClient on every ingested event (and once per
// Snapshot.Sources entry in the platform sweep), and each EventSchema field
// list is walked by validateEvent, protectSensitiveFields and
// reconstructEventForDigest per event. Without an arity cap those walks are
// O(caller-controlled length); these benchmarks and the two guard tests make
// that shape measurable instead of asserted.
//
// Cost guard style follows TestIngestCostIndependentOfArchivedEvents: a
// machine-independent allocation-parity assertion is load-bearing, while the
// wall-clock guards are same-process ratios plus a generous absolute ceiling,
// so a shared CI runner cannot turn the gate into a flake source.

import (
	"strconv"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// clientAuthorizationCeiling bounds one AllowsClient scan at the shipped cap.
// Derivation: BenchmarkIngest measures ~1.27 ms/op for a whole ingest on the
// reference machine, so 300 µs is ~25% of a full ingest — more than three
// orders of magnitude above the measured cost of scanning MaxSourceClientIDs
// short strings. It is a ceiling that only trips on a pathological
// implementation, not a tight performance target.
const clientAuthorizationCeiling = 300 * time.Microsecond

// BenchmarkSourceClientIDAuthorization measures the per-ingest authorization
// scan at 1 entry, cap/4, cap/2 and the cap, plus the cap's best case
// (authorized client first) and worst case (authorized client last).
func BenchmarkSourceClientIDAuthorization(b *testing.B) {
	for _, length := range []int{1, domain.MaxSourceClientIDs / 4, domain.MaxSourceClientIDs / 2, domain.MaxSourceClientIDs} {
		for _, authorizedLast := range []bool{false, true} {
			position := "first"
			if authorizedLast {
				position = "last"
			}
			b.Run(lengthName(length)+"/authorized="+position, func(b *testing.B) {
				source := benchmarkAuthorizedSource(length, authorizedLast)
				client := source.AllowedClientIDs[0]
				if authorizedLast {
					client = source.AllowedClientIDs[length-1]
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if !source.AllowsClient(client) {
						b.Fatal("benchmark client was rejected")
					}
				}
			})
		}
	}
}

// BenchmarkSchemaFieldListWalk measures the per-event schema list walks the cap
// bounds: protectSensitiveFields (encryption plus search-digest derivation over
// encrypted/searchable fields) and reconstructEventForDigest (the same two
// lists on the verification path), at 1 entry and at the cap.
func BenchmarkSchemaFieldListWalk(b *testing.B) {
	svc := benchService(b)
	for _, length := range []int{1, domain.MaxSchemaFields} {
		schema := benchmarkFieldSchema(length)
		schemas := map[string]domain.EventSchema{store.SchemaKey("tenant-a", schema.SchemaID, schema.Version): schema}
		event := benchmarkProtectedEvent(schema.SchemaID, schema.Version)
		b.Run("protect/"+lengthName(length), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				candidate := event
				payload := map[string]any{}
				for key, value := range candidate.Payload {
					payload[key] = value
				}
				candidate.Payload = payload
				if err := svc.protectSensitiveFields(&candidate, schema); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("reconstruct/"+lengthName(length), func(b *testing.B) {
			protected := event
			if err := svc.protectSensitiveFields(&protected, schema); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := svc.reconstructEventForDigest(protected, schemas); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestSourceAuthorizationCostAtCapIsBounded is the cost gate for the shipped
// cap: scanning the maximum allow-list must not allocate more than scanning a
// single entry (a linear scan over a []string allocates nothing, which is
// machine-independent), and must stay inside the same-process ratio and the
// absolute ceiling.
func TestSourceAuthorizationCostAtCapIsBounded(t *testing.T) {
	single := benchmarkAuthorizedSource(1, false)
	atCap := benchmarkAuthorizedSource(domain.MaxSourceClientIDs, true)

	singleCost, singleAllocs := measureClientAuthorization(single, single.AllowedClientIDs[0])
	capCost, capAllocs := measureClientAuthorization(atCap, atCap.AllowedClientIDs[len(atCap.AllowedClientIDs)-1])

	if capAllocs > singleAllocs {
		t.Fatalf("scanning %d allowed_client_ids allocated: 1 entry=%.0f allocs, cap=%.0f allocs",
			domain.MaxSourceClientIDs, singleAllocs, capAllocs)
	}
	if capCost > singleCost*3+time.Millisecond {
		t.Fatalf("scanning the cap is not bounded by list length: 1 entry=%v cap=%v", singleCost, capCost)
	}
	if capCost > clientAuthorizationCeiling {
		t.Fatalf("scanning %d allowed_client_ids took %v, over the %v ceiling", domain.MaxSourceClientIDs, capCost, clientAuthorizationCeiling)
	}
}

// TestSourceAuthorizationCostGrowsWithListLength documents the O(n) shape the
// cap exists to bound: doubling the list must roughly double the scan, so a
// future regression that made the scan super-linear (or, worse, hid it behind
// a cache keyed on an unbounded list) is visible.
func TestSourceAuthorizationCostGrowsWithListLength(t *testing.T) {
	atCap := benchmarkAuthorizedSource(domain.MaxSourceClientIDs, true)
	doubleCap := benchmarkAuthorizedSource(2*domain.MaxSourceClientIDs, true)

	capCost, _ := measureClientAuthorization(atCap, atCap.AllowedClientIDs[len(atCap.AllowedClientIDs)-1])
	doubleCost, _ := measureClientAuthorization(doubleCap, doubleCap.AllowedClientIDs[len(doubleCap.AllowedClientIDs)-1])

	if doubleCost > capCost*4+time.Millisecond {
		t.Fatalf("doubling allowed_client_ids (%d -> %d) changed the scan too much: cap=%v double=%v",
			domain.MaxSourceClientIDs, 2*domain.MaxSourceClientIDs, capCost, doubleCost)
	}
}

func measureClientAuthorization(source domain.SourceSystem, client string) (time.Duration, float64) {
	allows := func() {
		if !source.AllowsClient(client) {
			panic("unreachable: benchmark client must be authorized")
		}
	}
	allocs := testing.AllocsPerRun(50, allows)
	const rounds = 2000
	start := time.Now()
	for i := 0; i < rounds; i++ {
		allows()
	}
	return time.Since(start) / rounds, allocs
}

// benchmarkAuthorizedSource builds a source whose allow-list has length
// entries; the authorized client sits at the requested end (first = best case
// for a linear scan, last = worst case).
func benchmarkAuthorizedSource(length int, authorizedLast bool) domain.SourceSystem {
	entries := sequentialIDs(length)
	authorized := entries[0]
	list := make([]string, 0, length)
	if authorizedLast {
		authorized = entries[length-1]
		list = append(list, entries[:length-1]...)
		list = append(list, authorized)
	} else {
		list = append(list, authorized)
		list = append(list, entries[1:length]...)
	}
	return domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", AllowedClientIDs: list}
}

func benchmarkFieldSchema(length int) domain.EventSchema {
	fields := sequentialIDs(length)
	return domain.EventSchema{TenantID: "tenant-a", SchemaID: "bench.schema", Version: 1, EventType: "audit.event", Active: true,
		RequiredFields:   fields[:1],
		AllowedFields:    append(append([]string(nil), fields...), "national_id", "resource", "value"),
		EncryptedFields:  append(append([]string(nil), fields...), "national_id"),
		SearchableFields: append(append([]string(nil), fields...), "resource"),
	}
}

func benchmarkProtectedEvent(schemaID string, version int) domain.Event {
	return domain.Event{EventID: "bench-walk", TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event",
		SchemaID: schemaID, SchemaVersion: version, OccurredAt: time.Unix(1_700_000_010, 0).UTC(),
		Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "bench-walk-idem",
		Payload: map[string]any{"client-000000": 1, "national_id": "123456789", "resource": "invoice", "value": 10}}
}

func lengthName(length int) string {
	if length == 1 {
		return "n=1"
	}
	return "n=" + strconv.Itoa(length)
}
