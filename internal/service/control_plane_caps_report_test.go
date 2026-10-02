package service

// Measurement artifact for the control-plane resource caps (A1). The walker
// below is deliberately TEST-ONLY: the control plane has no non-HTTP writer
// besides cmd/audit-api bootstrap and tests, so this must not become shipped
// surface. It records what the shipped caps (domain.MaxSourceClientIDs,
// domain.MaxSchemaFields, domain.MaxEnvelopeFieldBytes) are sized against, and
// docs/BENCHMARKS.md carries the numbers plus the derivation rule.
//
// The same walker runs over two inputs:
//
//   - the in-repository fixture (benchService + injected 1/8/64/over-cap rows),
//     whose computed fields are asserted so the report itself is pinned; and
//   - an operator-supplied production snapshot, via AUDIT_CAPS_STATE (a
//     state.json path) or AUDIT_CAPS_STATE_GLOB. Without it the
//     production-snapshot subtest SKIPS and prints the exact command — the
//     skip is recorded in docs/BENCHMARKS.md, never silent.
//
// The snapshot is read as plain JSON rather than through store.Open: the file
// backend takes a process-lifetime exclusive flock on <path>.lock, so a
// running audit-api would make store.Open fail and a copy of the state
// directory would be mandatory.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// controlPlaneCapsState is the minimum document shape the walker needs.
type controlPlaneCapsState struct {
	Sources map[string]domain.SourceSystem `json:"sources"`
	Schemas map[string]domain.EventSchema  `json:"schemas"`
}

// controlPlaneListStats is one row of the distribution table. Empty lists are
// counted (lists, entries_total) and never skipped: a list nobody declares is
// itself a fact an operator needs when sizing a cap.
type controlPlaneListStats struct {
	Lists          int
	EntriesTotal   int
	MaxCount       int
	MaxEntryBytes  int
	MaxTotalBytes  int
	CountP50       int
	CountP95       int
	EntryBytesP50  int
	EntryBytesP95  int
	OverCap        int
	OverCapEntries int
}

func (s controlPlaneListStats) String() string {
	return fmt.Sprintf("lists=%d entries=%d max_count=%d p95_count=%d max_entry_bytes=%d p95_entry_bytes=%d max_total_bytes=%d over_cap=%d",
		s.Lists, s.EntriesTotal, s.MaxCount, s.CountP95, s.MaxEntryBytes, s.EntryBytesP95, s.MaxTotalBytes, s.OverCap)
}

// controlPlaneListKinds is the fixed reporting order.
var controlPlaneListKinds = []string{"allowed_client_ids", "required_fields", "allowed_fields", "encrypted_fields", "searchable_fields"}

func controlPlaneListReport(state controlPlaneCapsState) map[string]controlPlaneListStats {
	report := map[string]controlPlaneListStats{}
	for _, kind := range controlPlaneListKinds {
		cap := domain.MaxSchemaFields
		if kind == "allowed_client_ids" {
			cap = domain.MaxSourceClientIDs
		}
		stats := controlPlaneListStats{}
		for _, list := range controlPlaneListsOfKind(state, kind) {
			stats.Lists++
			stats.EntriesTotal += len(list)
			stats.MaxCount = maxInt(stats.MaxCount, len(list))
			stats.MaxEntryBytes = maxInt(stats.MaxEntryBytes, longestEntry(list))
			total := 0
			for _, entry := range list {
				total += len(entry)
			}
			stats.MaxTotalBytes = maxInt(stats.MaxTotalBytes, total)
			if len(list) > cap {
				stats.OverCap++
				stats.OverCapEntries += len(list) - cap
			}
		}
		stats.CountP50 = percentileInt(countsOfKind(state, kind), 0.50)
		stats.CountP95 = percentileInt(countsOfKind(state, kind), 0.95)
		stats.EntryBytesP50 = percentileInt(allEntryBytes(state, kind), 0.50)
		stats.EntryBytesP95 = percentileInt(allEntryBytes(state, kind), 0.95)
		report[kind] = stats
	}
	return report
}

// controlPlaneListsOfKind returns every list of one kind across the snapshot.
func controlPlaneListsOfKind(state controlPlaneCapsState, kind string) [][]string {
	var lists [][]string
	if kind == "allowed_client_ids" {
		for _, key := range sortedKeys(state.Sources) {
			lists = append(lists, state.Sources[key].AllowedClientIDs)
		}
		return lists
	}
	for _, key := range sortedKeys(state.Schemas) {
		schema := state.Schemas[key]
		switch kind {
		case "required_fields":
			lists = append(lists, schema.RequiredFields)
		case "allowed_fields":
			lists = append(lists, schema.AllowedFields)
		case "encrypted_fields":
			lists = append(lists, schema.EncryptedFields)
		case "searchable_fields":
			lists = append(lists, schema.SearchableFields)
		}
	}
	return lists
}

func countsOfKind(state controlPlaneCapsState, kind string) []int {
	counts := []int{}
	for _, list := range controlPlaneListsOfKind(state, kind) {
		counts = append(counts, len(list))
	}
	return counts
}

func allEntryBytes(state controlPlaneCapsState, kind string) []int {
	sizes := []int{}
	for _, list := range controlPlaneListsOfKind(state, kind) {
		for _, entry := range list {
			sizes = append(sizes, len(entry))
		}
	}
	return sizes
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// percentileInt uses the nearest-rank definition: the smallest value at or
// above the quantile's rank (ceil(q*n), 1-indexed). Empty input is 0.
func percentileInt(values []int, quantile float64) int {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int(nil), values...)
	sort.Ints(sorted)
	rank := int(float64(len(sorted))*quantile + 0.9999999999)
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func longestEntry(entries []string) int {
	longest := 0
	for _, entry := range entries {
		if len(entry) > longest {
			longest = len(entry)
		}
	}
	return longest
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// TestControlPlaneListDistributionReport pins the walker over the repository
// fixture and prints the table the docs entry quotes.
func TestControlPlaneListDistributionReport(t *testing.T) {
	svc := benchService(t)
	// One row per shape an operator would plausibly register: 1, 8, 64 and an
	// over-cap allow-list, each with a fresh schema version carrying lists of
	// 1, 8, 64 and MaxSchemaFields+1 entries. The over-cap rows are injected
	// straight into the snapshot — the write path rejects them by design, and
	// a pre-cap deployment is exactly how they get into a state file.
	for i, count := range []int{1, 8, 64} {
		if err := svc.AddSource(testCtx, "measure", domain.SourceSystem{TenantID: "tenant-a",
			ID: fmt.Sprintf("measured-%d", i), Name: "Measured", Active: true,
			AllowedClientIDs: sequentialIDs(count)}); err != nil {
			t.Fatalf("measured source %d entries: %v", count, err)
		}
		if err := svc.RegisterSchema(testCtx, "measure", domain.EventSchema{TenantID: "tenant-a",
			SchemaID: "measured.schema", Version: i + 1, EventType: "audit.event", Active: true,
			RequiredFields:   sequentialIDs(count),
			AllowedFields:    sequentialIDs(count),
			EncryptedFields:  sequentialIDs(count),
			SearchableFields: sequentialIDs(count),
		}); err != nil {
			t.Fatalf("measured schema %d entries: %v", count, err)
		}
	}
	overCap := domain.MaxSchemaFields + 1
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		data.Sources[store.SourceKey("tenant-a", "measured-over-cap")] = domain.SourceSystem{
			TenantID: "tenant-a", ID: "measured-over-cap", Name: "Measured", Active: true,
			AllowedClientIDs: sequentialIDs(overCap)}
		data.Schemas[store.SchemaKey("tenant-a", "measured.schema", 4)] = domain.EventSchema{
			TenantID: "tenant-a", SchemaID: "measured.schema", Version: 4, EventType: "audit.event", Active: true,
			RequiredFields:   sequentialIDs(overCap),
			AllowedFields:    sequentialIDs(overCap),
			EncryptedFields:  sequentialIDs(overCap),
			SearchableFields: sequentialIDs(overCap)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	state := controlPlaneCapsState{}
	if err := svc.Store.Read(testCtx, func(data *store.Snapshot) error {
		state.Sources = data.Sources
		state.Schemas = data.Schemas
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	report := controlPlaneListReport(state)
	t.Logf("%-18s %s", "list", "measured distribution")
	for _, kind := range controlPlaneListKinds {
		t.Logf("%-18s %s", kind, report[kind])
	}

	sourceLists := controlPlaneListsOfKind(state, "allowed_client_ids")
	if got, want := report["allowed_client_ids"].Lists, len(sourceLists); got != want {
		t.Fatalf("allowed_client_ids lists=%d, want %d", got, want)
	}
	// benchService registers crm with no allow-list; the fixture adds 1/8/64 and
	// one over-cap source, so the maximum count is the over-cap row.
	if got, want := report["allowed_client_ids"].MaxCount, domain.MaxSourceClientIDs+1; got != want {
		t.Fatalf("allowed_client_ids max_count=%d, want %d", got, want)
	}
	if got := report["allowed_client_ids"].OverCap; got != 1 {
		t.Fatalf("allowed_client_ids over_cap rows=%d, want 1", got)
	}
	if got, want := report["allowed_client_ids"].EntryBytesP95, len("client-000255"); got != want {
		t.Fatalf("allowed_client_ids p95 entry bytes=%d, want %d", got, want)
	}
	schemaReport := report["required_fields"]
	if got, want := schemaReport.Lists, 5; got != want {
		t.Fatalf("required_fields lists=%d, want %d (audit.event v1 + four measured versions)", got, want)
	}
	// audit.event v1 contributes 1 entry; the measured versions contribute
	// 1, 8, 64 and the injected over-cap row 257.
	if got, want := schemaReport.EntriesTotal, 1+1+8+64+(domain.MaxSchemaFields+1); got != want {
		t.Fatalf("required_fields entries_total=%d, want %d", got, want)
	}
	// Nearest-rank p50/p95 over [1,1,8,64,257]: rank 3 and rank 5.
	if got, want := schemaReport.CountP50, 8; got != want {
		t.Fatalf("required_fields p50 count=%d, want %d", got, want)
	}
	if got, want := schemaReport.CountP95, domain.MaxSchemaFields+1; got != want {
		t.Fatalf("required_fields p95 count=%d, want %d", got, want)
	}
	if got, want := schemaReport.MaxTotalBytes, (domain.MaxSchemaFields+1)*len("client-000256"); got != want {
		t.Fatalf("required_fields max_total_bytes=%d, want %d", got, want)
	}
	// allowed_fields carries one extra entry (audit.event v1 declares two), and
	// encrypted/searchable_fields one fewer (v1 declares none); their maxima,
	// percentiles and over-cap counts are identical.
	if got, want := report["allowed_fields"].EntriesTotal, schemaReport.EntriesTotal+1; got != want {
		t.Fatalf("allowed_fields entries_total=%d, want %d", got, want)
	}
	if got, want := report["encrypted_fields"].EntriesTotal, schemaReport.EntriesTotal-1; got != want {
		t.Fatalf("encrypted_fields entries_total=%d, want %d", got, want)
	}
	for _, kind := range []string{"encrypted_fields", "searchable_fields"} {
		if report[kind] != report["encrypted_fields"] {
			t.Fatalf("%s distribution differs from encrypted_fields: %+v vs %+v", kind, report[kind], report["encrypted_fields"])
		}
	}

	t.Run("production-snapshot", func(t *testing.T) {
		paths := productionSnapshotPaths(t)
		if len(paths) == 0 {
			t.Skipf("no production snapshot configured; run against a copy of a deployment state directory with:\n" +
				"  cp -r /var/lib/audit-governance ./state-copy   # the file backend flocks <path>.lock\n" +
				"  AUDIT_CAPS_STATE=./state-copy/state.json go test ./internal/service -run TestControlPlaneListDistributionReport -v\n" +
				"  AUDIT_CAPS_STATE_GLOB='/srv/*/state.json' go test ./internal/service -run TestControlPlaneListDistributionReport -v")
		}
		for _, path := range paths {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			var snapshot controlPlaneCapsState
			if err := json.Unmarshal(raw, &snapshot); err != nil {
				t.Fatalf("decode %s: %v", path, err)
			}
			measured := controlPlaneListReport(snapshot)
			t.Logf("snapshot %s", path)
			for _, kind := range controlPlaneListKinds {
				t.Logf("%-18s %s", kind, measured[kind])
			}
			for _, kind := range controlPlaneListKinds {
				if measured[kind].OverCap != 0 {
					t.Errorf("%s: %d stored lists exceed the shipped cap (%d entries above); "+
						"they keep serving, but their first rewrite now fails — trim them or widen the cap with evidence",
						kind, measured[kind].OverCap, measured[kind].OverCapEntries)
				}
			}
		}
	})
}

func productionSnapshotPaths(t *testing.T) []string {
	t.Helper()
	if path := os.Getenv("AUDIT_CAPS_STATE"); path != "" {
		return []string{path}
	}
	pattern := os.Getenv("AUDIT_CAPS_STATE_GLOB")
	if pattern == "" {
		return nil
	}
	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("AUDIT_CAPS_STATE_GLOB=%q: %v", pattern, err)
	}
	return matches
}

// BenchmarkControlPlaneListDistribution reports the max/p95 counts the cap
// values are derived from, so `python3 cli.py bench` emits the distribution.
func BenchmarkControlPlaneListDistribution(b *testing.B) {
	svc := benchService(b)
	state := controlPlaneCapsState{}
	if err := svc.Store.Read(testCtx, func(data *store.Snapshot) error {
		state.Sources = data.Sources
		state.Schemas = data.Schemas
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	report := controlPlaneListReport(state)
	b.ResetTimer()
	// ReportMetric must come after ResetTimer: ResetTimer clears the metric map.
	for _, kind := range controlPlaneListKinds {
		stats := report[kind]
		b.ReportMetric(float64(stats.MaxCount), "max_count["+kind+"]")
		b.ReportMetric(float64(stats.CountP95), "p95_count["+kind+"]")
		b.ReportMetric(float64(stats.MaxTotalBytes), "max_total_bytes["+kind+"]")
	}
	for i := 0; i < b.N; i++ {
		if measured := controlPlaneListReport(state); measured["allowed_client_ids"].Lists != report["allowed_client_ids"].Lists {
			b.Fatalf("report is not deterministic: %+v", measured["allowed_client_ids"])
		}
	}
}
