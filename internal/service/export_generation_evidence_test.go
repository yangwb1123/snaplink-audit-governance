package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportGenerationMetadataBindingsBudgetsAndDeepCopies(t *testing.T) {
	svc, _, _ := newGenerationService(t)
	claim := leaveGeneratedPage(t, svc)
	cases := map[string]func(*store.ExportPlan){
		"unknown-version":      func(p *store.ExportPlan) { p.Version = 99 },
		"legacy-with-progress": func(p *store.ExportPlan) { p.Version = 1 },
		"seal-version":         func(p *store.ExportPlan) { p.Chunks[0].SealVersion = 1 },
		"gap":                  func(p *store.ExportPlan) { p.Chunks[0].First++ },
		"short-page":           func(p *store.ExportPlan) { p.Chunks[0].Count-- },
		"long-page":            func(p *store.ExportPlan) { p.Chunks[0].Count++ },
		"foreign-path":         func(p *store.ExportPlan) { p.Chunks[0].ObjectPath = "export-chunks/other/0000-attempt.bin" },
		"plain-digest":         func(p *store.ExportPlan) { p.Chunks[0].Digest = "" },
		"sealed-digest":        func(p *store.ExportPlan) { p.Chunks[0].SealedDigest = "" },
		"future":               func(p *store.ExportPlan) { p.Chunks[0].CreatedAt = svc.Now().Add(time.Second) },
		"pre-plan":             func(p *store.ExportPlan) { p.Chunks[0].CreatedAt = p.CreatedAt.Add(-time.Second) },
		"repeat":               func(p *store.ExportPlan) { p.Chunks = append(p.Chunks, p.Chunks[0]) },
		"overflow":             func(p *store.ExportPlan) { p.Chunks = append(p.Chunks, p.Chunks[0], p.Chunks[0]) },
		"definition":           func(p *store.ExportPlan) { p.Members[0].SchemaDigest = strings.Repeat("a", 64) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			plan := copyExportPlan(claim.Plan)
			mutate(&plan)
			plan.Signature = svc.signExportPlan(plan) // even with an authentic MAC
			if svc.validExportPlan(claim.ExportJob, plan, svc.Now()) {
				t.Fatal("malformed progress accepted")
			}
		})
	}
	plan := copyExportPlan(claim.Plan)
	plan.Chunks[0].Digest = strings.Repeat("0", 64)
	if svc.validExportPlan(claim.ExportJob, plan, svc.Now()) || reflect.DeepEqual(plan.Chunks, claim.Plan.Chunks) {
		t.Fatal("progress mutation kept its MAC or aliased the captured plan")
	}
	if err := svc.Store.ReadControl(testCtx, func(data *store.Snapshot) error {
		data.ExportPlans[claim.ID].Chunks[0].Count = 0
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Store.UpdateControlChecked(testCtx, func(data *store.Snapshot) (bool, error) {
		data.ExportPlans[claim.ID].Chunks[0].Digest = "changed"
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(leaseSnapshot(t, svc).ExportPlans[claim.ID], claim.Plan) {
		t.Fatal("read/no-op mutated durable progress")
	}
	encoded, err := json.Marshal(claim.Plan.Chunks[0])
	if err != nil || len(encoded)+1 > 1024 {
		t.Fatal("page reference exceeded reserved metadata budget")
	}
	if svc.exportGenerationID(claim.Plan) == claim.Plan.Binding || svc.exportGenerationID(claim.Plan) == claim.Plan.Signature {
		t.Fatal("generation reused another HMAC purpose")
	}
}

type generationTamperArchive struct {
	archive.Store
	raw     []byte
	missing bool
}

func (a *generationTamperArchive) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "export-chunks/") {
		if a.missing {
			return nil, errors.New("injected missing committed page")
		}
		return append([]byte(nil), a.raw...), nil
	}
	return a.Store.Get(ctx, key)
}

func TestExportGenerationMissingCorruptOrForeignPageNeverRegeneratesCommittedProgress(t *testing.T) {
	svc, _, objects := newGenerationService(t)
	old := leaveGeneratedPage(t, svc)
	raw, err := objects.Get(testCtx, old.Plan.Chunks[0].ObjectPath)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := svc.authenticateExportChunk(old.Plan, 0, raw)
	if err != nil {
		t.Fatal("real committed page failed authentication")
	}
	for _, mode := range []string{"missing", "ciphertext", "digest", "legacy", "foreign-job", "foreign-plan", "jsonl-identity", "jsonl-order"} {
		t.Run(mode, func(t *testing.T) {
			plan := copyExportPlan(old.Plan)
			sealed := append([]byte(nil), raw...)
			chunk := plan.Chunks[0]
			switch mode {
			case "ciphertext":
				sealed[len(sealed)-1] ^= 1
			case "digest":
				plan.Chunks[0].Digest = strings.Repeat("a", 64)
			case "legacy":
				sealed, err = security.EncryptBytes(plain, svc.Config.EncryptionKey)
			case "foreign-job":
				sealed, err = security.EncryptBytesBound(plain, svc.Config.EncryptionKey, security.ExportBinding(old.TenantID, "other-job"))
			case "foreign-plan":
				foreign := copyExportPlan(plan)
				foreign.FormatVersion++
				sealed, err = security.EncryptBytesBound(plain, svc.Config.EncryptionKey, svc.exportChunkBinding(foreign, 0, chunk))
			case "jsonl-identity", "jsonl-order":
				lines := bytes.Split(plain, []byte{'\n'})
				if mode == "jsonl-order" {
					lines[0], lines[1] = lines[1], lines[0]
				} else {
					lines[0] = bytes.Replace(lines[0], []byte(`"tenant_id":"tenant-a"`), []byte(`"tenant_id":"tenant-b"`), 1)
				}
				changed := bytes.Join(lines, []byte{'\n'})
				plan.Chunks[0].Digest = domain.HashBytes(changed)
				sealed, err = security.EncryptBytesBound(changed, svc.Config.EncryptionKey, svc.exportChunkBinding(plan, 0, chunk))
			}
			if err != nil {
				t.Fatal(err)
			}
			// Except the explicit digest mismatch, recompute the sealed digest
			// and MAC to exercise AEAD/identity checks, not just hash rejection.
			plan.Chunks[0].SealedDigest = domain.HashBytes(sealed)
			plan.Signature = svc.signExportPlan(plan)
			if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error { data.ExportPlans[old.ID] = plan; return nil }); err != nil {
				t.Fatal(err)
			}
			expireCheckpoint(svc)
			probe := &generationArchive{Store: &generationTamperArchive{Store: objects, raw: sealed, missing: mode == "missing"}, forbiddenSources: generatedSourceKeys(plan)}
			svc.Config.Archive = probe
			svc.runExport(testCtx, old.ID)
			data := leaseSnapshot(t, svc)
			if data.Exports[old.ID].Status != "running" || data.Exports[old.ID].ObjectPath != "" || !reflect.DeepEqual(data.ExportPlans[old.ID], plan) || probe.sourceGets.Load() != 0 || probe.chunkGets.Load() != 1 || probe.chunkPuts.Load() != 0 || probe.finalPuts.Load() != 0 {
				t.Fatal("bad committed page was discarded, regenerated or published")
			}
		})
	}
	if checkpointFactCount(t, svc, domain.AdminActionExportProgressed) != 1 || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 0 {
		t.Fatal("failed page authentication invented progress or completion facts")
	}
}
