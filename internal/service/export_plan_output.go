package service

import (
	"bytes"
	"encoding/json"

	"github.com/snaplink/audit-governance/internal/store"
)

// Check the generated artifact against the selected stage before advancing to
// the sealed checkpoint. Decode identity/version only, never another payload
// history. Authentication of content/chain already preceded generation.
func exportPlainMatchesPlan(plain []byte, plan store.ExportPlan) bool {
	if len(plan.Members) == 0 {
		return len(plain) == 0
	}
	lines := bytes.Split(plain, []byte{'\n'})
	if len(lines) != len(plan.Members)+1 || len(lines[len(lines)-1]) != 0 {
		return false
	}
	for i, line := range lines[:len(lines)-1] {
		var identity struct {
			store.EventReference
			TenantID      string `json:"tenant_id"`
			SchemaID      string `json:"schema_id"`
			SchemaVersion int    `json:"schema_version"`
		}
		member := plan.Members[i]
		if json.Unmarshal(line, &identity) != nil || identity.TenantID != plan.TenantID ||
			identity.EventID != member.EventID || identity.StreamID != member.StreamID || identity.Sequence != member.Sequence ||
			identity.Hash != member.Hash || !identity.OccurredAt.Equal(member.OccurredAt) ||
			identity.SchemaID != member.SchemaID || identity.SchemaVersion != member.SchemaVersion {
			return false
		}
	}
	return true
}
