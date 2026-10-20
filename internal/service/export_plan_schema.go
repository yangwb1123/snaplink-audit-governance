package service

import "github.com/snaplink/audit-governance/internal/store"

// Schema bindings are checked at both selected/sealed control transitions, so
// a concurrent catalog edit cannot silently change the execution version. Each
// distinct schema is hashed once, not once per event; no remote I/O in the CAS.
func (s *Service) exportPlanSchemasMatch(data *store.Snapshot, plan store.ExportPlan) bool {
	digests := make(map[string]string)
	for _, member := range plan.Members {
		key := store.SchemaKey(plan.TenantID, member.SchemaID, member.SchemaVersion)
		digest, found := digests[key]
		if !found {
			schema := data.Schemas[key]
			if schema.TenantID != plan.TenantID || schema.SchemaID != member.SchemaID || schema.Version != member.SchemaVersion {
				return false
			}
			digest = s.exportPlanSchemaDigest(schema)
			digests[key] = digest
		}
		if digest != member.SchemaDigest {
			return false
		}
	}
	return true
}
