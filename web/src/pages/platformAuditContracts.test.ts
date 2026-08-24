import { describe, expect, it } from 'vitest'
import type { EventSchema, SourceSystem } from '../api/types'
import { assessPlatformAuditContracts } from './platformAuditContracts'

function source(id: string, clientID: string): SourceSystem {
  return {
    id,
    tenant_id: 'platform-local',
    name: id,
    allowed_client_ids: [clientID],
    active: true,
    created_at: '2026-08-23T00:00:00Z',
  }
}

function schema(
  schemaID: string,
  classification: string,
  requiredFields: string[] = [],
  allowedFields: string[] = [],
): EventSchema {
  return {
    tenant_id: 'platform-local',
    schema_id: schemaID,
    version: 1,
    event_type: schemaID,
    required_fields: requiredFields,
    allowed_fields: allowedFields,
    encrypted_fields: [],
    searchable_fields: [],
    classification,
    active: true,
    created_at: '2026-08-23T00:00:00Z',
  }
}

const readySources = [
  source('aero-id.tenant-digest', 'aero-id-audit'),
  source('aero-im.source', 'aero-im-audit'),
  source('aero-vault.tenant-digest', 'aero-vault-audit'),
]

const readySchemas = [
  schema('aero.id.audit-fact', 'personal'),
  schema('aero.im.security', 'confidential'),
  schema('aero.vault.security', 'confidential', ['fact_kind'], [
    'detail_sha256',
    'fact_kind',
    'object_size_bytes',
    'request_id',
    'storage_backend',
  ]),
]

describe('platform audit contracts', () => {
  it('recognizes all three platform producers without deriving source secrets', () => {
    const result = assessPlatformAuditContracts(readySources, readySchemas)
    expect(result).toHaveLength(3)
    expect(result.every((status) => status.ready)).toBe(true)
    expect(result.map((status) => status.client_id)).toEqual([
      'aero-id-audit',
      'aero-im-audit',
      'aero-vault-audit',
    ])
  })

  it('reports exact Client allowlist and Schema policy drift', () => {
    const sources = readySources.map((item) => ({ ...item }))
    sources[2] = {
      ...sources[2]!,
      allowed_client_ids: ['aero-vault-audit', 'unexpected-client'],
    }
    const schemas = readySchemas.map((item) => ({ ...item }))
    schemas[2] = { ...schemas[2]!, allowed_fields: ['fact_kind'] }

    const vault = assessPlatformAuditContracts(sources, schemas)[2]!
    expect(vault.ready).toBe(false)
    expect(vault.detail).toContain('Client 绑定必须精确')
    expect(vault.detail).toContain('允许字段漂移')
  })

  it('requires the producer-pinned Schema version instead of accepting a newer one', () => {
    const schemas = readySchemas
      .filter((item) => item.schema_id !== 'aero.im.security')
      .concat({ ...schema('aero.im.security', 'confidential'), version: 2 })
    const im = assessPlatformAuditContracts(readySources, schemas)[1]!
    expect(im.ready).toBe(false)
    expect(im.detail).toContain('aero.im.security v1')
  })
})
