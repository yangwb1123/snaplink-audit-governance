import type { EventSchema, SourceSystem } from '../api/types'

interface PlatformAuditContract {
  key: string
  service: string
  clientId: string
  sourceID?: string
  sourcePrefix?: string
  schemaID: string
  schemaVersion: number
  classification: string
  requiredFields: string[]
  allowedFields: string[]
  encryptedFields: string[]
  searchableFields: string[]
}

export interface PlatformAuditStatus extends Record<string, unknown> {
  key: string
  service: string
  client_id: string
  source_id: string
  schema_ref: string
  ready: boolean
  detail: string
}

// Pinned to the six-project platform deployment contract. Aero ID and Vault
// derive tenant-bound source IDs server-side, so the browser matches only the
// public prefix and never receives the derivation key.
const platformAuditContracts: PlatformAuditContract[] = [
  {
    key: 'aero-id',
    service: '账户系统 · Aero ID',
    clientId: 'aero-id-audit',
    sourcePrefix: 'aero-id.',
    schemaID: 'aero.id.audit-fact',
    schemaVersion: 1,
    classification: 'personal',
    requiredFields: [],
    allowedFields: [],
    encryptedFields: [],
    searchableFields: [],
  },
  {
    key: 'aero-im',
    service: '消息通知 · Aero IM',
    clientId: 'aero-im-audit',
    sourceID: 'aero-im.source',
    schemaID: 'aero.im.security',
    schemaVersion: 1,
    classification: 'confidential',
    requiredFields: [],
    allowedFields: [],
    encryptedFields: [],
    searchableFields: [],
  },
  {
    key: 'aero-vault',
    service: '文件服务 · Aero Vault',
    clientId: 'aero-vault-audit',
    sourcePrefix: 'aero-vault.',
    schemaID: 'aero.vault.security',
    schemaVersion: 1,
    classification: 'confidential',
    requiredFields: ['fact_kind'],
    allowedFields: [
      'detail_sha256',
      'fact_kind',
      'object_size_bytes',
      'request_id',
      'storage_backend',
    ],
    encryptedFields: [],
    searchableFields: [],
  },
]

function sameFields(actual: string[] | undefined, expected: string[]): boolean {
  const normalized = [...(actual ?? [])].sort()
  const wanted = [...expected].sort()
  return (
    normalized.length === wanted.length &&
    normalized.every((field, index) => field === wanted[index])
  )
}

function sourceMatches(source: SourceSystem, contract: PlatformAuditContract): boolean {
  if (contract.sourceID) return source.id === contract.sourceID
  return Boolean(contract.sourcePrefix && source.id.startsWith(contract.sourcePrefix))
}

function sourceIssues(
  sources: SourceSystem[],
  contract: PlatformAuditContract,
): { source?: SourceSystem; issues: string[] } {
  const matches = sources.filter((source) => sourceMatches(source, contract))
  if (matches.length === 0) return { issues: ['来源未登记'] }
  if (matches.length > 1) return { issues: ['发现多个匹配来源'] }
  const source = matches[0]!
  const issues: string[] = []
  if (!source.active) issues.push('来源已停用')
  if (!sameFields(source.allowed_client_ids, [contract.clientId])) {
    issues.push(`Client 绑定必须精确为 ${contract.clientId}`)
  }
  return { source, issues }
}

function schemaIssues(schemas: EventSchema[], contract: PlatformAuditContract): string[] {
  const schema = schemas.find(
    (candidate) =>
      candidate.schema_id === contract.schemaID && candidate.version === contract.schemaVersion,
  )
  if (!schema) return [`缺少 ${contract.schemaID} v${contract.schemaVersion}`]
  const issues: string[] = []
  if (!schema.active) issues.push('Schema 已停用')
  if (schema.event_type !== contract.schemaID) issues.push('事件类型不匹配')
  if (schema.classification !== contract.classification) issues.push('数据分级不匹配')
  if (!sameFields(schema.required_fields, contract.requiredFields)) issues.push('必填字段漂移')
  if (!sameFields(schema.allowed_fields, contract.allowedFields)) issues.push('允许字段漂移')
  if (!sameFields(schema.encrypted_fields, contract.encryptedFields)) issues.push('加密字段漂移')
  if (!sameFields(schema.searchable_fields, contract.searchableFields)) {
    issues.push('可搜索字段漂移')
  }
  return issues
}

export function assessPlatformAuditContracts(
  sources: SourceSystem[],
  schemas: EventSchema[],
): PlatformAuditStatus[] {
  return platformAuditContracts.map((contract) => {
    const sourceResult = sourceIssues(sources, contract)
    const issues = [...sourceResult.issues, ...schemaIssues(schemas, contract)]
    const sourcePattern = contract.sourceID ?? `${contract.sourcePrefix ?? ''}*`
    return {
      key: contract.key,
      service: contract.service,
      client_id: contract.clientId,
      source_id: sourceResult.source?.id ?? sourcePattern,
      schema_ref: `${contract.schemaID} v${contract.schemaVersion}`,
      ready: issues.length === 0,
      detail: issues.length ? issues.join('；') : '来源、Client 与 Schema 契约一致',
    }
  })
}
