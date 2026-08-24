export interface SchemaFieldPolicy {
  requiredFields: string[]
  allowedFields: string[]
  encryptedFields: string[]
  searchableFields: string[]
}

const envelopeRequiredFields = new Set([
  'event_id',
  'source_system',
  'event_type',
  'operation_id',
  'aggregate_id',
  'actor.id',
])

const searchDigestSuffix = '__search_digest'

export function parseSchemaFieldList(value: string): string[] {
  return [
    ...new Set(
      value
        .split(',')
        .map((field) => field.trim())
        .filter(Boolean),
    ),
  ]
}

export function validateSchemaFieldPolicy(policy: SchemaFieldPolicy): string | null {
  const configuredFields = [
    ...policy.requiredFields,
    ...policy.allowedFields,
    ...policy.encryptedFields,
    ...policy.searchableFields,
  ]
  const reservedField = configuredFields.find((field) => field.endsWith(searchDigestSuffix))
  if (reservedField) {
    return `${reservedField} 使用了服务端保留后缀 ${searchDigestSuffix}`
  }

  if (policy.allowedFields.length === 0) return null
  const allowed = new Set(policy.allowedFields)
  const missingRequired = policy.requiredFields.find(
    (field) => !envelopeRequiredFields.has(field) && !allowed.has(field),
  )
  if (missingRequired) return `必填 payload 字段 ${missingRequired} 不在允许字段中`

  const missingEncrypted = policy.encryptedFields.find((field) => !allowed.has(field))
  if (missingEncrypted) return `加密字段 ${missingEncrypted} 不在允许字段中`

  const missingSearchable = policy.searchableFields.find((field) => !allowed.has(field))
  if (missingSearchable) return `可搜索字段 ${missingSearchable} 不在允许字段中`
  return null
}
