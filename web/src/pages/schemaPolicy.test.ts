import { describe, expect, it } from 'vitest'
import { parseSchemaFieldList, validateSchemaFieldPolicy } from './schemaPolicy'

describe('schema field policy', () => {
  it('normalizes comma-separated fields without changing their order', () => {
    expect(parseSchemaFieldList(' resource, email,resource, amount ,, email ')).toEqual([
      'resource',
      'email',
      'amount',
    ])
  })

  it('accepts envelope requirements and searchable plaintext fields', () => {
    expect(
      validateSchemaFieldPolicy({
        requiredFields: ['event_id', 'actor.id', 'resource'],
        allowedFields: ['resource', 'email'],
        encryptedFields: [],
        searchableFields: ['email'],
      }),
    ).toBeNull()
  })

  it('rejects payload policy fields outside an explicit allowlist', () => {
    expect(
      validateSchemaFieldPolicy({
        requiredFields: ['resource'],
        allowedFields: ['resource'],
        encryptedFields: ['email'],
        searchableFields: [],
      }),
    ).toContain('email')
  })

  it('rejects the server-reserved search digest namespace', () => {
    expect(
      validateSchemaFieldPolicy({
        requiredFields: [],
        allowedFields: ['email__search_digest'],
        encryptedFields: [],
        searchableFields: [],
      }),
    ).toContain('__search_digest')
  })
})
