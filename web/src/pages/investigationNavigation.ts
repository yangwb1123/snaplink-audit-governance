// Investigation cross-links: audit event → operation timeline / aggregate
// history. These helpers only move bounded, server-shaped identifiers through
// the hash; payloads, tokens, filters and search digests never enter a URL.
export type InvestigationTarget =
  | { mode: 'operation'; operationId: string }
  | { mode: 'aggregate'; aggregateType: string; aggregateId: string }

const MAX_ID = 256
const MAX_TYPE = 64
const TYPE = /^[A-Za-z0-9._:-]+$/

function wellFormed(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1)
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false
      index += 1
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return false
    }
  }
  return true
}

function validValue(value: string, max: number): boolean {
  if (!value || value.length > max || !wellFormed(value)) return false
  if (/[\s/\\?#&=]/.test(value)) return false
  return ![...value].some((char) => char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127)
}

function safeSegment(raw: string, max: number): string {
  try {
    const value = decodeURIComponent(raw)
    return validValue(value, max) ? value : ''
  } catch {
    return ''
  }
}

function encodeSegment(value: string, max: number): string {
  if (!validValue(value, max)) return ''
  try {
    return encodeURIComponent(value)
  } catch {
    return ''
  }
}

function validAggregateType(value: string): boolean {
  return value.length > 0 && value.length <= MAX_TYPE && TYPE.test(value)
}

/** Parse `#operations/op/<id>` or `#operations/aggregate/<type>/<id>`. */
export function investigationLocation(hash: string): InvestigationTarget | null {
  const parts = hash.replace(/^#\/?/, '').split('/')
  if (parts[0] !== 'operations') return null
  if (parts[1] === 'op' && parts.length === 3) {
    const operationId = safeSegment(parts[2], MAX_ID)
    return operationId ? { mode: 'operation', operationId } : null
  }
  if (parts[1] === 'aggregate' && parts.length === 4) {
    const aggregateType = safeSegment(parts[2], MAX_TYPE)
    const aggregateId = safeSegment(parts[3], MAX_ID)
    if (!validAggregateType(aggregateType) || !aggregateId) return null
    return { mode: 'aggregate', aggregateType, aggregateId }
  }
  return null
}

/** Build a shareable investigation link; invalid identifiers fall back to the
 * plain operations route instead of embedding untrusted input. */
export function investigationHash(target: InvestigationTarget): string {
  if (target.mode === 'operation') {
    const id = encodeSegment(target.operationId, MAX_ID)
    return id ? `#operations/op/${id}` : '#operations'
  }
  const type = encodeSegment(target.aggregateType, MAX_TYPE)
  const id = encodeSegment(target.aggregateId, MAX_ID)
  if (!type || !validAggregateType(target.aggregateType) || !id) return '#operations'
  return `#operations/aggregate/${type}/${id}`
}

export function navigateInvestigation(target: InvestigationTarget): void {
  window.location.hash = investigationHash(target)
}
