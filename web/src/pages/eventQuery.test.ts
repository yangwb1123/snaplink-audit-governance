import { describe, expect, it } from 'vitest'
import { buildEventQuery, emptyEventFilters } from './eventQuery'

describe('event query builder', () => {
  it('maps and trims every cross-service correlation filter', () => {
    const result = buildEventQuery(
      { start: new Date('2026-08-21T00:00:00Z'), end: new Date('2026-08-22T00:00:00Z') },
      {
        ...emptyEventFilters,
        actorId: ' user-1 ',
        operationId: ' op-1 ',
        causationId: ' cause-1 ',
        correlationId: ' corr-1 ',
        traceId: ' trace-1 ',
        aggregateType: ' file ',
        aggregateId: ' f-1 ',
        streamId: ' stream-1 ',
        payloadField: ' object_id ',
        payloadDigest: ' sha256-value ',
      },
    )

    expect(result).toMatchObject({
      actor_id: 'user-1',
      operation_id: 'op-1',
      causation_id: 'cause-1',
      correlation_id: 'corr-1',
      trace_id: 'trace-1',
      aggregate_type: 'file',
      aggregate_id: 'f-1',
      stream_id: 'stream-1',
      payload_field: 'object_id',
      payload_digest: 'sha256-value',
      page_size: 100,
    })
  })

  it('rejects an incomplete or reversed time range', () => {
    expect(buildEventQuery({ start: null, end: new Date() }, emptyEventFilters)).toBeNull()
    const same = new Date('2026-08-22T00:00:00Z')
    expect(buildEventQuery({ start: same, end: same }, emptyEventFilters)).toBeNull()
    expect(
      buildEventQuery(
        { start: new Date('2026-08-22T00:00:00Z'), end: new Date('2026-08-21T00:00:00Z') },
        emptyEventFilters,
      ),
    ).toBeNull()
  })
})
