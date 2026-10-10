import { describe, expect, it } from 'vitest'
import { ApiError } from './client'
import { errorPresentation } from './errorPresentation'

describe('structured feedback', () => {
  it('does not infer Legal Hold from free-form text', () => {
    const blocked = new ApiError(403, 'forbidden', 'arbitrary diagnostic', 'req', 'legal_hold', 'hold')
    expect(errorPresentation(blocked).title).toBe('导出被法律保留阻断')
    expect(errorPresentation(blocked).retryable).toBe(false)
    expect(errorPresentation(new ApiError(403, 'forbidden', 'legal hold')).title).toBe('当前操作不被允许')
  })

  it('distinguishes login, authorization, budget and transient failure without automatic mutation retries', () => {
    expect(errorPresentation(new ApiError(401, 'unauthorized', '')).retryable).toBe(false)
    expect(errorPresentation(new ApiError(403, 'forbidden', '')).retryable).toBe(false)
    expect(errorPresentation(new ApiError(429, 'quota', '')).title).toContain('配额')
    expect(errorPresentation(new ApiError(503, 'store_timeout', '')).guidance).toContain('变更可能已提交')
    expect(errorPresentation(new ApiError(409, 'conflict', '')).guidance).toContain('最新状态')
  })
})
