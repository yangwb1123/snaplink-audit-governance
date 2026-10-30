// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  EXPORT_DIGEST_MAX_BYTES,
  checkLocalExportDigest,
  exportDigestMessages,
  isExpectedExportDigest,
} from './exportDigest'

// Published SHA-256 vectors. File bytes are never normalized, re-encoded or
// newline/BOM-rewritten before hashing.
const EMPTY = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
const ABC = 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'
const ABC_LF = 'edeaaff3f1774ad2888673770c6d64097e391bc362d7d6fb34982ddf0efd18cb'
const ABC_CRLF = '552bab6864c7a7b69a502ed1854b9245c0e1a30f008aaa0b281da62585fdb025'
const BOM_ABC = '1c28dc3f1f804a1ad9c9b4b4cf5e2658d16ad4ed08e3020d04a8d2865018947c'
const AUDIT = 'e73d84773a078bbb55902d0a216c5c633057e94fb9ba16a6aa94ded8891852f4'
const AUDIT_LF = '3e5f919baf986852d96a7146b6767da1cb8646d46aa02ed60b13e5d7ba84be9e'

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (cause: unknown) => void
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail })
  return { promise, resolve, reject }
}

function trackedFile(size: number, read: () => Promise<ArrayBuffer>) {
  const slice = vi.fn(() => ({ arrayBuffer: read }))
  return { file: { size, slice } as unknown as Blob, slice }
}

function digestBytes(hex: string): ArrayBuffer {
  const bytes = new Uint8Array(32)
  for (let index = 0; index < 32; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16)
  }
  return bytes.buffer
}

function textBytes(value: string): ArrayBuffer {
  return new TextEncoder().encode(value).buffer as ArrayBuffer
}

function withBom(value: string): File {
  const text = new TextEncoder().encode(value)
  const bytes = new Uint8Array(3 + text.byteLength)
  bytes.set([0xef, 0xbb, 0xbf], 0)
  bytes.set(text, 3)
  return new File([bytes], 'local-export.jsonl')
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe('local export digest helper', () => {
  it('matches real WebCrypto SHA-256 over raw bytes, including empty input and a case-insensitive hex expectation', async () => {
    const empty = await checkLocalExportDigest(new File([], 'empty.jsonl'), EMPTY)
    expect(empty).toEqual({ status: 'match', actualDigest: EMPTY, bytes: 0 })
    expect(empty).not.toHaveProperty('file')

    const abc = await checkLocalExportDigest(new File(['abc'], 'private-name.jsonl'), ABC.toUpperCase())
    expect(abc).toEqual({ status: 'match', actualDigest: ABC, bytes: 3 })
    expect(JSON.stringify(abc)).not.toContain('private-name')
  })

  it('hashes multibyte text and does not rewrite newline or BOM bytes', async () => {
    const audit = await checkLocalExportDigest(new File(['审计'], 'audit.jsonl'), AUDIT)
    expect(audit).toEqual({ status: 'match', actualDigest: AUDIT, bytes: 6 })

    const auditLf = await checkLocalExportDigest(new File(['审计\n'], 'audit.jsonl'), AUDIT_LF)
    expect(auditLf).toEqual({ status: 'match', actualDigest: AUDIT_LF, bytes: 7 })
    expect((await checkLocalExportDigest(new File(['审计\n'], 'audit.jsonl'), AUDIT)).status).toBe('mismatch')

    const bom = await checkLocalExportDigest(withBom('abc'), BOM_ABC)
    expect(bom).toEqual({ status: 'match', actualDigest: BOM_ABC, bytes: 6 })
    expect((await checkLocalExportDigest(withBom('abc'), ABC)).status).toBe('mismatch')
  })

  it('distinguishes newline variants by their exact bytes', async () => {
    expect(await checkLocalExportDigest(new File(['abc\n'], 'x.jsonl'), ABC_LF)).toEqual({
      status: 'match', actualDigest: ABC_LF, bytes: 4,
    })
    expect(await checkLocalExportDigest(new File(['abc\r\n'], 'x.jsonl'), ABC_CRLF)).toEqual({
      status: 'match', actualDigest: ABC_CRLF, bytes: 5,
    })
    expect((await checkLocalExportDigest(new File(['abc\r\n'], 'x.jsonl'), ABC_LF)).status).toBe('mismatch')
    expect((await checkLocalExportDigest(new File(['abc'], 'x.jsonl'), ABC_LF)).status).toBe('mismatch')
  })

  it.each([
    ['missing', undefined, 'missing_digest'],
    ['empty', '', 'missing_digest'],
    ['null', null, 'missing_digest'],
    ['short', ABC.slice(0, 63), 'invalid_digest'],
    ['long', `${ABC}0`, 'invalid_digest'],
    ['non-hex', `${ABC.slice(0, 63)}g`, 'invalid_digest'],
    ['padded', ` ${ABC}`, 'invalid_digest'],
    ['newline', `${ABC}\n`, 'invalid_digest'],
  ] as const)('rejects a %s expected digest before any file read', async (_case, expected, reason) => {
    const tracked = trackedFile(3, async () => textBytes('abc'))
    const outcome = await checkLocalExportDigest(tracked.file, expected)
    expect(outcome).toEqual({ status: 'unavailable', reason })
    expect(tracked.slice).not.toHaveBeenCalled()
    expect(exportDigestMessages[reason]).toContain('未读取本地文件')
  })

  it('validates hex syntax without trimming or interpreting an empty value as a passing digest', () => {
    expect(isExpectedExportDigest(ABC)).toBe(true)
    expect(isExpectedExportDigest(ABC.toUpperCase())).toBe(true)
    expect(isExpectedExportDigest(` ${ABC}`)).toBe(false)
    expect(isExpectedExportDigest(`${ABC}\n`)).toBe(false)
    expect(isExpectedExportDigest('')).toBe(false)
    expect(isExpectedExportDigest(undefined)).toBe(false)
  })

  it('accepts exactly 32 MiB but rejects an over-limit declaration before reading', async () => {
    const exact = trackedFile(EXPORT_DIGEST_MAX_BYTES, async () => ({ byteLength: EXPORT_DIGEST_MAX_BYTES } as ArrayBuffer))
    const outcome = await checkLocalExportDigest(exact.file, ABC, { hash: async () => digestBytes(ABC) })
    expect(outcome).toEqual({ status: 'match', actualDigest: ABC, bytes: EXPORT_DIGEST_MAX_BYTES })
    expect(exact.slice).toHaveBeenCalledWith(0, EXPORT_DIGEST_MAX_BYTES + 1)

    const tooLarge = trackedFile(EXPORT_DIGEST_MAX_BYTES + 1, async () => new ArrayBuffer(0))
    expect(await checkLocalExportDigest(tooLarge.file, ABC)).toEqual({
      status: 'unavailable', reason: 'too_large',
    })
    expect(tooLarge.slice).not.toHaveBeenCalled()
  })

  it('hashes an actual 32 MiB file with real WebCrypto at the exact boundary', async () => {
    // Independent SHA-256 reference: 32 * 1 MiB zero blocks via Python hashlib.
    // Keep the injected branch oracle above; this checks actual Blob/read/hash.
    const expected = '83ee47245398adee79bd9c0a8bc57b821e92aba10f5f9ade8a5d1fae4d8c4302'
    const file = new File([new Uint8Array(EXPORT_DIGEST_MAX_BYTES)], 'boundary-local.jsonl')
    expect(file.size).toBe(EXPORT_DIGEST_MAX_BYTES)
    expect(await checkLocalExportDigest(file, expected)).toEqual({
      status: 'match', actualDigest: expected, bytes: EXPORT_DIGEST_MAX_BYTES,
    })
  })

  it('rejects an actual over-limit read and declared/actual size disagreement', async () => {
    const actualTooLarge = trackedFile(EXPORT_DIGEST_MAX_BYTES, async () =>
      ({ byteLength: EXPORT_DIGEST_MAX_BYTES + 1 } as ArrayBuffer))
    expect(await checkLocalExportDigest(actualTooLarge.file, ABC)).toEqual({
      status: 'unavailable', reason: 'too_large',
    })

    const short = trackedFile(4, async () => new ArrayBuffer(3))
    expect(await checkLocalExportDigest(short.file, ABC)).toEqual({
      status: 'unavailable', reason: 'size_mismatch',
    })

    const long = trackedFile(3, async () => new ArrayBuffer(4))
    expect(await checkLocalExportDigest(long.file, ABC)).toEqual({
      status: 'unavailable', reason: 'size_mismatch',
    })
  })

  it('fails closed with a fixed message when WebCrypto is unavailable and does not read the file', async () => {
    vi.stubGlobal('crypto', {})
    const tracked = trackedFile(3, async () => textBytes('abc'))
    expect(await checkLocalExportDigest(tracked.file, ABC)).toEqual({
      status: 'unavailable', reason: 'crypto_unavailable',
    })
    expect(tracked.slice).not.toHaveBeenCalled()
    expect(exportDigestMessages.crypto_unavailable).not.toContain('Error')
  })

  it('maps read and hash failures to fixed messages without exposing the original error', async () => {
    const readFailure = trackedFile(3, async () => { throw new Error('private filename and bytes') })
    const readOutcome = await checkLocalExportDigest(readFailure.file, ABC)
    expect(readOutcome).toEqual({ status: 'unavailable', reason: 'read_failed' })
    expect(JSON.stringify(readOutcome)).not.toContain('private')
    expect(exportDigestMessages.read_failed).not.toContain('private')

    const hashFailure = trackedFile(3, async () => textBytes('abc'))
    const hashOutcome = await checkLocalExportDigest(hashFailure.file, ABC, {
      hash: async () => { throw new Error('private content') },
    })
    expect(hashOutcome).toEqual({ status: 'unavailable', reason: 'hash_failed' })
    expect(JSON.stringify(hashOutcome)).not.toContain('private')
    expect(exportDigestMessages.hash_failed).not.toContain('private')
  })

  it('treats an invalid hash result as a fixed hash failure', async () => {
    const tracked = trackedFile(3, async () => textBytes('abc'))
    expect(await checkLocalExportDigest(tracked.file, ABC, {
      hash: async () => new ArrayBuffer(31),
    })).toEqual({ status: 'unavailable', reason: 'hash_failed' })
  })

  it('returns a deterministic mismatch without exposing local file details', async () => {
    const outcome = await checkLocalExportDigest(new File(['abc\n'], 'private-name.jsonl'), ABC)
    expect(outcome).toEqual({ status: 'mismatch', actualDigest: ABC_LF, bytes: 4 })
    expect(JSON.stringify(outcome)).not.toContain('private-name')
    expect(exportDigestMessages).not.toHaveProperty('match')
  })

  it('does not fall back to a non-WebCrypto implementation when digest rejects', async () => {
    const tracked = trackedFile(3, async () => textBytes('abc'))
    const hash = vi.fn(async () => { throw new Error('subtle detail') })
    const outcome = await checkLocalExportDigest(tracked.file, ABC, { hash })
    expect(hash).toHaveBeenCalledTimes(1)
    expect(outcome).toEqual({ status: 'unavailable', reason: 'hash_failed' })
  })

  it('keeps a rejected injected hash unobservable through thrown values', async () => {
    const pending = deferred<ArrayBuffer>()
    const tracked = trackedFile(3, async () => textBytes('abc'))
    const check = checkLocalExportDigest(tracked.file, ABC, { hash: async () => pending.promise })
    pending.reject(new Error('private hash failure'))
    await expect(check).resolves.toEqual({ status: 'unavailable', reason: 'hash_failed' })
  })
})
