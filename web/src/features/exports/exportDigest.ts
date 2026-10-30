/**
 * Local-only export file digest checking.
 *
 * This module never uploads, parses or rewrites the selected file. It compares
 * the SHA-256 of the exact bytes exposed by Blob#slice with the digest already
 * observed on the current export task. The fixed ceiling belongs only to this
 * optional browser operation; it does not change server download/export limits.
 */
export const EXPORT_DIGEST_MAX_BYTES = 32 * 1024 * 1024

export const EXPORT_DIGEST_LIMIT_LABEL = '32 MiB'

const expectedShape = /^[0-9a-fA-F]{64}$/

export type ExportDigestFailureReason =
  | 'missing_digest'
  | 'invalid_digest'
  | 'crypto_unavailable'
  | 'too_large'
  | 'size_mismatch'
  | 'read_failed'
  | 'hash_failed'

export type ExportDigestOutcome =
  | { status: 'match' | 'mismatch'; actualDigest: string; bytes: number }
  | { status: 'unavailable'; reason: ExportDigestFailureReason }

export type ExportDigestHash = (bytes: ArrayBuffer) => Promise<ArrayBuffer>

export const exportDigestMessages: Readonly<Record<ExportDigestFailureReason, string>> = {
  missing_digest: '当前任务没有可核对的 SHA-256 摘要，未读取本地文件。',
  invalid_digest: '当前任务摘要不是有效的 64 位 SHA-256 十六进制值，未读取本地文件。',
  crypto_unavailable: '当前浏览器不支持 WebCrypto SHA-256，无法核对，未读取本地文件。',
  too_large: `文件超过本地核对上限 ${EXPORT_DIGEST_LIMIT_LABEL}，无法核对。`,
  size_mismatch: '文件声明大小与实际读取大小不一致，无法核对。',
  read_failed: '本地文件读取失败，无法核对。',
  hash_failed: '本地 SHA-256 计算失败，无法核对。',
}

/** A required task digest is exactly 64 hex digits with no trimming or padding.
 * Upper and lower case denote the same digest bytes for comparison. */
export function isExpectedExportDigest(value: unknown): value is string {
  return typeof value === 'string' && expectedShape.test(value)
}

function unavailable(reason: ExportDigestFailureReason): ExportDigestOutcome {
  return { status: 'unavailable', reason }
}

function hasWebCrypto(): boolean {
  return Boolean(globalThis.crypto?.subtle && typeof globalThis.crypto.subtle.digest === 'function')
}

function webCryptoSha256(bytes: ArrayBuffer): Promise<ArrayBuffer> {
  const subtle = globalThis.crypto?.subtle
  if (!subtle || typeof subtle.digest !== 'function') {
    return Promise.reject(new Error('WebCrypto unavailable'))
  }
  return subtle.digest('SHA-256', bytes)
}

function toHex(bytes: ArrayBuffer): string {
  const view = new Uint8Array(bytes)
  let result = ''
  for (const byte of view) result += byte.toString(16).padStart(2, '0')
  return result
}

/** Validate the observed task digest and declared size before touching the
 * selected bytes. The read is capped at one byte over the limit so even a
 * Blob whose declared size lies cannot force unbounded memory use. */
export async function checkLocalExportDigest(
  file: Blob,
  expectedDigest: unknown,
  options: { hash?: ExportDigestHash } = {},
): Promise<ExportDigestOutcome> {
  if (expectedDigest === undefined || expectedDigest === null || expectedDigest === '') {
    return unavailable('missing_digest')
  }
  if (!isExpectedExportDigest(expectedDigest)) return unavailable('invalid_digest')

  const hash = options.hash ?? webCryptoSha256
  if (!options.hash && !hasWebCrypto()) return unavailable('crypto_unavailable')

  const declared = file.size
  if (!Number.isSafeInteger(declared) || declared < 0) return unavailable('size_mismatch')
  if (declared > EXPORT_DIGEST_MAX_BYTES) return unavailable('too_large')

  let bytes: ArrayBuffer
  try {
    bytes = await file.slice(0, EXPORT_DIGEST_MAX_BYTES + 1).arrayBuffer()
  } catch {
    return unavailable('read_failed')
  }

  if (bytes.byteLength > EXPORT_DIGEST_MAX_BYTES) return unavailable('too_large')
  if (bytes.byteLength !== declared) return unavailable('size_mismatch')

  let digest: ArrayBuffer
  try {
    digest = await hash(bytes)
  } catch {
    return unavailable('hash_failed')
  }
  if (!(digest instanceof ArrayBuffer) || digest.byteLength !== 32) {
    return unavailable('hash_failed')
  }

  const actualDigest = toHex(digest)
  return {
    status: actualDigest === expectedDigest.toLowerCase() ? 'match' : 'mismatch',
    actualDigest,
    bytes: bytes.byteLength,
  }
}
