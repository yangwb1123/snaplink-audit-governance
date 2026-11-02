interface ApiErrorBody {
  error?: {
    code?: string
    message?: string
    request_id?: string
    reason?: string
    hold_id?: string
  }
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly requestId?: string,
    readonly reason?: string,
    readonly holdId?: string,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

export interface ApiClientOptions {
  baseUrl: string
  accessToken: string
  // Platform sessions must supply a selector for scoped operations. The API
  // layer keeps tenant-token compatibility, while preventing the UI from
  // emitting an unscoped request that the server will reject.
  platform?: boolean
  fetcher?: typeof fetch
  onUnauthorized?: () => void
  // A scoped adapter can supply a default signal for read methods whose public
  // signatures stay unchanged. RequestInit.signal still overrides it, including
  // an explicit null that intentionally disables the default.
  requestSignal?: AbortSignal
}

function joinUrl(baseUrl: string, path: string): string {
  return `${baseUrl.replace(/\/$/, '')}/${path.replace(/^\//, '')}`
}

export class ApiClient {
  private readonly fetcher: typeof fetch

  constructor(private readonly options: ApiClientOptions) {
    // Calling a native fetch as this.fetcher(...) would bind `this` to the
    // client, which Chromium rejects as an illegal Web API invocation.
    this.fetcher = (options.fetcher ?? fetch).bind(globalThis)
  }

  private effectiveSignal(init: RequestInit): AbortSignal | null | undefined {
    if (Object.prototype.hasOwnProperty.call(init, 'signal') && init.signal !== undefined) {
      return init.signal
    }
    return this.options.requestSignal
  }

  async request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers)
    headers.set('Accept', 'application/json')
    headers.set('Authorization', `Bearer ${this.options.accessToken}`)
    if (init.body !== undefined && !headers.has('Content-Type')) {
      headers.set('Content-Type', 'application/json')
    }
    const signal = this.effectiveSignal(init)
    const response = await this.fetcher(joinUrl(this.options.baseUrl, path), {
      ...init,
      ...(signal === undefined ? {} : { signal }),
      credentials: 'omit',
      headers,
    })
    if (!response.ok) {
      // Request-bound adapters retire only the effective request signal, not
      // an overridden default. Legacy adapters retain their original 401
      // notification contract; every request still rejects its own ApiError.
      if (response.status === 401 &&
          (this.options.requestSignal === undefined || signal?.aborted !== true)) this.options.onUnauthorized?.()
      throw await this.toError(response)
    }
    if (response.status === 204) return undefined as T
    return (await response.json()) as T
  }

  async blob(path: string, signal?: AbortSignal | null): Promise<Blob> {
    const effectiveSignal = signal === null ? null : signal ?? this.options.requestSignal
    const response = await this.fetcher(joinUrl(this.options.baseUrl, path), {
      credentials: 'omit',
      signal: effectiveSignal,
      headers: { Authorization: `Bearer ${this.options.accessToken}` },
    })
    if (!response.ok) {
      if (response.status === 401 &&
          (this.options.requestSignal === undefined || effectiveSignal?.aborted !== true)) this.options.onUnauthorized?.()
      throw await this.toError(response)
    }
    return response.blob()
  }

  private async toError(response: Response): Promise<ApiError> {
    let body: ApiErrorBody = {}
    try {
      body = (await response.json()) as ApiErrorBody
    } catch {
      // Non-JSON proxy errors deliberately collapse to the HTTP status.
    }
    return new ApiError(
      response.status,
      body.error?.code ?? `http_${response.status}`,
      body.error?.message ?? `Audit API request failed (${response.status})`,
      body.error?.request_id,
      body.error?.reason,
      body.error?.hold_id,
    )
  }
}
