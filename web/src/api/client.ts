interface ApiErrorBody {
  error?: {
    code?: string
    message?: string
    request_id?: string
  }
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly requestId?: string,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

export interface ApiClientOptions {
  baseUrl: string
  accessToken: string
  fetcher?: typeof fetch
  onUnauthorized?: () => void
}

function joinUrl(baseUrl: string, path: string): string {
  return `${baseUrl.replace(/\/$/, '')}/${path.replace(/^\//, '')}`
}

export class ApiClient {
  private readonly fetcher: typeof fetch

  constructor(private readonly options: ApiClientOptions) {
    this.fetcher = options.fetcher ?? fetch
  }

  async request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers)
    headers.set('Accept', 'application/json')
    headers.set('Authorization', `Bearer ${this.options.accessToken}`)
    if (init.body !== undefined && !headers.has('Content-Type')) {
      headers.set('Content-Type', 'application/json')
    }
    const response = await this.fetcher(joinUrl(this.options.baseUrl, path), {
      ...init,
      credentials: 'omit',
      headers,
    })
    if (!response.ok) {
      if (response.status === 401) this.options.onUnauthorized?.()
      throw await this.toError(response)
    }
    if (response.status === 204) return undefined as T
    return (await response.json()) as T
  }

  async blob(path: string): Promise<Blob> {
    const response = await this.fetcher(joinUrl(this.options.baseUrl, path), {
      credentials: 'omit',
      headers: { Authorization: `Bearer ${this.options.accessToken}` },
    })
    if (!response.ok) {
      if (response.status === 401) this.options.onUnauthorized?.()
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
    )
  }
}
