export interface RequestTicket {
  signal: AbortSignal
  current: () => boolean
  finish: () => void
}

/** In-memory request epochs. Cancellation alone is insufficient: a fetcher
 * may ignore AbortSignal, and A -> B -> A still needs to reject the first A. */
export class RequestScope {
  private generation = 0
  private disposed = false
  private readonly controllers = new Set<AbortController>()

  capture(): RequestTicket {
    const generation = this.generation
    const controller = new AbortController()
    if (this.disposed) controller.abort()
    else this.controllers.add(controller)
    return {
      signal: controller.signal,
      current: () => !this.disposed && generation === this.generation && !controller.signal.aborted,
      finish: () => this.controllers.delete(controller),
    }
  }

  next(): RequestTicket {
    this.invalidate()
    return this.capture()
  }

  invalidate(): void {
    this.generation += 1
    for (const controller of this.controllers) controller.abort()
    this.controllers.clear()
  }

  dispose(): void {
    this.disposed = true
    this.invalidate()
  }
}
