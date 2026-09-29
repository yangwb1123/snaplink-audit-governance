export interface LookupToken {
  generation: number
  key: string
}

export class LatestLookup {
  private generation = 0
  private activeKey = ''

  begin(key: string): LookupToken {
    this.activeKey = key
    this.generation += 1
    return { generation: this.generation, key }
  }

  invalidate(): void {
    this.activeKey = ''
    this.generation += 1
  }

  isCurrent(token: LookupToken, currentKey: string): boolean {
    return (
      token.generation === this.generation &&
      token.key === this.activeKey &&
      token.key === currentKey
    )
  }
}
