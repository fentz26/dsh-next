export interface JournalRead {
  data: Uint8Array
  nextOffset: number
  lossy: boolean
}

export interface JournaledLike {
  readonly nextOffset: number
  readonly windowStart: number
  append(b: Uint8Array): number
  readFrom(offset: number): JournalRead
}

export function makeJournaled(): null {
  return null
}
