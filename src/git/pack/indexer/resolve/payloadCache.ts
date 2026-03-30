import type { GitObjectType } from "@/git/core/objects.ts";

const LIST_END = -1;

export interface CacheEntry {
  type: GitObjectType;
  payload: Uint8Array;
}

function encodeType(type: GitObjectType): number {
  switch (type) {
    case "commit":
      return 1;
    case "tree":
      return 2;
    case "blob":
      return 3;
    case "tag":
      return 4;
  }
  throw new Error(`payload-cache: unknown object type ${type}`);
}

function decodeType(typeCode: number): GitObjectType {
  switch (typeCode) {
    case 1:
      return "commit";
    case 2:
      return "tree";
    case 3:
      return "blob";
    case 4:
      return "tag";
    default:
      throw new Error(`payload-cache: unknown object type code ${typeCode}`);
  }
}

export class PayloadLRU {
  private payloads: Array<Uint8Array | undefined>;
  private typeCodes: Uint8Array;
  private present: Uint8Array;
  private prev: Int32Array;
  private next: Int32Array;
  private totalBytes = 0;
  private budget: number;
  private cachedEntries = 0;
  private head = LIST_END;
  private tail = LIST_END;

  /**
   * Lifecycle-aware eviction: each base has a "last needed at" offset — the
   * maximum pack offset of any delta that depends on it. Once the current
   * processing offset passes this value, the base is safe to evict.
   *
   * The cache is indexed by pack entry number instead of a Map so V8 does not
   * pay per-entry hash/object overhead on large delta-heavy packs.
   */
  private deadlinesArr: Uint32Array | null = null;
  private currentOffset = 0;

  constructor(budget: number, capacity: number) {
    this.budget = budget;
    this.payloads = new Array<Uint8Array | undefined>(capacity);
    this.typeCodes = new Uint8Array(capacity);
    this.present = new Uint8Array(capacity);
    this.prev = new Int32Array(capacity).fill(LIST_END);
    this.next = new Int32Array(capacity).fill(LIST_END);
  }

  /** Set per-entry eviction deadlines before the resolve pass begins. */
  setDeadlines(deadlines: Uint32Array): void {
    this.deadlinesArr = deadlines;
  }

  /** Update the current processing offset so eviction can prioritize expired entries. */
  setCurrentOffset(offset: number): void {
    this.currentOffset = offset;
  }

  get(index: number): CacheEntry | undefined {
    if (!this.present[index]) return undefined;
    const payload = this.payloads[index];
    if (!payload) return undefined;
    this.touch(index);
    return {
      type: decodeType(this.typeCodes[index]),
      payload,
    };
  }

  set(index: number, entry: CacheEntry): void {
    if (this.present[index]) {
      this.remove(index);
    }
    if (entry.payload.length > this.budget) {
      return;
    }

    this.payloads[index] = entry.payload;
    this.typeCodes[index] = encodeType(entry.type);
    this.present[index] = 1;
    this.cachedEntries++;
    this.totalBytes += entry.payload.length;
    this.appendTail(index);
    this.evict();
  }

  private touch(index: number): void {
    if (this.tail === index) return;
    this.detach(index);
    this.appendTail(index);
  }

  private appendTail(index: number): void {
    this.prev[index] = this.tail;
    this.next[index] = LIST_END;
    if (this.tail !== LIST_END) {
      this.next[this.tail] = index;
    } else {
      this.head = index;
    }
    this.tail = index;
  }

  private detach(index: number): void {
    const prevIndex = this.prev[index];
    const nextIndex = this.next[index];
    if (prevIndex !== LIST_END) {
      this.next[prevIndex] = nextIndex;
    } else {
      this.head = nextIndex;
    }
    if (nextIndex !== LIST_END) {
      this.prev[nextIndex] = prevIndex;
    } else {
      this.tail = prevIndex;
    }
    this.prev[index] = LIST_END;
    this.next[index] = LIST_END;
  }

  private remove(index: number): void {
    if (!this.present[index]) return;
    const payload = this.payloads[index];
    this.detach(index);
    this.present[index] = 0;
    this.payloads[index] = undefined;
    this.typeCodes[index] = 0;
    this.cachedEntries--;
    if (payload) {
      this.totalBytes -= payload.length;
    }
  }

  private findEvictionCandidate(): number {
    // Walk the LRU chain from oldest to newest so we still prefer entries that
    // are both cold and past their dependency deadline. If none are expired yet,
    // evicting the oldest entry preserves the original LRU fallback behavior.
    let candidate = this.head;
    while (candidate !== LIST_END) {
      const deadline = this.deadlinesArr ? this.deadlinesArr[candidate] : 0;
      if (deadline <= this.currentOffset) {
        return candidate;
      }
      candidate = this.next[candidate];
    }
    return this.head;
  }

  private evict(): void {
    while (this.totalBytes > this.budget && this.cachedEntries > 1) {
      const candidate = this.findEvictionCandidate();
      if (candidate === LIST_END) break;
      this.remove(candidate);
    }
  }
}
