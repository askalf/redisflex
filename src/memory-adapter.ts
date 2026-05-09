/**
 * MemoryRedisAdapter — full IRedisAdapter implementation backed by
 * native JS data structures. Drop-in replacement for ioredis when you
 * don't want a Redis server in dev / standalone / CI.
 *
 * Backing structures:
 *   - Key/Value: Map with lazy TTL expiry + a 10s sweep interval
 *   - Hashes: Map<string, Map<string, string>>
 *   - Lists: Map<string, string[]>
 *   - Sorted sets: Map<string, Array<{score, member}>> kept in score order
 *   - Pub/Sub: EventEmitter with glob pattern matching
 *
 * Lua eval is the only inherently lossy operation — see eval() for the
 * sliding-window-rate-limit shape that's recognized; everything else
 * falls through to a no-op + warn (toggle warn off via opts.silent).
 */

import { EventEmitter } from 'node:events';
import type {
  IRedisAdapter,
  RedisGenericHandler,
} from './interface.js';

export interface MemoryRedisAdapterOptions {
  /** When true, suppress the console warning for unrecognized eval
   *  scripts. Default false (warn on first unknown script per process,
   *  for visibility during development). */
  silentEvalFallback?: boolean;
  /** TTL sweep interval in ms. Default 10_000. Lower values = more
   *  promptly-removed expired keys at the cost of CPU. */
  sweepIntervalMs?: number;
}

interface StoredValue {
  value: string;
  /** Wall-clock ms when this key expires. Undefined = no TTL. */
  expiresAt?: number;
}

interface SortedSetEntry {
  score: number;
  member: string;
}

function globToRegex(pattern: string): RegExp {
  const escaped = pattern
    .replace(/[.+^${}()|[\]\\]/g, '\\$&')
    .replace(/\*/g, '.*')
    .replace(/\?/g, '.');
  return new RegExp(`^${escaped}$`);
}

export class MemoryRedisAdapter implements IRedisAdapter {
  readonly mode = 'memory' as const;

  private store = new Map<string, StoredValue>();
  private hashes = new Map<string, Map<string, string>>();
  private lists = new Map<string, string[]>();
  private sortedSets = new Map<string, SortedSetEntry[]>();
  private emitter = new EventEmitter();
  private subscriptions = new Set<string>();
  private patternSubscriptions = new Set<string>();
  private sweepInterval: ReturnType<typeof setInterval> | null;
  private readonly silentEvalFallback: boolean;
  private warnedAboutEvalFallback = false;

  constructor(opts?: MemoryRedisAdapterOptions) {
    this.silentEvalFallback = opts?.silentEvalFallback ?? false;
    // Pub/sub fan-out + the BullMQ-shaped queue can attach a handful of
    // listeners per channel; bumping the soft cap to 100 keeps Node's
    // MaxListenersExceededWarning quiet for typical use.
    this.emitter.setMaxListeners(100);
    const sweepMs = opts?.sweepIntervalMs ?? 10_000;
    this.sweepInterval = setInterval(() => this.sweep(), sweepMs);
    // Don't keep the event loop alive just for the sweep — long-lived
    // server processes will stay running on their own work.
    this.sweepInterval.unref?.();
  }

  private sweep(): void {
    const now = Date.now();
    for (const [key, entry] of this.store) {
      if (entry.expiresAt !== undefined && entry.expiresAt <= now) {
        this.store.delete(key);
      }
    }
  }

  private isExpired(entry: StoredValue): boolean {
    return entry.expiresAt !== undefined && entry.expiresAt <= Date.now();
  }

  // ── Key/Value ──

  async get(key: string): Promise<string | null> {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(entry)) {
      this.store.delete(key);
      return null;
    }
    return entry.value;
  }

  async set(key: string, value: string): Promise<void> {
    this.store.set(key, { value });
  }

  async setex(key: string, seconds: number, value: string): Promise<void> {
    this.store.set(key, { value, expiresAt: Date.now() + seconds * 1000 });
  }

  async del(...keys: string[]): Promise<number> {
    let count = 0;
    for (const key of keys) {
      if (this.store.delete(key)) count++;
      if (this.hashes.delete(key)) count++;
      if (this.lists.delete(key)) count++;
      if (this.sortedSets.delete(key)) count++;
    }
    return count;
  }

  async keys(pattern: string): Promise<string[]> {
    const regex = globToRegex(pattern);
    const result: string[] = [];
    for (const key of this.store.keys()) {
      if (regex.test(key)) {
        const entry = this.store.get(key);
        if (entry && !this.isExpired(entry)) result.push(key);
      }
    }
    return result;
  }

  async exists(...keys: string[]): Promise<number> {
    let count = 0;
    for (const key of keys) {
      const entry = this.store.get(key);
      if (entry && !this.isExpired(entry)) { count++; continue; }
      if (this.hashes.has(key)) { count++; continue; }
      if (this.lists.has(key)) { count++; continue; }
      if (this.sortedSets.has(key)) { count++; continue; }
    }
    return count;
  }

  // ── Hashes ──

  async hset(key: string, field: string, value: string): Promise<void> {
    let hash = this.hashes.get(key);
    if (!hash) {
      hash = new Map();
      this.hashes.set(key, hash);
    }
    hash.set(field, value);
  }

  async hget(key: string, field: string): Promise<string | null> {
    return this.hashes.get(key)?.get(field) ?? null;
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    const hash = this.hashes.get(key);
    if (!hash) return {};
    return Object.fromEntries(hash);
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    const hash = this.hashes.get(key);
    if (!hash) return 0;
    let count = 0;
    for (const field of fields) {
      if (hash.delete(field)) count++;
    }
    if (hash.size === 0) this.hashes.delete(key);
    return count;
  }

  // ── Lists ──

  async rpush(key: string, ...values: string[]): Promise<number> {
    let list = this.lists.get(key);
    if (!list) { list = []; this.lists.set(key, list); }
    list.push(...values);
    return list.length;
  }

  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    const list = this.lists.get(key);
    if (!list) return [];
    const end = stop === -1 ? list.length : stop + 1;
    return list.slice(start, end);
  }

  // ── TTL ──

  async expire(key: string, seconds: number): Promise<void> {
    const entry = this.store.get(key);
    if (entry) entry.expiresAt = Date.now() + seconds * 1000;
  }

  async pexpire(key: string, ms: number): Promise<void> {
    const entry = this.store.get(key);
    if (entry) entry.expiresAt = Date.now() + ms;
  }

  // ── Sorted Sets ──

  async zadd(key: string, score: number, member: string): Promise<void> {
    let set = this.sortedSets.get(key);
    if (!set) { set = []; this.sortedSets.set(key, set); }
    // Remove existing entry for this member (ZADD overwrites).
    const idx = set.findIndex(e => e.member === member);
    if (idx !== -1) set.splice(idx, 1);
    // Insert in score order.
    const insertIdx = set.findIndex(e => e.score > score);
    if (insertIdx === -1) set.push({ score, member });
    else set.splice(insertIdx, 0, { score, member });
  }

  async zcard(key: string): Promise<number> {
    return this.sortedSets.get(key)?.length ?? 0;
  }

  async zrange(key: string, start: number, stop: number, ..._args: string[]): Promise<string[]> {
    const set = this.sortedSets.get(key);
    if (!set) return [];
    const end = stop === -1 ? set.length : stop + 1;
    return set.slice(start, end).map(e => e.member);
  }

  async zremrangebyscore(key: string, min: number | string, max: number | string): Promise<number> {
    const set = this.sortedSets.get(key);
    if (!set) return 0;
    const minN = min === '-inf' ? -Infinity : Number(min);
    const maxN = max === '+inf' ? Infinity : Number(max);
    const before = set.length;
    const filtered = set.filter(e => e.score < minN || e.score > maxN);
    this.sortedSets.set(key, filtered);
    return before - filtered.length;
  }

  // ── Pub/Sub ──

  async publish(channel: string, message: string): Promise<void> {
    if (this.subscriptions.has(channel)) {
      this.emitter.emit('message', channel, message);
    }
    for (const pattern of this.patternSubscriptions) {
      if (globToRegex(pattern).test(channel)) {
        this.emitter.emit('pmessage', pattern, channel, message);
      }
    }
  }

  async subscribe(channel: string): Promise<void> {
    this.subscriptions.add(channel);
  }

  async unsubscribe(...channels: string[]): Promise<void> {
    for (const ch of channels) this.subscriptions.delete(ch);
  }

  async psubscribe(pattern: string): Promise<void> {
    this.patternSubscriptions.add(pattern);
  }

  on(event: string, handler: RedisGenericHandler): void {
    this.emitter.on(event, handler);
  }

  // ── Scripting ──

  async eval(script: string, _numKeys: number, ...args: (string | number)[]): Promise<unknown> {
    // Sliding-window-rate-limit shape recognition. The classic Lua
    // script: ZADD key now member, ZREMRANGEBYSCORE key -inf (now-window),
    // ZCARD key, EXPIRE key window. Args layout: [key, now, window, member?].
    // Real apps use this constantly (rate limits, sliding-window auth
    // throttles), so it's worth handling natively in memory mode.
    if (script.includes('ZADD') || script.includes('zadd')) {
      const key = String(args[0]);
      const now = Number(args[1]);
      const window = Number(args[2]);
      const member = String(args[3] ?? now);

      await this.zremrangebyscore(key, '-inf', now - window);
      await this.zadd(key, now, member);
      const count = await this.zcard(key);
      await this.expire(key, Math.ceil(window / 1000));
      return count;
    }

    // Anything we don't recognize: warn once, return 0. The warn helps
    // surface "you're using a Lua script in memory mode and it isn't
    // doing what you think" during development.
    if (!this.silentEvalFallback && !this.warnedAboutEvalFallback) {
      this.warnedAboutEvalFallback = true;
      process.stderr.write(
        '[redisflex/memory] eval() received an unrecognized Lua script; returning 0. ' +
        'Add a native handler in memory-adapter.ts or pass { silentEvalFallback: true } to silence.\n',
      );
    }
    return 0;
  }

  /** Create a duplicate. In memory mode, duplicates SHARE backing
   *  state — that's how pub/sub fans out across the "two connections"
   *  pattern apps use with real ioredis. */
  duplicate(): MemoryRedisAdapter {
    const dup = new MemoryRedisAdapter({ silentEvalFallback: this.silentEvalFallback });
    dup.store = this.store;
    dup.hashes = this.hashes;
    dup.lists = this.lists;
    dup.sortedSets = this.sortedSets;
    dup.emitter = this.emitter;
    dup.subscriptions = this.subscriptions;
    dup.patternSubscriptions = this.patternSubscriptions;
    // The duplicate's own sweep interval is redundant — share the
    // primary's. Cancel it so we don't double-sweep.
    if (dup.sweepInterval) {
      clearInterval(dup.sweepInterval);
      dup.sweepInterval = null;
    }
    return dup;
  }

  async quit(): Promise<void> {
    if (this.sweepInterval) {
      clearInterval(this.sweepInterval);
      this.sweepInterval = null;
    }
    this.store.clear();
    this.hashes.clear();
    this.lists.clear();
    this.sortedSets.clear();
    this.emitter.removeAllListeners();
  }
}
