/**
 * MemoryRedisAdapter — full IRedisAdapter implementation backed by
 * native JS data structures. Drop-in replacement for ioredis when you
 * don't want a Redis server in dev / standalone / CI.
 *
 * Backing structures:
 *   - Key/Value + counters: Map<string, string>
 *   - Hashes: Map<string, Map<string, string>>
 *   - Lists: Map<string, string[]>
 *   - Sets: Map<string, Set<string>>
 *   - Sorted sets: Map<string, Array<{score, member}>> kept in score order
 *   - TTLs: one expirations Map<key, wall-clock ms> covering EVERY key
 *     type, enforced lazily on each read + a 10s sweep interval
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

interface SortedSetEntry {
  score: number;
  member: string;
}

/** A ZRANGEBYSCORE-style bound after parsing: the numeric value plus
 *  whether the comparison excludes it (the '(n' form). */
interface ScoreBound {
  value: number;
  exclusive: boolean;
}

function globToRegex(pattern: string): RegExp {
  const escaped = pattern
    .replace(/[.+^${}()|[\]\\]/g, '\\$&')
    .replace(/\*/g, '.*')
    .replace(/\?/g, '.');
  return new RegExp(`^${escaped}$`);
}

/** Parse a score bound: number, numeric string, '-inf' / '+inf' / 'inf',
 *  or the exclusive '(n' form. Shared by zrangebyscore and
 *  zremrangebyscore. Throws on garbage instead of letting NaN poison
 *  the comparison (which used to make '(5' remove everything). */
function parseScoreBound(bound: number | string): ScoreBound {
  if (typeof bound === 'number') return { value: bound, exclusive: false };
  let raw = bound;
  let exclusive = false;
  if (raw.startsWith('(')) { exclusive = true; raw = raw.slice(1); }
  if (raw === '-inf') return { value: -Infinity, exclusive };
  if (raw === '+inf' || raw === 'inf') return { value: Infinity, exclusive };
  const value = Number(raw);
  if (raw === '' || Number.isNaN(value)) {
    throw new Error('ERR min or max is not a float');
  }
  return { value, exclusive };
}

function scoreInRange(score: number, min: ScoreBound, max: ScoreBound): boolean {
  const aboveMin = min.exclusive ? score > min.value : score >= min.value;
  const belowMax = max.exclusive ? score < max.value : score <= max.value;
  return aboveMin && belowMax;
}

/** Normalize Redis-style start/stop indexes (negative = offset from the
 *  end) into a concrete inclusive [start, stop] pair, or null when the
 *  resulting range is empty. Mirrors LRANGE/ZRANGE semantics:
 *  out-of-bounds indexes clamp rather than error. */
function normalizeRange(length: number, start: number, stop: number): [number, number] | null {
  let s = start < 0 ? length + start : start;
  let e = stop < 0 ? length + stop : stop;
  if (s < 0) s = 0;
  if (e >= length) e = length - 1;
  if (s >= length || e < 0 || s > e) return null;
  return [s, e];
}

/** Parse `raw` as a Redis integer (missing key/field counts as 0) and
 *  add `delta`. Throws the same error real Redis raises when the stored
 *  value isn't an integer. */
function addToInteger(raw: string | undefined, delta: number): number {
  if (raw === undefined) return delta;
  if (!/^-?\d+$/.test(raw)) {
    throw new Error('ERR value is not an integer or out of range');
  }
  return Number(raw) + delta;
}

export class MemoryRedisAdapter implements IRedisAdapter {
  readonly mode = 'memory' as const;

  private store = new Map<string, string>();
  private hashes = new Map<string, Map<string, string>>();
  private lists = new Map<string, string[]>();
  private sets = new Map<string, Set<string>>();
  private sortedSets = new Map<string, SortedSetEntry[]>();
  /** Wall-clock ms when each key expires. One map for ALL key types —
   *  strings, hashes, lists, sets, sorted sets — so expire() works on
   *  everything, like real Redis. */
  private expirations = new Map<string, number>();
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
    for (const [key, expiresAt] of this.expirations) {
      if (expiresAt <= now) this.removeKey(key);
    }
  }

  /** Remove `key` from every backing store + the expirations map.
   *  Returns true when the key existed in at least one store. */
  private removeKey(key: string): boolean {
    let existed = false;
    if (this.store.delete(key)) existed = true;
    if (this.hashes.delete(key)) existed = true;
    if (this.lists.delete(key)) existed = true;
    if (this.sets.delete(key)) existed = true;
    if (this.sortedSets.delete(key)) existed = true;
    this.expirations.delete(key);
    return existed;
  }

  /** Lazily evict `key` if its TTL has passed. Every read/write path
   *  calls this first so expired keys behave as gone even between
   *  sweeps. Returns true when the key was expired (and is now gone). */
  private expireIfNeeded(key: string): boolean {
    const expiresAt = this.expirations.get(key);
    if (expiresAt === undefined || expiresAt > Date.now()) return false;
    this.removeKey(key);
    return true;
  }

  private hasKey(key: string): boolean {
    return this.store.has(key) || this.hashes.has(key) || this.lists.has(key)
      || this.sets.has(key) || this.sortedSets.has(key);
  }

  /** Redis deletes a collection key when its last element is removed —
   *  and the TTL dies with it. Call after emptying a hash/list/set/zset. */
  private dropTtlIfGone(key: string): void {
    if (!this.hasKey(key)) this.expirations.delete(key);
  }

  // ── Key/Value ──

  async get(key: string): Promise<string | null> {
    this.expireIfNeeded(key);
    return this.store.get(key) ?? null;
  }

  async set(key: string, value: string, ...args: (string | number)[]): Promise<void> {
    let expiresAt: number | undefined;
    // EX/PX come in option/operand pairs; anything else is rejected
    // loudly — silently ignoring NX/XX/KEEPTTL would hand callers
    // different semantics per mode without telling them.
    for (let i = 0; i < args.length; i += 2) {
      const option = String(args[i]).toUpperCase();
      if (option !== 'EX' && option !== 'PX') {
        throw new Error(
          `[redisflex/memory] set(): unsupported option "${String(args[i])}" — memory mode handles EX <seconds> and PX <ms> only`,
        );
      }
      const operand = args[i + 1];
      const n = Number(operand);
      if (operand === undefined || !Number.isFinite(n) || n <= 0) {
        throw new Error(
          `[redisflex/memory] set(): ${option} requires a positive number, got "${String(operand)}"`,
        );
      }
      expiresAt = Date.now() + (option === 'EX' ? n * 1000 : n);
    }
    this.store.set(key, value);
    // SET without an expiry option clears any existing TTL (Redis
    // semantics — there's no implicit KEEPTTL).
    if (expiresAt !== undefined) this.expirations.set(key, expiresAt);
    else this.expirations.delete(key);
  }

  async setex(key: string, seconds: number, value: string): Promise<void> {
    this.store.set(key, value);
    this.expirations.set(key, Date.now() + seconds * 1000);
  }

  async mget(...keys: string[]): Promise<(string | null)[]> {
    return Promise.all(keys.map((key) => this.get(key)));
  }

  async del(...keys: string[]): Promise<number> {
    let count = 0;
    for (const key of keys) {
      if (this.expireIfNeeded(key)) continue; // expired = already gone
      if (this.removeKey(key)) count++;
    }
    return count;
  }

  async keys(pattern: string): Promise<string[]> {
    const regex = globToRegex(pattern);
    // Union across every store — a key can live in any of them.
    const all = new Set<string>([
      ...this.store.keys(),
      ...this.hashes.keys(),
      ...this.lists.keys(),
      ...this.sets.keys(),
      ...this.sortedSets.keys(),
    ]);
    const result: string[] = [];
    for (const key of all) {
      if (!regex.test(key)) continue;
      if (this.expireIfNeeded(key)) continue;
      result.push(key);
    }
    return result;
  }

  async exists(...keys: string[]): Promise<number> {
    let count = 0;
    for (const key of keys) {
      if (this.expireIfNeeded(key)) continue;
      if (this.hasKey(key)) count++;
    }
    return count;
  }

  // ── Counters ──

  async incr(key: string): Promise<number> {
    return this.incrby(key, 1);
  }

  async decr(key: string): Promise<number> {
    return this.incrby(key, -1);
  }

  async incrby(key: string, n: number): Promise<number> {
    this.expireIfNeeded(key);
    const next = addToInteger(this.store.get(key), n);
    this.store.set(key, String(next));
    return next;
  }

  async hincrby(key: string, field: string, n: number): Promise<number> {
    this.expireIfNeeded(key);
    let hash = this.hashes.get(key);
    if (!hash) {
      hash = new Map();
      this.hashes.set(key, hash);
    }
    const next = addToInteger(hash.get(field), n);
    hash.set(field, String(next));
    return next;
  }

  // ── Hashes ──

  async hset(key: string, field: string, value: string): Promise<void> {
    this.expireIfNeeded(key);
    let hash = this.hashes.get(key);
    if (!hash) {
      hash = new Map();
      this.hashes.set(key, hash);
    }
    hash.set(field, value);
  }

  async hget(key: string, field: string): Promise<string | null> {
    this.expireIfNeeded(key);
    return this.hashes.get(key)?.get(field) ?? null;
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    this.expireIfNeeded(key);
    const hash = this.hashes.get(key);
    if (!hash) return {};
    return Object.fromEntries(hash);
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    this.expireIfNeeded(key);
    const hash = this.hashes.get(key);
    if (!hash) return 0;
    let count = 0;
    for (const field of fields) {
      if (hash.delete(field)) count++;
    }
    if (hash.size === 0) {
      this.hashes.delete(key);
      this.dropTtlIfGone(key);
    }
    return count;
  }

  // ── Lists ──

  async lpush(key: string, ...values: string[]): Promise<number> {
    this.expireIfNeeded(key);
    let list = this.lists.get(key);
    if (!list) { list = []; this.lists.set(key, list); }
    // LPUSH inserts each value at the head in turn, so the final order
    // is the reverse of the argument list.
    list.unshift(...[...values].reverse());
    return list.length;
  }

  async rpush(key: string, ...values: string[]): Promise<number> {
    this.expireIfNeeded(key);
    let list = this.lists.get(key);
    if (!list) { list = []; this.lists.set(key, list); }
    list.push(...values);
    return list.length;
  }

  async lpop(key: string): Promise<string | null> {
    this.expireIfNeeded(key);
    const list = this.lists.get(key);
    if (!list || list.length === 0) return null;
    const value = list.shift() ?? null;
    if (list.length === 0) {
      this.lists.delete(key);
      this.dropTtlIfGone(key);
    }
    return value;
  }

  async rpop(key: string): Promise<string | null> {
    this.expireIfNeeded(key);
    const list = this.lists.get(key);
    if (!list || list.length === 0) return null;
    const value = list.pop() ?? null;
    if (list.length === 0) {
      this.lists.delete(key);
      this.dropTtlIfGone(key);
    }
    return value;
  }

  async llen(key: string): Promise<number> {
    this.expireIfNeeded(key);
    return this.lists.get(key)?.length ?? 0;
  }

  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    this.expireIfNeeded(key);
    const list = this.lists.get(key);
    if (!list) return [];
    const range = normalizeRange(list.length, start, stop);
    if (!range) return [];
    return list.slice(range[0], range[1] + 1);
  }

  // ── Sets ──

  async sadd(key: string, ...members: string[]): Promise<number> {
    this.expireIfNeeded(key);
    let set = this.sets.get(key);
    if (!set) { set = new Set(); this.sets.set(key, set); }
    let added = 0;
    for (const member of members) {
      if (!set.has(member)) { set.add(member); added++; }
    }
    return added;
  }

  async srem(key: string, ...members: string[]): Promise<number> {
    this.expireIfNeeded(key);
    const set = this.sets.get(key);
    if (!set) return 0;
    let removed = 0;
    for (const member of members) {
      if (set.delete(member)) removed++;
    }
    if (set.size === 0) {
      this.sets.delete(key);
      this.dropTtlIfGone(key);
    }
    return removed;
  }

  async smembers(key: string): Promise<string[]> {
    this.expireIfNeeded(key);
    return [...(this.sets.get(key) ?? [])];
  }

  async sismember(key: string, member: string): Promise<number> {
    this.expireIfNeeded(key);
    return this.sets.get(key)?.has(member) ? 1 : 0;
  }

  async scard(key: string): Promise<number> {
    this.expireIfNeeded(key);
    return this.sets.get(key)?.size ?? 0;
  }

  // ── TTL ──

  async expire(key: string, seconds: number): Promise<void> {
    await this.pexpire(key, seconds * 1000);
  }

  async pexpire(key: string, ms: number): Promise<void> {
    if (this.expireIfNeeded(key)) return;
    // EXPIRE on a missing key is a no-op (Redis returns 0).
    if (this.hasKey(key)) this.expirations.set(key, Date.now() + ms);
  }

  async ttl(key: string): Promise<number> {
    const ms = await this.pttl(key);
    return ms < 0 ? ms : Math.ceil(ms / 1000);
  }

  async pttl(key: string): Promise<number> {
    if (this.expireIfNeeded(key)) return -2;
    if (!this.hasKey(key)) return -2;
    const expiresAt = this.expirations.get(key);
    if (expiresAt === undefined) return -1;
    return expiresAt - Date.now();
  }

  // ── Sorted Sets ──

  async zadd(key: string, score: number, member: string): Promise<void> {
    this.expireIfNeeded(key);
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
    this.expireIfNeeded(key);
    return this.sortedSets.get(key)?.length ?? 0;
  }

  async zscore(key: string, member: string): Promise<string | null> {
    this.expireIfNeeded(key);
    const entry = this.sortedSets.get(key)?.find(e => e.member === member);
    // ioredis returns scores as strings; match that.
    return entry ? String(entry.score) : null;
  }

  async zrange(key: string, start: number, stop: number, ..._args: string[]): Promise<string[]> {
    this.expireIfNeeded(key);
    const set = this.sortedSets.get(key);
    if (!set) return [];
    const range = normalizeRange(set.length, start, stop);
    if (!range) return [];
    return set.slice(range[0], range[1] + 1).map(e => e.member);
  }

  async zrangebyscore(key: string, min: number | string, max: number | string): Promise<string[]> {
    this.expireIfNeeded(key);
    const set = this.sortedSets.get(key);
    if (!set) return [];
    const lo = parseScoreBound(min);
    const hi = parseScoreBound(max);
    return set.filter(e => scoreInRange(e.score, lo, hi)).map(e => e.member);
  }

  async zrem(key: string, ...members: string[]): Promise<number> {
    this.expireIfNeeded(key);
    const set = this.sortedSets.get(key);
    if (!set) return 0;
    let removed = 0;
    for (const member of members) {
      const idx = set.findIndex(e => e.member === member);
      if (idx !== -1) { set.splice(idx, 1); removed++; }
    }
    if (set.length === 0) {
      this.sortedSets.delete(key);
      this.dropTtlIfGone(key);
    }
    return removed;
  }

  async zremrangebyscore(key: string, min: number | string, max: number | string): Promise<number> {
    this.expireIfNeeded(key);
    const set = this.sortedSets.get(key);
    if (!set) return 0;
    const lo = parseScoreBound(min);
    const hi = parseScoreBound(max);
    const kept = set.filter(e => !scoreInRange(e.score, lo, hi));
    const removed = set.length - kept.length;
    if (kept.length === 0) {
      this.sortedSets.delete(key);
      this.dropTtlIfGone(key);
    } else {
      this.sortedSets.set(key, kept);
    }
    return removed;
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
    dup.sets = this.sets;
    dup.sortedSets = this.sortedSets;
    dup.expirations = this.expirations;
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
    this.sets.clear();
    this.sortedSets.clear();
    this.expirations.clear();
    this.emitter.removeAllListeners();
  }
}
