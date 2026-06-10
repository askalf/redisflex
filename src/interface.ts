/**
 * Redis adapter interface — the contract every redisflex adapter
 * implements. Lets your app issue one set of Redis calls that runs
 * against either a real Redis server (`ioredis` mode) or an in-process
 * Map+EventEmitter (`memory` mode) without changing call sites.
 *
 * Surface covers what real apps actually use day-to-day:
 *   - Key/value (get, set [+ EX/PX], setex, mget, del, keys, exists)
 *   - Counters (incr, decr, incrby, hincrby)
 *   - Hashes (hset/hget/hgetall/hdel)
 *   - Lists (lpush, rpush, lpop, rpop, llen, lrange)
 *   - Sets (sadd, srem, smembers, sismember, scard)
 *   - TTL (expire, pexpire, ttl, pttl)
 *   - Sorted sets (zadd, zcard, zscore, zrange, zrangebyscore, zrem,
 *     zremrangebyscore)
 *   - Pub/Sub (publish, subscribe, psubscribe + on())
 *   - Lua eval (with sliding-window-rate-limit shape recognized in
 *     memory mode; see memory-adapter.ts for the heuristic)
 *
 * Anything outside this surface — streams, geo, cluster, transactions
 * (MULTI/EXEC), bitmap ops — isn't covered. Open an issue if you need
 * one; most are mechanical to add.
 */

export type RedisMessageHandler = (channel: string, message: string) => void;
export type RedisPatternHandler = (pattern: string, channel: string, message: string) => void;

// ESLint will rightly object to `any` in a public interface, but the
// `on(event, handler)` overload is the same shape ioredis exposes and
// matching it lets redisflex be a drop-in. The two narrow overloads
// above cover the typed cases; the catch-all is the escape hatch.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type RedisGenericHandler = (...args: any[]) => void;

export interface IRedisAdapter {
  // ── Key/Value ──
  get(key: string): Promise<string | null>;
  /** Plain `set(key, value)`, plus optional trailing `'EX' <seconds>`
   *  or `'PX' <ms>` (case-insensitive). Memory mode throws on any
   *  other option token (NX, XX, KEEPTTL, ...) rather than silently
   *  ignoring it; ioredis mode passes everything through to the
   *  server unchanged. */
  set(key: string, value: string, ...args: (string | number)[]): Promise<void>;
  setex(key: string, seconds: number, value: string): Promise<void>;
  /** One null slot per missing key, like real MGET. */
  mget(...keys: string[]): Promise<(string | null)[]>;
  del(...keys: string[]): Promise<number>;
  keys(pattern: string): Promise<string[]>;
  exists(...keys: string[]): Promise<number>;

  // ── Counters ──
  incr(key: string): Promise<number>;
  decr(key: string): Promise<number>;
  incrby(key: string, n: number): Promise<number>;
  hincrby(key: string, field: string, n: number): Promise<number>;

  // ── Hashes ──
  hset(key: string, field: string, value: string): Promise<void>;
  hget(key: string, field: string): Promise<string | null>;
  hgetall(key: string): Promise<Record<string, string>>;
  hdel(key: string, ...fields: string[]): Promise<number>;

  // ── Lists ──
  lpush(key: string, ...values: string[]): Promise<number>;
  rpush(key: string, ...values: string[]): Promise<number>;
  lpop(key: string): Promise<string | null>;
  rpop(key: string): Promise<string | null>;
  llen(key: string): Promise<number>;
  lrange(key: string, start: number, stop: number): Promise<string[]>;

  // ── Sets ──
  /** Returns the count of members newly added (members already in the
   *  set don't count). */
  sadd(key: string, ...members: string[]): Promise<number>;
  srem(key: string, ...members: string[]): Promise<number>;
  smembers(key: string): Promise<string[]>;
  /** 1 when member is in the set, 0 otherwise. */
  sismember(key: string, member: string): Promise<number>;
  scard(key: string): Promise<number>;

  // ── TTL ──
  expire(key: string, seconds: number): Promise<void>;
  pexpire(key: string, ms: number): Promise<void>;
  /** Remaining TTL in seconds (rounded up). -2 = key missing,
   *  -1 = key exists but has no expiry. */
  ttl(key: string): Promise<number>;
  /** Remaining TTL in milliseconds; same -2/-1 sentinels as ttl(). */
  pttl(key: string): Promise<number>;

  // ── Sorted Sets ──
  zadd(key: string, score: number, member: string): Promise<void>;
  zcard(key: string): Promise<number>;
  /** Score as a string (matching ioredis), or null when the member
   *  isn't in the set. */
  zscore(key: string, member: string): Promise<string | null>;
  zrange(key: string, start: number, stop: number, ...args: string[]): Promise<string[]>;
  /** min/max accept numbers, numeric strings, '-inf' / '+inf', and the
   *  exclusive '(n' form — same grammar as real ZRANGEBYSCORE. */
  zrangebyscore(key: string, min: number | string, max: number | string): Promise<string[]>;
  zrem(key: string, ...members: string[]): Promise<number>;
  zremrangebyscore(key: string, min: number | string, max: number | string): Promise<number>;

  // ── Pub/Sub ──
  publish(channel: string, message: string): Promise<void>;
  subscribe(channel: string): Promise<void>;
  unsubscribe(...channels: string[]): Promise<void>;
  psubscribe(pattern: string): Promise<void>;
  on(event: 'message', handler: RedisMessageHandler): void;
  on(event: 'pmessage', handler: RedisPatternHandler): void;
  on(event: string, handler: RedisGenericHandler): void;

  // ── Scripting ──
  eval(script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown>;

  // ── Lifecycle ──
  duplicate(): IRedisAdapter;
  quit(): Promise<void>;

  readonly mode: 'ioredis' | 'memory';
}
