/**
 * IoRedisAdapter — wraps `ioredis`. The "production" mode: connects to
 * a real Redis server (Docker, ElastiCache, Upstash, Redis Cloud, any
 * RESP-speaking endpoint).
 *
 * Defaults: maxRetriesPerRequest 3 (so a flapping Redis doesn't turn
 * into unbounded retry storms), and lazyConnect off by default
 * (constructor connects synchronously). Pass `{ lazyConnect: true }`
 * if you want to defer the TCP handshake until the first command —
 * useful for tests that mock the connection.
 */

import { Redis } from 'ioredis';
import type {
  IRedisAdapter,
  RedisGenericHandler,
} from './interface.js';

export interface IoRedisAdapterOptions {
  /** Defer the TCP connect until the first command. Default false. */
  lazyConnect?: boolean;
  /** Max retries per request before failing. Default 3. */
  maxRetriesPerRequest?: number;
}

export class IoRedisAdapter implements IRedisAdapter {
  readonly mode = 'ioredis' as const;
  private client: Redis;

  /** The third parameter is internal — duplicate() uses it to wrap an
   *  already-constructed client so no throwaway Redis instance (and no
   *  spurious connection attempt) ever gets created. When present, the
   *  url/opts arguments are ignored. */
  constructor(url: string, opts?: IoRedisAdapterOptions, existingClient?: Redis) {
    this.client = existingClient ?? new Redis(url, {
      maxRetriesPerRequest: opts?.maxRetriesPerRequest ?? 3,
      ...(opts?.lazyConnect !== undefined ? { lazyConnect: opts.lazyConnect } : {}),
    });
  }

  // ── Key/Value ──
  async get(key: string): Promise<string | null> { return this.client.get(key); }
  async set(key: string, value: string, ...args: (string | number)[]): Promise<void> {
    // ioredis types SET's options as a tower of overloads that a
    // variadic passthrough can't satisfy; erase them the same way the
    // interface's generic on() handler does.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const rawSet = this.client.set.bind(this.client) as (...setArgs: any[]) => Promise<unknown>;
    await rawSet(key, value, ...args);
  }
  async setex(key: string, seconds: number, value: string): Promise<void> { await this.client.setex(key, seconds, value); }
  async mget(...keys: string[]): Promise<(string | null)[]> { return this.client.mget(...keys); }
  async del(...keys: string[]): Promise<number> { return this.client.del(...keys); }
  async keys(pattern: string): Promise<string[]> { return this.client.keys(pattern); }
  async exists(...keys: string[]): Promise<number> { return this.client.exists(...keys); }

  // ── Counters ──
  async incr(key: string): Promise<number> { return this.client.incr(key); }
  async decr(key: string): Promise<number> { return this.client.decr(key); }
  async incrby(key: string, n: number): Promise<number> { return this.client.incrby(key, n); }
  async hincrby(key: string, field: string, n: number): Promise<number> { return this.client.hincrby(key, field, n); }

  // ── Lists ──
  async lpush(key: string, ...values: string[]): Promise<number> { return this.client.lpush(key, ...values); }
  async rpush(key: string, ...values: string[]): Promise<number> { return this.client.rpush(key, ...values); }
  async lpop(key: string): Promise<string | null> { return this.client.lpop(key); }
  async rpop(key: string): Promise<string | null> { return this.client.rpop(key); }
  async llen(key: string): Promise<number> { return this.client.llen(key); }
  async lrange(key: string, start: number, stop: number): Promise<string[]> { return this.client.lrange(key, start, stop); }

  // ── Sets ──
  async sadd(key: string, ...members: string[]): Promise<number> { return this.client.sadd(key, ...members); }
  async srem(key: string, ...members: string[]): Promise<number> { return this.client.srem(key, ...members); }
  async smembers(key: string): Promise<string[]> { return this.client.smembers(key); }
  async sismember(key: string, member: string): Promise<number> { return this.client.sismember(key, member); }
  async scard(key: string): Promise<number> { return this.client.scard(key); }

  // ── Hashes ──
  async hset(key: string, field: string, value: string): Promise<void> { await this.client.hset(key, field, value); }
  async hget(key: string, field: string): Promise<string | null> { return this.client.hget(key, field); }
  async hgetall(key: string): Promise<Record<string, string>> { return this.client.hgetall(key); }
  async hdel(key: string, ...fields: string[]): Promise<number> { return this.client.hdel(key, ...fields); }

  // ── TTL ──
  async expire(key: string, seconds: number): Promise<void> { await this.client.expire(key, seconds); }
  async pexpire(key: string, ms: number): Promise<void> { await this.client.pexpire(key, ms); }
  async ttl(key: string): Promise<number> { return this.client.ttl(key); }
  async pttl(key: string): Promise<number> { return this.client.pttl(key); }

  // ── Sorted Sets ──
  async zadd(key: string, score: number, member: string): Promise<void> { await this.client.zadd(key, score, member); }
  async zcard(key: string): Promise<number> { return this.client.zcard(key); }
  async zscore(key: string, member: string): Promise<string | null> { return this.client.zscore(key, member); }
  async zrange(key: string, start: number, stop: number, ...args: string[]): Promise<string[]> {
    // ioredis 6 narrowed ZRANGE's `stop` to `string | Buffer` (only
    // `start` still takes a number). Redis parses the index off the
    // wire identically either way, so stringify to satisfy the overload.
    return this.client.zrange(key, start, String(stop), ...(args as []));
  }
  async zrangebyscore(key: string, min: number | string, max: number | string): Promise<string[]> {
    return this.client.zrangebyscore(key, min, max);
  }
  async zrem(key: string, ...members: string[]): Promise<number> { return this.client.zrem(key, ...members); }
  async zremrangebyscore(key: string, min: number | string, max: number | string): Promise<number> {
    return this.client.zremrangebyscore(key, min, max);
  }

  // ── Pub/Sub ──
  async publish(channel: string, message: string): Promise<void> { await this.client.publish(channel, message); }
  async subscribe(channel: string): Promise<void> { await this.client.subscribe(channel); }
  async unsubscribe(...channels: string[]): Promise<void> { await this.client.unsubscribe(...channels); }
  async psubscribe(pattern: string): Promise<void> { await this.client.psubscribe(pattern); }
  on(event: string, handler: RedisGenericHandler): void { this.client.on(event, handler); }

  // ── Scripting ──
  async eval(script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown> {
    return this.client.eval(script, numKeys, ...args);
  }

  /** Create a duplicate connection. Redis pub/sub requires a separate
   *  TCP connection from the one issuing commands; this is the same
   *  contract `Redis.prototype.duplicate()` provides. */
  duplicate(): IoRedisAdapter {
    return new IoRedisAdapter('', undefined, this.client.duplicate());
  }

  /** Escape hatch: get the underlying ioredis `Redis` client for code
   *  that needs commands not exposed on `IRedisAdapter` (streams, geo,
   *  cluster ops, MULTI/EXEC). Code that touches the raw client won't
   *  work in `memory` mode. */
  getClient(): Redis {
    return this.client;
  }

  async quit(): Promise<void> { await this.client.quit(); }
}
