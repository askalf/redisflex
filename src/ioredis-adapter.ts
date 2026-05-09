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

  constructor(url: string, opts?: IoRedisAdapterOptions) {
    this.client = new Redis(url, {
      maxRetriesPerRequest: opts?.maxRetriesPerRequest ?? 3,
      ...(opts?.lazyConnect !== undefined ? { lazyConnect: opts.lazyConnect } : {}),
    });
  }

  // ── Key/Value ──
  async get(key: string): Promise<string | null> { return this.client.get(key); }
  async set(key: string, value: string): Promise<void> { await this.client.set(key, value); }
  async setex(key: string, seconds: number, value: string): Promise<void> { await this.client.setex(key, seconds, value); }
  async del(...keys: string[]): Promise<number> { return this.client.del(...keys); }
  async keys(pattern: string): Promise<string[]> { return this.client.keys(pattern); }
  async exists(...keys: string[]): Promise<number> { return this.client.exists(...keys); }

  // ── Lists ──
  async rpush(key: string, ...values: string[]): Promise<number> { return this.client.rpush(key, ...values); }
  async lrange(key: string, start: number, stop: number): Promise<string[]> { return this.client.lrange(key, start, stop); }

  // ── Hashes ──
  async hset(key: string, field: string, value: string): Promise<void> { await this.client.hset(key, field, value); }
  async hget(key: string, field: string): Promise<string | null> { return this.client.hget(key, field); }
  async hgetall(key: string): Promise<Record<string, string>> { return this.client.hgetall(key); }
  async hdel(key: string, ...fields: string[]): Promise<number> { return this.client.hdel(key, ...fields); }

  // ── TTL ──
  async expire(key: string, seconds: number): Promise<void> { await this.client.expire(key, seconds); }
  async pexpire(key: string, ms: number): Promise<void> { await this.client.pexpire(key, ms); }

  // ── Sorted Sets ──
  async zadd(key: string, score: number, member: string): Promise<void> { await this.client.zadd(key, score, member); }
  async zcard(key: string): Promise<number> { return this.client.zcard(key); }
  async zrange(key: string, start: number, stop: number, ...args: string[]): Promise<string[]> {
    return this.client.zrange(key, start, stop, ...(args as []));
  }
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
    const dup = new IoRedisAdapter('', { lazyConnect: true });
    dup.client = this.client.duplicate();
    return dup;
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
