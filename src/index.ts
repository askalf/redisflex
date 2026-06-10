/**
 * @askalf/redisflex — one Redis API, two modes.
 *
 *   import { createRedisAdapter } from '@askalf/redisflex';
 *
 *   // Production: real Redis server
 *   const redis = createRedisAdapter({
 *     mode: 'ioredis',
 *     url: process.env.REDIS_URL!,
 *   });
 *
 *   // Dev / standalone: in-process, no Redis server
 *   const redis = createRedisAdapter({ mode: 'memory' });
 *
 *   await redis.set('key', 'value');
 *   await redis.publish('channel', 'message');
 *
 * Same surface across both modes — get/set, counters, hashes, lists,
 * sets, sorted sets, pub/sub, expiry, and a
 * sliding-window-rate-limit-shaped Lua eval.
 *
 * Also ships a BullMQ-shaped in-memory queue (`InMemoryQueue`,
 * `InMemoryWorker`) so you can drop the Redis dep entirely for queueing
 * needs in dev / standalone — see memory-queue.ts.
 */

export type {
  IRedisAdapter,
  RedisMessageHandler,
  RedisPatternHandler,
  RedisGenericHandler,
} from './interface.js';
export { IoRedisAdapter, type IoRedisAdapterOptions } from './ioredis-adapter.js';
export { MemoryRedisAdapter, type MemoryRedisAdapterOptions } from './memory-adapter.js';
export {
  InMemoryQueue,
  InMemoryWorker,
  type Job,
  type JobData,
  type JobOpts,
  type JobState,
  type ProcessorFn,
} from './memory-queue.js';

import { IoRedisAdapter, type IoRedisAdapterOptions } from './ioredis-adapter.js';
import { MemoryRedisAdapter, type MemoryRedisAdapterOptions } from './memory-adapter.js';
import type { IRedisAdapter } from './interface.js';

/** Strict union of valid adapter configs. The mode field discriminates. */
export type RedisAdapterConfig =
  | { mode: 'ioredis'; url: string; opts?: IoRedisAdapterOptions }
  | ({ mode: 'memory' } & MemoryRedisAdapterOptions);

/** Build a Redis adapter from explicit config. */
export function createRedisAdapter(config: RedisAdapterConfig): IRedisAdapter {
  if (config.mode === 'memory') {
    const { mode: _mode, ...rest } = config;
    void _mode;
    return new MemoryRedisAdapter(rest);
  }
  return new IoRedisAdapter(config.url, config.opts);
}

export interface FromEnvOptions {
  /** Env var name that selects the mode. Default `'REDISFLEX_MODE'`. Set
   *  to `'memory'` for in-process, anything else for ioredis. */
  modeEnvVar?: string;
  /** Env var name that holds the Redis URL for `ioredis` mode.
   *  Default `'REDIS_URL'`. */
  urlEnvVar?: string;
  /** Fallback URL when the URL env var is unset and mode is ioredis.
   *  Pass `null` to make it required (throws). Default
   *  `'redis://localhost:6379'` (matches ioredis's own default). */
  defaultUrl?: string | null;
}

/** Build a Redis adapter from environment variables. Convenient for
 *  apps that flip modes via deploy config rather than code change. */
export function createRedisAdapterFromEnv(opts: FromEnvOptions = {}): IRedisAdapter {
  const modeVar = opts.modeEnvVar ?? 'REDISFLEX_MODE';
  const urlVar = opts.urlEnvVar ?? 'REDIS_URL';

  if (process.env[modeVar] === 'memory') {
    return createRedisAdapter({ mode: 'memory' });
  }

  const url = process.env[urlVar] ?? opts.defaultUrl;
  if (url === null || url === undefined) {
    throw new Error(
      `${urlVar} is required for ioredis mode (set ${modeVar}=memory to use the in-process backend instead, or pass defaultUrl: ... to createRedisAdapterFromEnv)`,
    );
  }

  // The default fallback `'redis://localhost:6379'` lands here too.
  return createRedisAdapter({ mode: 'ioredis', url });
}
