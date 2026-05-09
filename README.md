# @askalf/redisflex

> One Redis API. Two modes. Same call sites.

Switch between **real Redis** (`ioredis`) and **in-process Redis** (Map + EventEmitter) with one line of config. Production runs on real Redis; dev / standalone / "no-Docker mode" runs in-process. Same get/set, hashes, lists, sorted sets, pub/sub — drop the Redis server when you don't need it.

Also ships a **BullMQ-shaped in-memory queue** so you can drop the Redis dep entirely for queueing too.

```bash
npm install @askalf/redisflex
```

[![CI](https://img.shields.io/github/actions/workflow/status/askalf/redisflex/ci.yml?style=flat-square&label=CI&labelColor=020612)](https://github.com/askalf/redisflex/actions)
[![npm](https://img.shields.io/npm/v/@askalf/redisflex?style=flat-square&color=00ff88&label=npm&labelColor=020612)](https://www.npmjs.com/package/@askalf/redisflex)
[![License](https://img.shields.io/badge/MIT-00ff88?style=flat-square&label=license&labelColor=020612)](LICENSE)

## Why

Most apps use Redis for three things: cache, pub/sub, and queueing. Most dev environments don't want to spin up a Redis server for any of them. Most CI environments REALLY don't want it.

`redisflex` swaps a real Redis connection for an in-process implementation that speaks the same surface — `get/set`, hashes, lists, sorted sets, pub/sub, expiry, and a sliding-window-rate-limit-shaped Lua eval. Plus a tiny BullMQ-API-compatible queue if you use BullMQ for jobs.

Your call sites stay identical. Flip the mode in config and your app no longer needs Redis to run.

## Use it

### Direct

```ts
import { createRedisAdapter } from '@askalf/redisflex';

// Production — real Redis
const redis = createRedisAdapter({
  mode: 'ioredis',
  url: process.env.REDIS_URL!,
});

// Dev / standalone — in-process
const redis = createRedisAdapter({ mode: 'memory' });

await redis.set('user:1', 'alice');
await redis.publish('events', 'user.created');
```

### From environment

```ts
import { createRedisAdapterFromEnv } from '@askalf/redisflex';

// REDISFLEX_MODE=memory     → in-process
// otherwise                 → ioredis at $REDIS_URL (or redis://localhost:6379)
const redis = createRedisAdapterFromEnv();
```

Custom env-var names:

```ts
const redis = createRedisAdapterFromEnv({
  modeEnvVar: 'MYAPP_MODE',
  urlEnvVar: 'MYAPP_REDIS',
  defaultUrl: null, // null = throw if URL missing, instead of defaulting
});
```

### In-memory queue

```ts
import { InMemoryQueue, InMemoryWorker } from '@askalf/redisflex';

const queue = new InMemoryQueue('emails');
const worker = new InMemoryWorker(
  'emails',
  async (job) => {
    await sendEmail(job.data);
  },
  { queue, concurrency: 4 },
);

worker.on('completed', (job) => console.log('sent', job.id));
worker.on('failed', (job, err) => console.error('failed', job.id, err));

await queue.add('welcome', { to: 'alice@example.com' });
await queue.add('reminder', { to: 'bob@example.com' }, {
  delay: 60_000,
  attempts: 3,
  backoff: { delay: 5000 }, // exponential: 5s, 10s, 20s
});
```

The shape matches BullMQ's `Queue` / `Worker` so you can swap to real BullMQ later by changing imports.

## What's covered

| Family | Operations |
|---|---|
| Key/Value | `get`, `set`, `setex`, `del`, `keys`, `exists` |
| Hashes | `hset`, `hget`, `hgetall`, `hdel` |
| Lists | `rpush`, `lrange` |
| TTL | `expire`, `pexpire` |
| Sorted Sets | `zadd`, `zcard`, `zrange`, `zremrangebyscore` |
| Pub/Sub | `publish`, `subscribe`, `unsubscribe`, `psubscribe`, `on('message'/'pmessage')` |
| Scripting | `eval` (sliding-window-rate-limit shape recognized in memory mode) |
| Lifecycle | `duplicate`, `quit` |

That's enough surface for cache, pub/sub, BullMQ-style queues, sliding-window rate limits, and most idiomatic Redis usage. Streams, geo, cluster, MULTI/EXEC, bitmap ops aren't covered. Open an issue if you need one — most are mechanical to add.

## Lua eval in memory mode

Memory mode recognizes the canonical sliding-window-rate-limit Lua script (`ZADD` + `ZREMRANGEBYSCORE` + `ZCARD` + `EXPIRE`, args layout `[key, now, window-ms, member?]`) and returns the post-add count. That's enough for typical rate-limiter use.

Anything else: returns `0` and prints a stderr warning once per process. Pass `{ silentEvalFallback: true }` to suppress the warning. If you need a different Lua script handled natively, open an issue — they're ~5 lines each in `memory-adapter.ts`.

## Pub/Sub: `duplicate()` semantics

Real Redis requires a separate connection for pub/sub vs commands. `IoRedisAdapter.duplicate()` calls `Redis.prototype.duplicate()` — a fresh TCP connection. `MemoryRedisAdapter.duplicate()` returns a new adapter that **shares** the underlying EventEmitter + state, so pub/sub fans out the same way real Redis does.

## What it isn't

- **Not a cluster client.** `ioredis` mode supports cluster URLs; memory mode is single-node by definition.
- **Not durable.** Memory mode loses everything on process restart. Persistence is your application's problem (database run table, snapshot to disk, etc.).
- **Not a full Lua interpreter.** See above.
- **Not a substitute for real Redis under load.** The data structures are correct but not optimized for high throughput. Use real Redis in production.

## License

MIT — see [LICENSE](LICENSE).

## Also by askalf

| Project | What it does |
|---------|-------------|
| [pgflex](https://github.com/askalf/pgflex) | Same dual-mode trick for Postgres: real PostgreSQL ↔ PGlite (in-process WASM). |
| [dario](https://github.com/askalf/dario) | Use your Claude Max/Pro subscription as an API. |
| [brio](https://github.com/askalf/brio) | Capability layer for AI workloads — semantic cache, cost tiering, policy. |
| [hands](https://github.com/askalf/hands) | Cross-platform computer-use agent. |
