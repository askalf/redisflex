# @askalf/redisflex

> One Redis API. Two modes. Same call sites.

Switch between **real Redis** (`ioredis`) and **in-process Redis** (Map + EventEmitter) with one line of config. Production runs on real Redis; dev / standalone / "no-Docker mode" runs in-process. Same get/set, counters, hashes, lists, sets, sorted sets, pub/sub — drop the Redis server when you don't need it.

Also ships a **BullMQ-shaped in-memory queue** so you can drop the Redis dep entirely for queueing too.

```bash
npm install @askalf/redisflex
```

[![CI](https://img.shields.io/github/actions/workflow/status/askalf/redisflex/ci.yml?style=flat-square&label=CI&labelColor=020612)](https://github.com/askalf/redisflex/actions)
[![npm](https://img.shields.io/npm/v/@askalf/redisflex?style=flat-square&color=00ff88&label=npm&labelColor=020612)](https://www.npmjs.com/package/@askalf/redisflex)
[![License](https://img.shields.io/badge/MIT-00ff88?style=flat-square&label=license&labelColor=020612)](LICENSE)

## Why

Most apps use Redis for three things: cache, pub/sub, and queueing. Most dev environments don't want to spin up a Redis server for any of them. Most CI environments REALLY don't want it.

`redisflex` swaps a real Redis connection for an in-process implementation that speaks the same surface — `get/set`, counters, hashes, lists, sets, sorted sets, pub/sub, expiry, and a sliding-window-rate-limit-shaped Lua eval. Plus a tiny BullMQ-API-compatible queue if you use BullMQ for jobs.

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
worker.on('drained', () => console.log('all caught up')); // once per drain

await queue.add('welcome', { to: 'alice@example.com' });
await queue.add('reminder', { to: 'bob@example.com' }, {
  delay: 60_000,
  attempts: 3,
  backoff: { delay: 5000 }, // exponential: 5s, 10s, 20s
});

await queue.getJobCounts();
// { delayed: 1, waiting: 0, active: 1, completed: 0, failed: 0 }

await worker.close(); // waits for in-flight jobs; close(true) skips the wait
```

The shape matches BullMQ's `Queue` / `Worker` so you can swap to real BullMQ later by changing imports. Jobs move through the BullMQ states (`delayed → waiting → active → completed`/`failed`; a retrying job counts as `delayed` while its backoff timer is pending) — `getJobs(states)` and `getJobCounts(...states)` report them, and the queue re-emits `completed`/`failed` so you don't need a separate `QueueEvents` object. Completed/failed history is retained for introspection, capped at 1000 each (oldest dropped); set `removeOnComplete`/`removeOnFail` per job to skip retention.

## What's covered

| Family | Operations |
|---|---|
| Key/Value | `get`, `set` (incl. trailing `EX <seconds>` / `PX <ms>`), `setex`, `mget`, `del`, `keys`, `exists` |
| Counters | `incr`, `decr`, `incrby`, `hincrby` |
| Hashes | `hset`, `hget`, `hgetall`, `hdel` |
| Lists | `lpush`, `rpush`, `lpop`, `rpop`, `llen`, `lrange` |
| Sets | `sadd`, `srem`, `smembers`, `sismember`, `scard` |
| TTL | `expire`, `pexpire`, `ttl`, `pttl` |
| Sorted Sets | `zadd`, `zcard`, `zscore`, `zrange`, `zrangebyscore`, `zrem`, `zremrangebyscore` (incl. `-inf`/`+inf` and exclusive `(n` bounds) |
| Pub/Sub | `publish`, `subscribe`, `unsubscribe`, `psubscribe`, `on('message'/'pmessage')` |
| Scripting | `eval` (sliding-window-rate-limit shape recognized in memory mode) |
| Lifecycle | `duplicate`, `quit` |

That's enough surface for cache, pub/sub, BullMQ-style queues, sliding-window rate limits, and most idiomatic Redis usage. Streams, geo, cluster, MULTI/EXEC, bitmap ops aren't covered. Open an issue if you need one — most are mechanical to add.

Note on `set` options: memory mode handles `EX`/`PX` and **throws** on anything else (`NX`, `XX`, `KEEPTTL`, ...) instead of silently ignoring it, so the two modes can't quietly diverge. ioredis mode passes all options through to the server.

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
| [arnie](https://github.com/askalf/arnie) | Portable IT troubleshooting companion. Networking, AD, Windows Update, package managers, log triage, hardware checks. |
| [browser-bridge](https://github.com/askalf/browser-bridge) | Stealth headless Chromium in a container. CDP on 9222 — Playwright/Puppeteer/MCP-compatible. |
| [dario](https://github.com/askalf/dario) | Local LLM router. Use your Claude Max/Pro subscription as an API. |
| [deepdive](https://github.com/askalf/deepdive) | Local research agent. Plan → search → fetch → extract → synthesize. Cited answers. |
| [git-providers](https://github.com/askalf/git-providers) | Unified GitHub + GitLab + Bitbucket Cloud REST clients behind one GitProvider interface. Plus a 44-entry api-key-provider taxonomy. |
| [hands](https://github.com/askalf/hands) | Cross-platform computer-use agent. Mouse, keyboard, screen. |
| [pgflex](https://github.com/askalf/pgflex) | One Postgres API. Two modes (real PG ↔ PGlite WASM). |


---

## Built by Sprayberry Labs

This is one of the open-source building blocks from **[Sprayberry Labs](https://sprayberrylabs.com)** — an independent studio (Atlanta, GA) that ships bespoke software and **fixed-price code & security audits**, delivered with the AI workforce these tools are part of.

Part of the [askalf](https://askalf.org) ecosystem — a self-hosted AI workforce platform, now in early access.

**Got a codebase that needs an expert read?** → **[Scan a repo — free mini-audit](https://sprayberrylabs.com)**, or see the **$1,500 fixed-price Audit** and build Sprints. · [sprayberrylabs.com](https://sprayberrylabs.com) · hello@sprayberrylabs.com

---
Part of **[Own Your Stack](https://github.com/askalf)** — own your AI infrastructure instead of renting it. Built by Thomas Sprayberry.
