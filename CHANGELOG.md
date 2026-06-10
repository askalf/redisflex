# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!--
Release convention: land changes under `## [Unreleased]`. At release
time, rename that heading to `## [X.Y.Z] - YYYY-MM-DD` and add a fresh
`## [Unreleased]` above it.
-->

## [Unreleased]

The in-memory queue grows from "fan-out only" to honest job-state
tracking — jobs are visible in every BullMQ state, finished jobs are
retained for introspection, and worker shutdown is graceful.

### Added

- In-memory queue: job-state tracking through the BullMQ state machine
  (`delayed → waiting → active → completed`/`failed`). `getJobs(states)`
  now honors its argument (it previously returned the waiting set
  regardless; unknown state names are ignored), and the new
  `getJobCounts(...states)` returns BullMQ-shaped per-state counts —
  all five states when called with no arguments. A retrying job
  (failed attempt, backoff timer pending) counts as `delayed` until
  re-enqueued. `JobState` exported as a type.
- In-memory queue: `removeOnComplete` / `removeOnFail` job opts are now
  honored (previously accepted but unused) — when true the job is not
  retained after finishing. Default is to retain, capped at 1000
  completed + 1000 failed records (oldest dropped first) so a
  long-lived dev process doesn't leak memory.
- `InMemoryWorker` `'drained'` event — fires when the worker finishes a
  job and finds the queue empty; once per drain, not on every idle
  poll.
- Queue-level `'completed'` (job, result) and `'failed'` (job, err)
  events on `InMemoryQueue`, mirroring the worker's — a simplification
  of BullMQ's separate `QueueEvents` class.

### Changed

- `InMemoryWorker.close()` now waits for in-flight jobs to finish
  before resolving (BullMQ semantics); `close(true)` skips the wait
  (running processors aren't aborted, just not awaited). No new jobs
  are started once closing.
- `InMemoryQueue.obliterate()` (and `close()`) now clear every state
  collection and cancel pending delay/backoff timers — an obliterated
  delayed job never runs.

## [0.1.0] - 2026-06-10

The surface grows from "cache + pub/sub + rate limit" to most of the
day-to-day Redis command set, and memory mode's TTL handling becomes
correct for every key type.

### Added

- Sets: `sadd` (returns count newly added), `srem`, `smembers`,
  `sismember`, `scard`.
- Counters: `incr`, `decr`, `incrby`, `hincrby`. Memory mode counts
  missing keys/fields from 0 and throws Redis's
  "value is not an integer or out of range" on non-integer values.
- TTL introspection: `ttl` (-2 missing / -1 no expiry / seconds rounded
  up) and `pttl` (same in ms).
- Lists: `lpush`, `lpop`, `rpop`, `llen` (joining the existing `rpush`
  + `lrange`). Popping the last element deletes the key, like real
  Redis.
- `mget` — one `null` slot per missing key.
- Sorted sets: `zscore` (score as string, matching ioredis), `zrem`,
  `zrangebyscore` (with `-inf`/`+inf` and exclusive `(n` bounds).
- `set` now accepts trailing `EX <seconds>` / `PX <ms>` options
  (case-insensitive). Memory mode throws on any other option token
  (`NX`, `XX`, `KEEPTTL`, ...) instead of silently ignoring it; ioredis
  mode passes everything through to the server.

### Fixed

- **`expire`/`pexpire` now work on every key type in memory mode.**
  TTLs moved from a per-string-value field to a single expirations map
  covering strings, hashes, lists, sets, and sorted sets; previously
  expire() silently no-oped on anything that wasn't a plain string key.
  Reads lazily evict, and the periodic sweep covers all stores.
- `keys(pattern)` in memory mode now scans every store (it only saw
  string keys before); `del`/`exists` cover the new sets store too.
- `lrange`/`zrange` in memory mode handle negative indexes properly
  (e.g. `lrange(0, -2)`); previously only `stop === -1` was
  special-cased.
- `zremrangebyscore` no longer turns exclusive bounds like `'(5'` into
  NaN (which removed *every* member); bound parsing is shared with the
  new `zrangebyscore` and rejects malformed bounds loudly.
- Plain `set(key, value)` in memory mode clears an existing TTL (Redis
  semantics — there's no implicit KEEPTTL).

### Changed

- `IoRedisAdapter.duplicate()` no longer constructs a throwaway
  `Redis` instance just to overwrite its client; the duplicated client
  is handed to an internal constructor slot instead.

## [0.0.2] - 2026-05-09 — npm-orphan

(v0.0.1 was tagged + GitHub-released but never reached npm — the freshly-created repo didn't have an `NPM_TOKEN` secret yet, so the auto-release workflow's `npm publish` step exited `ENEEDAUTH`. After provisioning the token, the version-changed gate considered v0.0.1 already-shipped from its perspective, so this re-tag bumps to v0.0.2 with identical content. Same pattern as pgflex 0.0.1 → 0.0.2 a few minutes ago.)

Identical content to the v0.0.1 entry below.

## [0.0.1] - 2026-05-09

Initial release. Extracted from a private monorepo where the same
shape ran in production and in standalone / dev. Same call sites,
two backends, plus a BullMQ-shaped in-memory job queue.

### Added

- `createRedisAdapter({ mode: 'ioredis' | 'memory', ... })` — explicit factory.
- `createRedisAdapterFromEnv()` — env-driven factory with configurable
  variable names (defaults: `REDISFLEX_MODE`, `REDIS_URL`).
- `IoRedisAdapter` — wraps `ioredis`; `lazyConnect` and
  `maxRetriesPerRequest` exposed as constructor options. `getClient()`
  escape hatch for raw access.
- `MemoryRedisAdapter` — full IRedisAdapter implementation backed by
  Map / EventEmitter. Sliding-window-rate-limit-shaped Lua eval
  recognized natively. Sweep interval for TTL is unrefed so it doesn't
  keep the event loop alive.
- `InMemoryQueue` + `InMemoryWorker` — BullMQ-shaped queue/worker pair.
  Supports `add` with `delay`, `attempts`, exponential `backoff`,
  concurrency, `completed` / `failed` events.
- Unified `IRedisAdapter` interface: K/V, hashes, lists, TTL,
  sorted sets, pub/sub (incl. pattern subscriptions), eval, duplicate,
  quit.
- TypeScript build → `dist/`. `engines: node >=20`.
- CI matrix on Node 20 + 22 across Ubuntu and Windows. CodeQL.
  actionlint. Auto-release on `package.json` version bump.
