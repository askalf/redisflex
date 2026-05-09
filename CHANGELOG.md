# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!--
Release convention: land changes under `## [Unreleased]`. At release
time, rename that heading to `## [X.Y.Z] - YYYY-MM-DD` and add a fresh
`## [Unreleased]` above it.
-->

## [Unreleased]

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
