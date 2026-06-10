// Factory unit tests — exercise createRedisAdapter + createRedisAdapterFromEnv
// without needing a real Redis server. ioredis paths use lazyConnect:true
// so the constructor doesn't try to TCP-connect during the test.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRedisAdapter, createRedisAdapterFromEnv } from '../dist/index.js';

test('createRedisAdapter dispatches to memory backend', async () => {
  const r = createRedisAdapter({ mode: 'memory' });
  assert.equal(r.mode, 'memory');
  await r.set('k', 'v');
  assert.equal(await r.get('k'), 'v');
  await r.quit();
});

test('createRedisAdapter dispatches to ioredis backend (lazy)', async () => {
  // lazyConnect avoids an actual TCP attempt — we just want to verify
  // the dispatch picked ioredis, and that quit() cleans up cleanly.
  const r = createRedisAdapter({
    mode: 'ioredis',
    url: 'redis://localhost:6379',
    opts: { lazyConnect: true },
  });
  assert.equal(r.mode, 'ioredis');
  await r.quit();
});

test('createRedisAdapter memory backend supports the v0.1.0 surface', async () => {
  // Smoke the new commands through the factory-built adapter — full
  // semantics are covered in memory.test.mjs; this guards the wiring.
  const r = createRedisAdapter({ mode: 'memory' });
  try {
    assert.equal(await r.incr('count'), 1);
    assert.equal(await r.sadd('s', 'a', 'b'), 2);
    assert.equal(await r.scard('s'), 2);
    assert.equal(await r.lpush('l', 'x'), 1);
    assert.equal(await r.llen('l'), 1);
    await r.set('k', 'v', 'EX', 30);
    assert.equal(await r.ttl('k'), 30);
    assert.deepEqual(await r.mget('k', 'missing'), ['v', null]);
    await r.zadd('z', 5, 'm');
    assert.equal(await r.zscore('z', 'm'), '5');
    assert.deepEqual(await r.zrangebyscore('z', '-inf', '+inf'), ['m']);
  } finally {
    await r.quit();
  }
});

test('createRedisAdapter ioredis backend exposes the v0.1.0 surface (lazy)', async () => {
  // lazyConnect again — no server needed to verify the methods exist
  // on the adapter (passthrough wiring is one line each; presence is
  // the realistic failure mode).
  const r = createRedisAdapter({
    mode: 'ioredis',
    url: 'redis://localhost:6379',
    opts: { lazyConnect: true },
  });
  try {
    const newMethods = [
      'sadd', 'srem', 'smembers', 'sismember', 'scard',
      'incr', 'decr', 'incrby', 'hincrby',
      'ttl', 'pttl',
      'lpush', 'lpop', 'rpop', 'llen',
      'mget',
      'zscore', 'zrem', 'zrangebyscore',
    ];
    for (const method of newMethods) {
      assert.equal(typeof r[method], 'function', `${method} is missing on IoRedisAdapter`);
    }
  } finally {
    await r.quit();
  }
});

test('ioredis duplicate() returns a working adapter (lazy, no throwaway connection)', async () => {
  const r = createRedisAdapter({
    mode: 'ioredis',
    url: 'redis://localhost:6379',
    opts: { lazyConnect: true },
  });
  // duplicate() of a lazy client copies lazyConnect, so neither the
  // original nor the duplicate ever attempts a TCP connect here.
  const dup = r.duplicate();
  assert.equal(dup.mode, 'ioredis');
  assert.notEqual(dup, r);
  await dup.quit();
  await r.quit();
});

test('createRedisAdapterFromEnv: REDISFLEX_MODE=memory routes to memory', async () => {
  const original = process.env['REDISFLEX_MODE'];
  process.env['REDISFLEX_MODE'] = 'memory';
  try {
    const r = createRedisAdapterFromEnv();
    assert.equal(r.mode, 'memory');
    await r.quit();
  } finally {
    if (original !== undefined) process.env['REDISFLEX_MODE'] = original;
    else delete process.env['REDISFLEX_MODE'];
  }
});

test('createRedisAdapterFromEnv: respects custom env var names', async () => {
  process.env['MYAPP_REDIS_MODE'] = 'memory';
  try {
    const r = createRedisAdapterFromEnv({ modeEnvVar: 'MYAPP_REDIS_MODE' });
    assert.equal(r.mode, 'memory');
    await r.quit();
  } finally {
    delete process.env['MYAPP_REDIS_MODE'];
  }
});

test('createRedisAdapterFromEnv: throws when ioredis URL is required and missing', async () => {
  const originalMode = process.env['REDISFLEX_MODE'];
  const originalUrl = process.env['REDIS_URL'];
  delete process.env['REDISFLEX_MODE'];
  delete process.env['REDIS_URL'];
  try {
    // defaultUrl: null means "no fallback — require explicit URL".
    assert.throws(
      () => createRedisAdapterFromEnv({ defaultUrl: null }),
      /REDIS_URL is required/,
    );
  } finally {
    if (originalMode !== undefined) process.env['REDISFLEX_MODE'] = originalMode;
    if (originalUrl !== undefined) process.env['REDIS_URL'] = originalUrl;
  }
});
