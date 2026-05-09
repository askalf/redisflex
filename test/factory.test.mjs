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
