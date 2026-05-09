// MemoryRedisAdapter integration tests — exercise the full IRedisAdapter
// surface against the in-process backend. Always runs (no Redis server
// needed); these tests are the primary coverage for redisflex.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { MemoryRedisAdapter } from '../dist/index.js';

test('memory: get/set/del round-trip', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.set('a', '1');
    await r.set('b', '2');
    assert.equal(await r.get('a'), '1');
    assert.equal(await r.get('b'), '2');
    assert.equal(await r.get('missing'), null);

    const deleted = await r.del('a', 'missing');
    assert.equal(deleted, 1, 'del returns count of keys actually removed');
    assert.equal(await r.get('a'), null);
  } finally {
    await r.quit();
  }
});

test('memory: setex enforces TTL on get', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.setex('temp', 1, 'shortlived'); // 1 second TTL
    assert.equal(await r.get('temp'), 'shortlived');

    // Wait past expiry. 1100ms gives a 100ms cushion.
    await new Promise((resolve) => setTimeout(resolve, 1100));
    assert.equal(await r.get('temp'), null, 'expired key returns null on get');
  } finally {
    await r.quit();
  }
});

test('memory: keys() globs with * and ?', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.set('user:1', 'alice');
    await r.set('user:2', 'bob');
    await r.set('post:1', 'hello');
    const userKeys = (await r.keys('user:*')).sort();
    assert.deepEqual(userKeys, ['user:1', 'user:2']);
    const oneCharKeys = (await r.keys('user:?')).sort();
    assert.deepEqual(oneCharKeys, ['user:1', 'user:2']);
  } finally {
    await r.quit();
  }
});

test('memory: hashes round-trip', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.hset('user:1', 'name', 'alice');
    await r.hset('user:1', 'role', 'admin');
    assert.equal(await r.hget('user:1', 'name'), 'alice');
    assert.equal(await r.hget('user:1', 'missing'), null);
    assert.deepEqual(await r.hgetall('user:1'), { name: 'alice', role: 'admin' });

    const deleted = await r.hdel('user:1', 'role', 'nope');
    assert.equal(deleted, 1, 'hdel returns count of fields actually removed');
    assert.deepEqual(await r.hgetall('user:1'), { name: 'alice' });
  } finally {
    await r.quit();
  }
});

test('memory: lists round-trip', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.rpush('q', 'a', 'b', 'c');
    assert.deepEqual(await r.lrange('q', 0, -1), ['a', 'b', 'c']);
    assert.deepEqual(await r.lrange('q', 0, 1), ['a', 'b']);
    assert.deepEqual(await r.lrange('missing', 0, -1), []);
  } finally {
    await r.quit();
  }
});

test('memory: sorted set round-trip', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.zadd('leaderboard', 50, 'alice');
    await r.zadd('leaderboard', 100, 'bob');
    await r.zadd('leaderboard', 75, 'carol');
    assert.equal(await r.zcard('leaderboard'), 3);
    assert.deepEqual(await r.zrange('leaderboard', 0, -1), ['alice', 'carol', 'bob']);

    // ZADD on existing member overwrites the score.
    await r.zadd('leaderboard', 200, 'alice');
    assert.equal(await r.zcard('leaderboard'), 3, 'ZADD on existing member updates, does not duplicate');
    assert.deepEqual(await r.zrange('leaderboard', 0, -1), ['carol', 'bob', 'alice']);

    const removed = await r.zremrangebyscore('leaderboard', '-inf', 75);
    assert.equal(removed, 1, 'zremrangebyscore removes carol (score 75)');
    assert.deepEqual(await r.zrange('leaderboard', 0, -1), ['bob', 'alice']);
  } finally {
    await r.quit();
  }
});

test('memory: pub/sub fans out to subscribers', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    const received = [];
    r.on('message', (channel, message) => {
      received.push({ channel, message });
    });
    await r.subscribe('updates');

    await r.publish('updates', 'hello');
    await r.publish('other', 'ignored');
    await r.publish('updates', 'world');

    // Pub/sub is synchronous in memory mode — emit fires before the
    // publish() promise resolves — so no need to wait.
    assert.deepEqual(received, [
      { channel: 'updates', message: 'hello' },
      { channel: 'updates', message: 'world' },
    ]);
  } finally {
    await r.quit();
  }
});

test('memory: psubscribe matches glob patterns', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    const received = [];
    r.on('pmessage', (pattern, channel, message) => {
      received.push({ pattern, channel, message });
    });
    await r.psubscribe('user.*');

    await r.publish('user.created', '{"id":1}');
    await r.publish('user.updated', '{"id":1}');
    await r.publish('post.created', 'no-match');

    assert.deepEqual(received, [
      { pattern: 'user.*', channel: 'user.created', message: '{"id":1}' },
      { pattern: 'user.*', channel: 'user.updated', message: '{"id":1}' },
    ]);
  } finally {
    await r.quit();
  }
});

test('memory: duplicate() shares pub/sub state', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    const sub = r.duplicate();
    const received = [];
    sub.on('message', (_channel, message) => received.push(message));
    await sub.subscribe('chan');

    // Publish from the original; the duplicate must receive it.
    await r.publish('chan', 'hi');
    assert.deepEqual(received, ['hi'],
      'duplicate() shares the EventEmitter so pub/sub fans out across both');

    await sub.quit();
  } finally {
    await r.quit();
  }
});

test('memory: eval() recognizes the sliding-window-rate-limit shape', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    // Fake the canonical ZADD-based rate-limiter Lua. Args layout:
    // [key, now, window-ms, member?]. The eval() heuristic returns
    // the post-add zcard, which is the request count in the window.
    const now = Date.now();
    const window = 1000; // 1 second window

    const c1 = await r.eval('ZADD ...', 1, 'rl:user:1', now, window, 'req-1');
    const c2 = await r.eval('ZADD ...', 1, 'rl:user:1', now + 100, window, 'req-2');
    const c3 = await r.eval('ZADD ...', 1, 'rl:user:1', now + 200, window, 'req-3');
    assert.equal(c1, 1);
    assert.equal(c2, 2);
    assert.equal(c3, 3);

    // Skip past the window — old entries get evicted on the next eval.
    const later = now + 2000;
    const c4 = await r.eval('ZADD ...', 1, 'rl:user:1', later, window, 'req-4');
    assert.equal(c4, 1, 'rate-limit window slid: old entries dropped');
  } finally {
    await r.quit();
  }
});
