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

test('memory: sets round-trip', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    assert.equal(await r.sadd('tags', 'a', 'b', 'c'), 3);
    assert.equal(await r.sadd('tags', 'b', 'd'), 1, 'sadd counts only newly-added members');
    assert.deepEqual((await r.smembers('tags')).sort(), ['a', 'b', 'c', 'd']);
    assert.equal(await r.sismember('tags', 'a'), 1);
    assert.equal(await r.sismember('tags', 'nope'), 0);
    assert.equal(await r.scard('tags'), 4);

    assert.equal(await r.srem('tags', 'a', 'nope'), 1, 'srem returns count of members actually removed');
    assert.equal(await r.scard('tags'), 3);
    assert.deepEqual(await r.smembers('missing'), []);

    // Removing the last member deletes the key, like real Redis.
    await r.srem('tags', 'b', 'c', 'd');
    assert.equal(await r.exists('tags'), 0, 'emptied set key is deleted');
  } finally {
    await r.quit();
  }
});

test('memory: counters round-trip', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    assert.equal(await r.incr('hits'), 1, 'incr on a missing key counts from 0');
    assert.equal(await r.incr('hits'), 2);
    assert.equal(await r.decr('hits'), 1);
    assert.equal(await r.incrby('hits', 40), 41);
    assert.equal(await r.get('hits'), '41', 'counter is stored as a plain string value');
    assert.equal(await r.decr('fresh'), -1, 'decr on a missing key counts from 0');

    assert.equal(await r.hincrby('stats', 'visits', 7), 7, 'hincrby on a missing field counts from 0');
    assert.equal(await r.hincrby('stats', 'visits', -2), 5);
    assert.equal(await r.hget('stats', 'visits'), '5');
  } finally {
    await r.quit();
  }
});

test('memory: incr/hincrby throw on non-integer values', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.set('word', 'abc');
    await assert.rejects(() => r.incr('word'), /value is not an integer or out of range/);
    await assert.rejects(() => r.incrby('word', 5), /value is not an integer or out of range/);

    await r.hset('h', 'field', 'xyz');
    await assert.rejects(() => r.hincrby('h', 'field', 1), /value is not an integer or out of range/);
  } finally {
    await r.quit();
  }
});

test('memory: ttl/pttl introspection', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    assert.equal(await r.ttl('missing'), -2, 'missing key reports -2');
    assert.equal(await r.pttl('missing'), -2);

    await r.set('forever', 'v');
    assert.equal(await r.ttl('forever'), -1, 'key without expiry reports -1');
    assert.equal(await r.pttl('forever'), -1);

    await r.setex('soon', 5, 'v');
    assert.equal(await r.ttl('soon'), 5, 'ttl rounds the remaining time up to whole seconds');

    await r.set('blink', 'v');
    await r.pexpire('blink', 40);
    const remaining = await r.pttl('blink');
    assert.ok(remaining > 0 && remaining <= 40, `pttl reports remaining ms (got ${remaining})`);

    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(await r.ttl('blink'), -2, 'expired key reports -2');
  } finally {
    await r.quit();
  }
});

test('memory: set with EX/PX options', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.set('ex-key', 'v', 'EX', 1);
    assert.equal(await r.ttl('ex-key'), 1);

    // Option tokens are case-insensitive, matching real SET.
    await r.set('px-key', 'v', 'px', 40);
    assert.equal(await r.get('px-key'), 'v');
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(await r.get('px-key'), null, 'PX expiry enforced');

    // Plain set clears any existing TTL (Redis semantics — no implicit KEEPTTL).
    await r.set('ex-key', 'v2');
    assert.equal(await r.ttl('ex-key'), -1, 'set without expiry options clears the TTL');

    // Unsupported options throw instead of silently diverging from ioredis mode.
    await assert.rejects(() => r.set('k', 'v', 'NX'), /unsupported option/);
    await assert.rejects(() => r.set('k', 'v', 'EX', 'not-a-number'), /requires a positive number/);
  } finally {
    await r.quit();
  }
});

test('memory: expire works on every key type', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.hset('h', 'f', 'v');
    await r.rpush('l', 'a');
    await r.zadd('z', 1, 'm');
    await r.sadd('s', 'x');
    assert.equal(await r.exists('h', 'l', 'z', 's'), 4);

    for (const key of ['h', 'l', 'z', 's']) await r.pexpire(key, 40);
    await new Promise((resolve) => setTimeout(resolve, 120));

    assert.deepEqual(await r.hgetall('h'), {}, 'hash expired');
    assert.deepEqual(await r.lrange('l', 0, -1), [], 'list expired');
    assert.equal(await r.zcard('z'), 0, 'sorted set expired');
    assert.deepEqual(await r.smembers('s'), [], 'set expired');
    assert.equal(await r.exists('h', 'l', 'z', 's'), 0);
    assert.deepEqual(await r.keys('*'), [], 'expired keys do not show up in keys()');
  } finally {
    await r.quit();
  }
});

test('memory: lpush/lpop/rpop/llen round-trip', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    // LPUSH inserts each value at the head in turn → reversed order.
    assert.equal(await r.lpush('q', 'a', 'b', 'c'), 3);
    assert.deepEqual(await r.lrange('q', 0, -1), ['c', 'b', 'a']);
    assert.equal(await r.llen('q'), 3);

    assert.equal(await r.lpop('q'), 'c');
    assert.equal(await r.rpop('q'), 'a');
    assert.equal(await r.llen('q'), 1);

    assert.equal(await r.lpop('missing'), null);
    assert.equal(await r.rpop('missing'), null);
    assert.equal(await r.llen('missing'), 0);

    // Popping the last element deletes the key, like real Redis.
    assert.equal(await r.lpop('q'), 'b');
    assert.equal(await r.exists('q'), 0, 'emptied list key is deleted');
  } finally {
    await r.quit();
  }
});

test('memory: lrange/zrange handle negative indexes', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.rpush('l', 'a', 'b', 'c', 'd', 'e');
    assert.deepEqual(await r.lrange('l', 0, -2), ['a', 'b', 'c', 'd']);
    assert.deepEqual(await r.lrange('l', -3, -1), ['c', 'd', 'e']);
    assert.deepEqual(await r.lrange('l', -100, 100), ['a', 'b', 'c', 'd', 'e'], 'out-of-bounds indexes clamp');
    assert.deepEqual(await r.lrange('l', 2, 1), [], 'inverted range is empty');

    await r.zadd('z', 1, 'w');
    await r.zadd('z', 2, 'x');
    await r.zadd('z', 3, 'y');
    await r.zadd('z', 4, 'zz');
    assert.deepEqual(await r.zrange('z', 0, -2), ['w', 'x', 'y']);
    assert.deepEqual(await r.zrange('z', -2, -1), ['y', 'zz']);
  } finally {
    await r.quit();
  }
});

test('memory: mget returns null slots for missing keys', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.set('a', '1');
    await r.set('b', '2');
    assert.deepEqual(await r.mget('a', 'missing', 'b'), ['1', null, '2']);
  } finally {
    await r.quit();
  }
});

test('memory: zscore/zrem round-trip', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.zadd('z', 50, 'alice');
    await r.zadd('z', 100, 'bob');
    assert.equal(await r.zscore('z', 'alice'), '50', 'score comes back as a string, like ioredis');
    assert.equal(await r.zscore('z', 'ghost'), null);

    assert.equal(await r.zrem('z', 'alice', 'ghost'), 1, 'zrem returns count of members actually removed');
    assert.equal(await r.zcard('z'), 1);

    // Removing the last member deletes the key, like real Redis.
    await r.zrem('z', 'bob');
    assert.equal(await r.exists('z'), 0, 'emptied sorted set key is deleted');
  } finally {
    await r.quit();
  }
});

test('memory: zrangebyscore + zremrangebyscore support inf and exclusive bounds', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.zadd('z', 10, 'a');
    await r.zadd('z', 20, 'b');
    await r.zadd('z', 30, 'c');
    await r.zadd('z', 40, 'd');

    assert.deepEqual(await r.zrangebyscore('z', '-inf', '+inf'), ['a', 'b', 'c', 'd']);
    assert.deepEqual(await r.zrangebyscore('z', 15, 35), ['b', 'c']);
    assert.deepEqual(await r.zrangebyscore('z', '(10', 30), ['b', 'c'], 'exclusive min skips the boundary score');
    assert.deepEqual(await r.zrangebyscore('z', 10, '(40'), ['a', 'b', 'c'], 'exclusive max skips the boundary score');

    // Regression: '(30' used to parse as NaN, making the remove-range
    // filter drop EVERY member. It must only remove scores > 30.
    assert.equal(await r.zremrangebyscore('z', '(30', '+inf'), 1);
    assert.deepEqual(await r.zrange('z', 0, -1), ['a', 'b', 'c']);
  } finally {
    await r.quit();
  }
});

test('memory: keys() scans every store', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    await r.set('str', 'v');
    await r.hset('hash', 'f', 'v');
    await r.rpush('list', 'a');
    await r.zadd('zset', 1, 'm');
    await r.sadd('set', 'x');
    assert.deepEqual((await r.keys('*')).sort(), ['hash', 'list', 'set', 'str', 'zset']);
  } finally {
    await r.quit();
  }
});

test('memory: duplicate() shares all stores and expirations', async () => {
  const r = new MemoryRedisAdapter({ silentEvalFallback: true });
  try {
    const dup = r.duplicate();

    await r.sadd('s', 'x');
    await dup.sadd('s', 'y');
    assert.equal(await r.scard('s'), 2, 'duplicate() shares the sets store');

    await dup.incr('n');
    assert.equal(await r.get('n'), '1', 'duplicate() shares the string store');

    await r.expire('s', 5);
    assert.equal(await dup.ttl('s'), 5, 'duplicate() shares the expirations map');

    await dup.quit();
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
