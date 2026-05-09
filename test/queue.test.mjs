// In-memory queue tests — exercise the BullMQ-shaped Queue/Worker
// surface: add, process, completed event, failed event, retry with
// exponential backoff, concurrency.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { InMemoryQueue, InMemoryWorker } from '../dist/index.js';

test('queue: processor runs jobs added to the queue', async () => {
  const q = new InMemoryQueue('emails');
  const processed = [];
  const w = new InMemoryWorker(
    'emails',
    async (job) => {
      processed.push(job.data);
      return { ok: true };
    },
    { queue: q },
  );

  try {
    await q.add('send', { to: 'alice@example.com' });
    await q.add('send', { to: 'bob@example.com' });

    // Wait for the worker to drain. tryProcess is scheduled via setImmediate
    // so we need to yield enough turns of the event loop for both jobs.
    await new Promise((resolve) => setTimeout(resolve, 50));

    assert.deepEqual(processed, [
      { to: 'alice@example.com' },
      { to: 'bob@example.com' },
    ]);
  } finally {
    await w.close();
    await q.close();
  }
});

test('queue: completed event fires with job + result', async () => {
  const q = new InMemoryQueue('greet');
  const events = [];
  const w = new InMemoryWorker(
    'greet',
    async (job) => `hello, ${job.data.name}`,
    { queue: q },
  );
  w.on('completed', (job, result) => {
    events.push({ id: job.id, result });
  });

  try {
    await q.add('greet', { name: 'world' });
    await new Promise((resolve) => setTimeout(resolve, 30));

    assert.equal(events.length, 1);
    assert.equal(events[0].result, 'hello, world');
  } finally {
    await w.close();
    await q.close();
  }
});

test('queue: retries with exponential backoff up to attempts', async () => {
  const q = new InMemoryQueue('flaky');
  let invocations = 0;
  const w = new InMemoryWorker(
    'flaky',
    async () => {
      invocations++;
      if (invocations < 3) throw new Error(`fail ${invocations}`);
      return 'finally';
    },
    { queue: q },
  );
  const completedResults = [];
  const failedErrors = [];
  w.on('completed', (_job, result) => completedResults.push(result));
  w.on('failed', (_job, err) => failedErrors.push(err.message));

  try {
    await q.add('flaky', { x: 1 }, {
      attempts: 3,
      // Tiny base delay so the test doesn't take forever.
      backoff: { type: 'exponential', delay: 5 },
    });

    // 5ms + 10ms backoffs + processing — wait long enough for both retries.
    await new Promise((resolve) => setTimeout(resolve, 200));

    assert.equal(invocations, 3, 'processor invoked 3 times (initial + 2 retries)');
    assert.deepEqual(completedResults, ['finally'],
      'job completed on the third attempt');
    assert.deepEqual(failedErrors, [],
      'no failed event because the third attempt succeeded');
  } finally {
    await w.close();
    await q.close();
  }
});

test('queue: emits failed event when retries are exhausted', async () => {
  const q = new InMemoryQueue('always-bad');
  const w = new InMemoryWorker(
    'always-bad',
    async () => { throw new Error('nope'); },
    { queue: q },
  );
  const failures = [];
  w.on('failed', (job, err) => {
    failures.push({ id: job.id, message: err.message });
  });

  try {
    await q.add('boom', {}, { attempts: 2, backoff: { delay: 5 } });
    await new Promise((resolve) => setTimeout(resolve, 100));

    assert.equal(failures.length, 1, 'one failed event after both attempts exhausted');
    assert.equal(failures[0].message, 'nope');
  } finally {
    await w.close();
    await q.close();
  }
});

test('queue: respects concurrency limit', async () => {
  const q = new InMemoryQueue('parallel');
  let inflight = 0;
  let maxInflight = 0;
  const w = new InMemoryWorker(
    'parallel',
    async () => {
      inflight++;
      if (inflight > maxInflight) maxInflight = inflight;
      await new Promise((resolve) => setTimeout(resolve, 30));
      inflight--;
    },
    { queue: q, concurrency: 2 },
  );

  try {
    // Five jobs, max 2 in flight at once.
    for (let i = 0; i < 5; i++) await q.add('p', { i });
    await new Promise((resolve) => setTimeout(resolve, 200));

    // Concurrency 2 doesn't mean exactly 2 — early scheduling can make
    // it 1 if the first job finishes before the next is dispatched —
    // but it must NEVER exceed 2.
    assert.ok(maxInflight <= 2,
      `peak inflight should respect concurrency limit, got ${maxInflight}`);
  } finally {
    await w.close();
    await q.close();
  }
});
