// In-memory queue tests — exercise the BullMQ-shaped Queue/Worker
// surface: add, process, completed event, failed event, retry with
// exponential backoff, concurrency, job-state tracking (getJobs /
// getJobCounts), removeOnComplete/removeOnFail, graceful close,
// drained, obliterate.

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

test('queue: jobs move delayed → waiting → active → completed', async () => {
  const q = new InMemoryQueue('states');
  try {
    await q.add('job', { n: 1 }, { delay: 20 });
    assert.deepEqual(await q.getJobCounts(), {
      delayed: 1, waiting: 0, active: 0, completed: 0, failed: 0,
    });

    // Delay elapses with no worker attached → job parks in 'waiting'.
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(await q.getJobCounts('delayed', 'waiting'),
      { delayed: 0, waiting: 1 });
    assert.equal((await q.getJobs(['waiting']))[0].data.n, 1);

    // Attach a worker whose processor blocks on a gate → 'active'.
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    const w = new InMemoryWorker('states', () => gate, { queue: q });
    try {
      await new Promise((resolve) => setTimeout(resolve, 10));
      assert.deepEqual(await q.getJobCounts('waiting', 'active'),
        { waiting: 0, active: 1 });

      // Open the gate → 'completed'.
      release('ok');
      await new Promise((resolve) => setTimeout(resolve, 10));
      assert.deepEqual(await q.getJobCounts(), {
        delayed: 0, waiting: 0, active: 0, completed: 1, failed: 0,
      });
    } finally {
      await w.close();
    }
  } finally {
    await q.close();
  }
});

test('queue: getJobs honors its states argument and ignores unknown names', async () => {
  const q = new InMemoryQueue('filters');
  try {
    await q.add('soon', {}, { delay: 50 });
    await q.add('now', {});

    const delayedOnly = await q.getJobs(['delayed']);
    assert.equal(delayedOnly.length, 1);
    assert.equal(delayedOnly[0].name, 'soon');

    const both = await q.getJobs(['waiting', 'delayed']);
    assert.equal(both.length, 2);

    // Unknown state names are simply ignored, not an error.
    assert.deepEqual(await q.getJobs(['bogus']), []);
    assert.deepEqual(await q.getJobCounts('bogus', 'waiting'), { waiting: 1 });
  } finally {
    await q.close();
  }
});

test('queue: a retrying job counts as delayed until its backoff elapses', async () => {
  const q = new InMemoryQueue('retry-states');
  let invocations = 0;
  const w = new InMemoryWorker(
    'retry-states',
    async () => {
      invocations++;
      if (invocations === 1) throw new Error('first attempt fails');
      return 'ok';
    },
    { queue: q },
  );
  const waitingEvents = [];
  q.on('waiting', (job) => waitingEvents.push(job.id));

  try {
    await q.add('flaky', {}, { attempts: 2, backoff: { delay: 40 } });

    // First attempt fails fast; the 40ms backoff timer is now pending,
    // so the job shows as 'delayed' — not waiting, not failed.
    await new Promise((resolve) => setTimeout(resolve, 15));
    assert.equal(invocations, 1);
    assert.deepEqual(await q.getJobCounts('delayed', 'waiting', 'failed'),
      { delayed: 1, waiting: 0, failed: 0 });

    // Backoff elapses → re-enqueued (second 'waiting' event) → retried
    // → completed.
    await new Promise((resolve) => setTimeout(resolve, 80));
    assert.equal(invocations, 2);
    assert.equal(waitingEvents.length, 2, 'initial enqueue + retry re-enqueue');
    assert.deepEqual(await q.getJobCounts('delayed', 'completed'),
      { delayed: 0, completed: 1 });
  } finally {
    await w.close();
    await q.close();
  }
});

test('queue: removeOnComplete / removeOnFail skip retention', async () => {
  const q = new InMemoryQueue('no-history');
  const w = new InMemoryWorker(
    'no-history',
    async (job) => {
      if (job.name === 'boom') throw new Error('nope');
      return 'done';
    },
    { queue: q },
  );
  const failures = [];
  w.on('failed', (job) => failures.push(job.name));

  try {
    await q.add('kept', {});
    await q.add('dropped', {}, { removeOnComplete: true });
    await q.add('boom', {}, { removeOnFail: true });
    await new Promise((resolve) => setTimeout(resolve, 30));

    const completed = await q.getJobs(['completed']);
    assert.equal(completed.length, 1, 'only the job without removeOnComplete is retained');
    assert.equal(completed[0].name, 'kept');

    // The failure happened (event fired) but left no failed record.
    assert.deepEqual(failures, ['boom']);
    assert.deepEqual(await q.getJobCounts('failed'), { failed: 0 });
  } finally {
    await w.close();
    await q.close();
  }
});

test('queue: close() waits for the in-flight job to finish', async () => {
  const q = new InMemoryQueue('graceful');
  let finished = false;
  const w = new InMemoryWorker(
    'graceful',
    async () => {
      await new Promise((resolve) => setTimeout(resolve, 50));
      finished = true;
    },
    { queue: q },
  );

  try {
    await q.add('slow', {});
    // Give the worker a beat to pick the job up before closing.
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(finished, false, 'job is still in flight when close() is called');
    await w.close();
    assert.equal(finished, true, 'close() resolved only after the processor finished');
  } finally {
    await q.close();
  }
});

test('queue: close(true) skips waiting for in-flight jobs', async () => {
  const q = new InMemoryQueue('forced');
  let finished = false;
  const w = new InMemoryWorker(
    'forced',
    async () => {
      await new Promise((resolve) => setTimeout(resolve, 60));
      finished = true;
    },
    { queue: q },
  );

  try {
    await q.add('slow', {});
    await new Promise((resolve) => setTimeout(resolve, 10));
    await w.close(true);
    assert.equal(finished, false, 'close(true) resolved without waiting for the processor');
  } finally {
    // Let the orphaned processor finish so its timer doesn't outlive
    // the test.
    await new Promise((resolve) => setTimeout(resolve, 70));
    await q.close();
  }
});

test('queue: drained fires exactly once after a burst completes', async () => {
  const q = new InMemoryQueue('burst');
  let drains = 0;
  const w = new InMemoryWorker(
    'burst',
    async () => {
      await new Promise((resolve) => setTimeout(resolve, 5));
    },
    { queue: q, concurrency: 2 },
  );
  w.on('drained', () => drains++);

  try {
    for (let i = 0; i < 6; i++) await q.add('job', { i });
    // Burst takes ~15ms (6 jobs × 5ms / concurrency 2); the long tail
    // of idle time after would catch any spurious extra emits.
    await new Promise((resolve) => setTimeout(resolve, 150));
    assert.equal(drains, 1, 'one drained event for the whole burst, not one per job');
  } finally {
    await w.close();
    await q.close();
  }
});

test('queue: obliterate() cancels pending delayed jobs', async () => {
  const q = new InMemoryQueue('wipe');
  let ran = false;
  const w = new InMemoryWorker('wipe', async () => { ran = true; }, { queue: q });

  try {
    await q.add('later', {}, { delay: 30 });
    assert.deepEqual(await q.getJobCounts('delayed'), { delayed: 1 });

    await q.obliterate();
    assert.deepEqual(await q.getJobCounts(), {
      delayed: 0, waiting: 0, active: 0, completed: 0, failed: 0,
    });

    // Wait past the original delay — the canceled job must never run.
    await new Promise((resolve) => setTimeout(resolve, 60));
    assert.equal(ran, false, 'obliterated delayed job never reached the worker');
  } finally {
    await w.close();
    await q.close();
  }
});

test('queue: emits completed/failed events on the queue itself', async () => {
  const q = new InMemoryQueue('observed');
  const w = new InMemoryWorker(
    'observed',
    async (job) => {
      if (job.name === 'bad') throw new Error('nope');
      return 'fine';
    },
    { queue: q },
  );
  const completed = [];
  const failed = [];
  q.on('completed', (job, result) => completed.push({ name: job.name, result }));
  q.on('failed', (job, err) => failed.push({ name: job.name, message: err.message }));

  try {
    await q.add('good', {});
    await q.add('bad', {});
    await new Promise((resolve) => setTimeout(resolve, 40));

    assert.deepEqual(completed, [{ name: 'good', result: 'fine' }]);
    assert.deepEqual(failed, [{ name: 'bad', message: 'nope' }]);
  } finally {
    await w.close();
    await q.close();
  }
});
