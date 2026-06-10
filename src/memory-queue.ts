/**
 * In-memory job queue with a BullMQ-shaped surface.
 *
 * Implements the subset of the BullMQ API that real apps actually use:
 *   - Queue#add(name, data, opts)
 *   - Queue#getJobs(states) / Queue#getJobCounts(...states) over the
 *     full job state machine ('delayed', 'waiting', 'active',
 *     'completed', 'failed')
 *   - removeOnComplete / removeOnFail job opts
 *   - Worker(name, processor, { concurrency })
 *   - Worker#close(force?) — waits for in-flight jobs unless forced
 *   - Retry with exponential backoff (`opts.attempts`, `opts.backoff`)
 *   - Events: 'completed' (job, result) and 'failed' (job, err) on both
 *     the worker and the queue; 'waiting' (job) on the queue; 'drained'
 *     on the worker
 *
 * Persistence is not implemented — jobs live in-process. Pair this
 * with a database-backed run table (for crash recovery) if you need
 * durability; the platform monorepo this was extracted from did
 * exactly that (orphan recovery on startup against the workflow run
 * table, queue used only as a fan-out).
 *
 * Use this when you don't want to run Redis just for BullMQ. For
 * production-grade durable queues, use BullMQ proper against a real
 * Redis.
 */

import { EventEmitter } from 'node:events';

export type JobData = Record<string, unknown>;

/** The job states InMemoryQueue tracks. Mirrors BullMQ's core state
 *  machine (minus 'prioritized'/'waiting-children', not implemented). */
export type JobState = 'delayed' | 'waiting' | 'active' | 'completed' | 'failed';

const ALL_STATES: readonly JobState[] = ['delayed', 'waiting', 'active', 'completed', 'failed'];

/** Cap on retained completed/failed job records (each). Oldest are
 *  dropped first so a long-lived dev process doesn't leak memory while
 *  still giving getJobs()/getJobCounts() useful history. */
const MAX_RETAINED_JOBS = 1000;

export interface JobOpts {
  /** Delay (ms) before the job becomes available to workers. */
  delay?: number;
  /** Max retries (including the first attempt). Default 1 (no retries). */
  attempts?: number;
  /** Backoff config. Only `delay` is used; the curve is exponential
   *  (delay * 2^(attempt-1)). The `type` field is accepted for BullMQ
   *  shape compatibility but ignored — we always do exponential. */
  backoff?: { type?: string; delay: number };
  /** When true, don't retain the job in the 'completed' set after it
   *  succeeds (it just disappears). Default: retain, capped. */
  removeOnComplete?: boolean;
  /** When true, don't retain the job in the 'failed' set after its
   *  retries are exhausted. Default: retain, capped. */
  removeOnFail?: boolean;
}

export interface Job<T = JobData> {
  id: string;
  name: string;
  data: T;
  opts: JobOpts;
  /** How many times the processor has been invoked for this job
   *  (1 after the first attempt, 2 after the first retry, ...). */
  attemptsMade: number;
  /** Timestamp when the job was first enqueued. */
  timestamp: number;
}

export type ProcessorFn<T = JobData> = (job: Job<T>) => Promise<unknown>;

/** A job whose delay (or retry backoff) hasn't elapsed yet, plus the
 *  pending timer that will move it to 'waiting'. */
interface DelayedEntry<T extends JobData> {
  job: Job<T>;
  timer: ReturnType<typeof setTimeout>;
}

/**
 * BullMQ-shaped queue with honest job-state tracking:
 *
 *   delayed → waiting → active → completed | failed
 *
 * A retrying job (failed attempt, backoff timer pending) counts as
 * 'delayed' until its timer re-enqueues it as 'waiting'.
 *
 * Completed/failed jobs are retained for getJobs()/getJobCounts()
 * introspection, capped at 1000 each (oldest dropped first) so a
 * long-lived dev process doesn't leak memory. Set `removeOnComplete` /
 * `removeOnFail` on a job to skip retention entirely.
 *
 * The queue also emits 'completed' (job, result) and 'failed'
 * (job, err) — mirroring the worker's events — as a simplification of
 * BullMQ's separate QueueEvents class, so you don't need a second
 * object just to observe outcomes.
 */
export class InMemoryQueue<T extends JobData = JobData> extends EventEmitter {
  readonly name: string;
  private readonly waiting: Job<T>[] = [];
  private readonly delayed = new Map<string, DelayedEntry<T>>();
  private readonly active = new Map<string, Job<T>>();
  private readonly completed = new Map<string, Job<T>>();
  private readonly failed = new Map<string, Job<T>>();
  private idCounter = 0;

  constructor(name: string) {
    super();
    this.name = name;
  }

  async add(name: string, data: T, opts?: JobOpts): Promise<Job<T>> {
    const job: Job<T> = {
      id: String(++this.idCounter),
      name,
      data,
      opts: opts ?? {},
      attemptsMade: 0,
      timestamp: Date.now(),
    };

    if (opts?.delay) {
      // Delayed jobs aren't visible to workers until their delay elapses.
      // Using setTimeout here is fine — Node clamps to ~1ms minimum and
      // the queue is in-process so wall-clock drift is bounded. The
      // entry is held in the delayed map so getJobs(['delayed']) sees
      // it and obliterate() can cancel the timer.
      const timer = setTimeout(() => {
        this.delayed.delete(job.id);
        this.waiting.push(job);
        this.emit('waiting', job);
      }, opts.delay);
      timer.unref?.();
      this.delayed.set(job.id, { job, timer });
    } else {
      this.waiting.push(job);
      this.emit('waiting', job);
    }

    return job;
  }

  // ── Worker handshake (internal, not part of the public BullMQ API) ──

  /** Pop one job off the front of the waiting list and mark it
   *  'active'. Returns undefined if the queue is empty. Used by
   *  InMemoryWorker; not part of the public BullMQ API. */
  _takeNext(): Job<T> | undefined {
    const job = this.waiting.shift();
    if (job) this.active.set(job.id, job);
    return job;
  }

  /** Re-enqueue a job (the landing step of _retryLater's backoff timer).
   *  Pushes to the front of the waiting list so retries are prioritized
   *  over fresh jobs — typical "drain backed-up retries first" semantics. */
  _reenqueue(job: Job<T>): void {
    this.waiting.unshift(job);
    this.emit('waiting', job);
  }

  /** Park a failed-but-retryable job as 'delayed' for `waitMs`, then
   *  re-enqueue it. Used by InMemoryWorker for retry-after-backoff;
   *  not part of the public BullMQ API. The pending timer is canceled
   *  by obliterate()/close(). */
  _retryLater(job: Job<T>, waitMs: number): void {
    this.active.delete(job.id);
    const timer = setTimeout(() => {
      this.delayed.delete(job.id);
      this._reenqueue(job);
    }, waitMs);
    timer.unref?.();
    this.delayed.set(job.id, { job, timer });
  }

  /** Move a job from 'active' to 'completed' (subject to
   *  removeOnComplete + the retention cap) and emit the queue-level
   *  'completed' event. Used by InMemoryWorker; not part of the public
   *  BullMQ API. */
  _markCompleted(job: Job<T>, result: unknown): void {
    this.active.delete(job.id);
    if (!job.opts.removeOnComplete) this.retain(this.completed, job);
    this.emit('completed', job, result);
  }

  /** Move a job from 'active' to 'failed' (subject to removeOnFail +
   *  the retention cap) and emit the queue-level 'failed' event. Used
   *  by InMemoryWorker; not part of the public BullMQ API. */
  _markFailed(job: Job<T>, err: unknown): void {
    this.active.delete(job.id);
    if (!job.opts.removeOnFail) this.retain(this.failed, job);
    this.emit('failed', job, err);
  }

  /** True when nothing is waiting. Delayed jobs don't block a drain —
   *  matching BullMQ's 'drained' semantics. Used by InMemoryWorker;
   *  not part of the public BullMQ API. */
  _isDrained(): boolean {
    return this.waiting.length === 0;
  }

  /** Retain a finished job, dropping the oldest record once the cap is
   *  hit. Maps iterate in insertion order, so the first key is oldest. */
  private retain(store: Map<string, Job<T>>, job: Job<T>): void {
    store.set(job.id, job);
    if (store.size > MAX_RETAINED_JOBS) {
      const oldest = store.keys().next().value;
      if (oldest !== undefined) store.delete(oldest);
    }
  }

  // ── Introspection ──

  /** BullMQ-shape: return jobs in any of the requested states, e.g.
   *  getJobs(['waiting', 'delayed']). Unknown state names are ignored.
   *  The returned array is a snapshot — mutating it doesn't touch the
   *  queue (the Job objects themselves are live, as in BullMQ). */
  async getJobs(states: string[]): Promise<Job<T>[]> {
    const out: Job<T>[] = [];
    for (const state of new Set(states)) out.push(...this.jobsInState(state));
    return out;
  }

  /** BullMQ-shape: per-state job counts. With no arguments, returns
   *  counts for all five states. Unknown state names are ignored. */
  async getJobCounts(...states: string[]): Promise<Record<string, number>> {
    const wanted = states.length > 0 ? new Set(states) : new Set<string>(ALL_STATES);
    const counts: Record<string, number> = {};
    for (const state of wanted) {
      if (!(ALL_STATES as readonly string[]).includes(state)) continue;
      counts[state] = this.jobsInState(state).length;
    }
    return counts;
  }

  private jobsInState(state: string): Job<T>[] {
    switch (state) {
      case 'delayed': return [...this.delayed.values()].map((entry) => entry.job);
      case 'waiting': return [...this.waiting];
      case 'active': return [...this.active.values()];
      case 'completed': return [...this.completed.values()];
      case 'failed': return [...this.failed.values()];
      default: return [];
    }
  }

  // ── Lifecycle ──

  /** Drop every job in every state and cancel pending delay/backoff
   *  timers — an obliterated delayed job never runs. */
  async obliterate(): Promise<void> {
    for (const { timer } of this.delayed.values()) clearTimeout(timer);
    this.delayed.clear();
    this.waiting.length = 0;
    this.active.clear();
    this.completed.clear();
    this.failed.clear();
  }

  async close(): Promise<void> {
    await this.obliterate();
    this.removeAllListeners();
  }
}

/**
 * BullMQ-shaped worker. Events: 'completed' (job, result), 'failed'
 * (job, err), and 'drained' — emitted when the worker finishes a job
 * and finds the queue empty (once per drain, not on every idle poll).
 */
export class InMemoryWorker<T extends JobData = JobData> extends EventEmitter {
  private running = true;
  private activeCount = 0;
  /** True once a job has been taken since the last 'drained' emit —
   *  gates 'drained' to once per drain. */
  private wasBusy = false;
  /** close() callers parked until activeCount hits 0. */
  private readonly closeWaiters: Array<() => void> = [];
  private readonly processor: ProcessorFn<T>;
  private readonly concurrency: number;
  private queue: InMemoryQueue<T> | null = null;

  constructor(
    _name: string,
    processor: ProcessorFn<T>,
    opts?: { concurrency?: number; queue?: InMemoryQueue<T> },
  ) {
    super();
    this.processor = processor;
    this.concurrency = opts?.concurrency ?? 1;
    if (opts?.queue) this.attachQueue(opts.queue);
  }

  /** Wire this worker to a specific queue. The worker subscribes to
   *  the queue's 'waiting' events and processes jobs as they arrive. */
  attachQueue(queue: InMemoryQueue<T>): void {
    this.queue = queue;
    queue.on('waiting', () => this.tryProcess());
    // Drain anything already queued before the worker attached.
    setImmediate(() => this.tryProcess());
  }

  private async tryProcess(): Promise<void> {
    if (!this.running) return;
    if (this.activeCount >= this.concurrency) return;
    const queue = this.queue;
    if (!queue) return;

    const job = queue._takeNext();
    if (!job) return;

    this.activeCount++;
    this.wasBusy = true;
    try {
      job.attemptsMade++;
      const result = await this.processor(job);
      queue._markCompleted(job, result);
      this.emit('completed', job, result);
    } catch (err) {
      const maxAttempts = job.opts.attempts ?? 1;
      if (job.attemptsMade < maxAttempts) {
        // Exponential backoff: delay * 2^(attempt-1). attemptsMade is
        // 1-based after the increment above, so the first retry waits
        // exactly `delay`, the second waits `2*delay`, etc. The queue
        // holds the job as 'delayed' (attemptsMade preserved so the
        // retry budget tracks correctly) until its timer re-enqueues
        // it, which emits 'waiting' and kicks off the next tryProcess().
        const baseDelay = job.opts.backoff?.delay ?? 1000;
        const wait = baseDelay * Math.pow(2, job.attemptsMade - 1);
        queue._retryLater(job, wait);
      } else {
        queue._markFailed(job, err);
        this.emit('failed', job, err);
      }
    } finally {
      this.activeCount--;
      if (this.activeCount === 0) {
        // Unblock close() callers waiting for in-flight work.
        for (const resolve of this.closeWaiters.splice(0)) resolve();
        // 'drained': the worker just went idle with nothing left
        // waiting. wasBusy gates the emit to once per drain.
        if (this.wasBusy && queue._isDrained()) {
          this.wasBusy = false;
          this.emit('drained');
        }
      }
      // Drain anything still queued. setImmediate keeps each iteration
      // a clean turn of the event loop so a flood of jobs doesn't
      // starve other I/O.
      setImmediate(() => this.tryProcess());
    }
  }

  /** Stop the worker. No new jobs are started once closing; by default
   *  close() resolves only after in-flight jobs finish (BullMQ
   *  semantics). Pass force=true to skip the wait — running processors
   *  aren't aborted (promises can't be), close() just won't wait for
   *  them. */
  async close(force = false): Promise<void> {
    this.running = false;
    if (!force && this.activeCount > 0) {
      await new Promise<void>((resolve) => this.closeWaiters.push(resolve));
    }
    this.removeAllListeners();
  }
}
