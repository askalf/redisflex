/**
 * In-memory job queue with a BullMQ-shaped surface.
 *
 * Implements the subset of the BullMQ API that real apps actually use:
 *   - Queue#add(name, data, opts)
 *   - Worker(name, processor, { concurrency })
 *   - Worker#close()
 *   - Retry with exponential backoff (`opts.attempts`, `opts.backoff`)
 *   - Events: 'completed' (job, result), 'failed' (job, err)
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

export interface JobOpts {
  /** Delay (ms) before the job becomes available to workers. */
  delay?: number;
  /** Max retries (including the first attempt). Default 1 (no retries). */
  attempts?: number;
  /** Backoff config. Only `delay` is used; the curve is exponential
   *  (delay * 2^(attempt-1)). The `type` field is accepted for BullMQ
   *  shape compatibility but ignored — we always do exponential. */
  backoff?: { type?: string; delay: number };
  removeOnComplete?: boolean;
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

export class InMemoryQueue<T extends JobData = JobData> extends EventEmitter {
  readonly name: string;
  private readonly waiting: Job<T>[] = [];
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
      // the queue is in-process so wall-clock drift is bounded.
      setTimeout(() => {
        this.waiting.push(job);
        this.emit('waiting', job);
      }, opts.delay).unref?.();
    } else {
      this.waiting.push(job);
      this.emit('waiting', job);
    }

    return job;
  }

  /** Pop one job off the front of the waiting list. Returns undefined
   *  if the queue is empty. Used by InMemoryWorker; not part of the
   *  public BullMQ API. */
  _takeNext(): Job<T> | undefined {
    return this.waiting.shift();
  }

  /** Re-enqueue a job (used by the worker for retry-after-backoff).
   *  Pushes to the front of the waiting list so retries are prioritized
   *  over fresh jobs — typical "drain backed-up retries first" semantics. */
  _reenqueue(job: Job<T>): void {
    this.waiting.unshift(job);
    this.emit('waiting', job);
  }

  /** BullMQ-shape: getJobs(['waiting']) returns the current waiting set.
   *  Other states (active, completed, failed, delayed) aren't tracked. */
  async getJobs(_states: string[]): Promise<Job<T>[]> {
    return [...this.waiting];
  }

  async obliterate(): Promise<void> {
    this.waiting.length = 0;
  }

  async close(): Promise<void> {
    this.waiting.length = 0;
    this.removeAllListeners();
  }
}

export class InMemoryWorker<T extends JobData = JobData> extends EventEmitter {
  private running = true;
  private activeCount = 0;
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
    if (!this.queue) return;

    const job = this.queue._takeNext();
    if (!job) return;

    this.activeCount++;
    try {
      job.attemptsMade++;
      const result = await this.processor(job);
      this.emit('completed', job, result);
    } catch (err) {
      const maxAttempts = job.opts.attempts ?? 1;
      if (job.attemptsMade < maxAttempts) {
        // Exponential backoff: delay * 2^(attempt-1). attemptsMade is
        // 1-based after the increment above, so the first retry waits
        // exactly `delay`, the second waits `2*delay`, etc.
        const baseDelay = job.opts.backoff?.delay ?? 1000;
        const wait = baseDelay * Math.pow(2, job.attemptsMade - 1);
        setTimeout(() => {
          // Re-enqueue with attemptsMade preserved so the retry budget
          // tracks correctly across invocations. _reenqueue() also
          // emits 'waiting', which kicks off the next tryProcess().
          this.queue?._reenqueue(job);
        }, wait).unref?.();
      } else {
        this.emit('failed', job, err);
      }
    } finally {
      this.activeCount--;
      // Drain anything still queued. setImmediate keeps each iteration
      // a clean turn of the event loop so a flood of jobs doesn't
      // starve other I/O.
      setImmediate(() => this.tryProcess());
    }
  }

  async close(): Promise<void> {
    this.running = false;
    this.removeAllListeners();
  }
}
