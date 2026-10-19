/** Bounded local request ownership, shared by database and inference clients. */
export interface RequestAdmissionOptions {
  /** Active requests, including response consumption. Default: 100. */
  maxConcurrent?: number;
  /** Waiting requests. Zero refuses immediately when busy. Default: 100. */
  maxQueued?: number;
  /** Original queue wait ceiling in milliseconds. Zero refuses when busy. Default: 1000. */
  maxWaitMs?: number;
}

export class ClientCapacityError extends Error {
  readonly requestSent = false;

  constructor(readonly reason: "queue_full" | "wait_timeout" | "closed") {
    super(`Client request admission refused: ${reason}`);
    this.name = "ClientCapacityError";
  }
}

type Waiter = {
  deadline: number;
  signal?: AbortSignal;
  timer: ReturnType<typeof setTimeout>;
  abort: () => void;
  resolve: (release: () => void) => void;
  reject: (reason: unknown) => void;
};

/** One FIFO owner. Pass the same instance to clients that should share capacity. */
export class RequestAdmission {
  private readonly maxConcurrent: number;
  private readonly maxQueued: number;
  private readonly maxWaitMs: number;
  private active = 0;
  private closed = false;
  private queue: Waiter[] = [];

  constructor(options: RequestAdmissionOptions = {}) {
    this.maxConcurrent = options.maxConcurrent ?? 100;
    this.maxQueued = options.maxQueued ?? 100;
    this.maxWaitMs = options.maxWaitMs ?? 1000;
    for (const [name, value, minimum] of [
      ["maxConcurrent", this.maxConcurrent, 1],
      ["maxQueued", this.maxQueued, 0],
      ["maxWaitMs", this.maxWaitMs, 0],
    ] as const) {
      if (!Number.isSafeInteger(value) || value < minimum || value > 0x7fffffff) {
        throw new RangeError(`${name} must be an integer from ${minimum} to 2147483647`);
      }
    }
  }

  get stats(): Readonly<{ active: number; queued: number; closed: boolean }> {
    return { active: this.active, queued: this.queue.length, closed: this.closed };
  }

  /** Refuse new work and wake waiters. Active bodies remain owned until retired. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const waiter of this.queue.splice(0)) {
      this.detach(waiter);
      waiter.reject(new ClientCapacityError("closed"));
    }
  }

  private detach(waiter: Waiter): void {
    clearTimeout(waiter.timer);
    waiter.signal?.removeEventListener("abort", waiter.abort);
  }

  private refuse(waiter: Waiter, reason: unknown): void {
    const index = this.queue.indexOf(waiter);
    if (index === -1) return;
    this.queue.splice(index, 1);
    this.detach(waiter);
    waiter.reject(reason);
  }

  private lease(): () => void {
    this.active++;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.active--;
      this.grant();
    };
  }

  private grant(): void {
    while (!this.closed && this.active < this.maxConcurrent && this.queue.length > 0) {
      const waiter = this.queue.shift();
      if (!waiter) return;
      this.detach(waiter);
      if (waiter.signal?.aborted) {
        waiter.reject(waiter.signal.reason);
      } else if (performance.now() >= waiter.deadline) {
        waiter.reject(new ClientCapacityError("wait_timeout"));
      } else {
        waiter.resolve(this.lease());
      }
    }
  }

  private async acquire(signal?: AbortSignal): Promise<() => void> {
    signal?.throwIfAborted();
    if (this.closed) throw new ClientCapacityError("closed");
    this.grant();
    if (this.active < this.maxConcurrent && this.queue.length === 0) return this.lease();
    if (this.queue.length >= this.maxQueued || this.maxWaitMs === 0) {
      throw new ClientCapacityError("queue_full");
    }
    return new Promise((resolve, reject) => {
      const waiter: Waiter = {
        deadline: performance.now() + this.maxWaitMs,
        signal,
        timer: setTimeout(
          () => this.refuse(waiter, new ClientCapacityError("wait_timeout")),
          this.maxWaitMs
        ),
        abort: () => this.refuse(waiter, signal?.reason),
        resolve,
        reject,
      };
      this.queue.push(waiter);
      signal?.addEventListener("abort", waiter.abort, { once: true });
      if (signal?.aborted) waiter.abort();
    });
  }

  /** Hold capacity through response EOF, read failure or completed cancellation. */
  readonly fetch: typeof globalThis.fetch = async (input, init) => {
    const signal =
      init?.signal !== undefined
        ? init.signal
        : input instanceof Request
          ? input.signal
          : undefined;
    const release = await this.acquire(signal ?? undefined);
    try {
      signal?.throwIfAborted();
      const response = await globalThis.fetch(input, init);
      if (!response.body) {
        release();
        return response;
      }
      const reader = response.body.getReader();
      let canceling = false;
      const body = new ReadableStream<Uint8Array>(
        {
          start(controller) {
            // Fetch can abort/error an unread body after returning headers.
            // Observe that terminal failure without requiring another pull.
            // A successful close is not enough: explicit cancellation closes
            // pending reads before its physical cancellation promise settles.
            void reader.closed.catch((error: unknown) => {
              if (canceling) return;
              controller.error(error);
              // An already failed body may never be pulled. Release its reader
              // here as well as in pull's failure path.
              reader.releaseLock();
              release();
            });
          },
          async pull(controller) {
            try {
              const result = await reader.read();
              if (canceling) return;
              if (result.done) {
                controller.close();
                reader.releaseLock();
                release();
              } else {
                controller.enqueue(result.value);
              }
            } catch (error) {
              if (canceling) return;
              controller.error(error);
              reader.releaseLock();
              release();
            }
          },
          async cancel(reason) {
            // cancel() settles pending reads before the source's cancellation
            // promise. Only this branch may retire the lease during that gap.
            canceling = true;
            try {
              await reader.cancel(reason);
            } finally {
              reader.releaseLock();
              release();
            }
          },
        },
        // Do not pull/retire an unread response merely because headers arrived.
        { highWaterMark: 0 }
      );
      const owned = new Response(body, {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
      });
      Object.defineProperties(owned, {
        url: { value: response.url },
        redirected: { value: response.redirected },
        type: { value: response.type },
      });
      return owned;
    } catch (error) {
      release();
      throw error;
    }
  };
}
