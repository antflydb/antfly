// Copyright 2026 The Antfly Contributors
// SPDX-License-Identifier: Apache-2.0

/** Optional client-local limits. Server admission remains authoritative. */
export interface ClientAdmission {
  maxInFlight: number;
  maxQueued: number;
  maxWaitMs: number;
}

/** This attempt never reached fetch. Earlier attempts may have executed. */
export class ClientBusyError extends Error {
  readonly executionStarted = false;

  constructor() {
    super("Antfly client admission capacity exhausted");
    this.name = "ClientBusyError";
  }
}

type Release = () => void;
interface Waiter {
  deadline: number;
  signal?: AbortSignal;
  resolve: (release: Release) => void;
  reject: (error: unknown) => void;
  cleanup: () => void;
}

/** Reuse one pool across database and inference clients to share their bound. */
export class AdmissionPool {
  private active = 0;
  private readonly queue: Waiter[] = [];
  private readonly config: Readonly<ClientAdmission>;

  constructor(config: ClientAdmission) {
    if (
      !Number.isSafeInteger(config.maxInFlight) ||
      config.maxInFlight <= 0 ||
      !Number.isSafeInteger(config.maxQueued) ||
      config.maxQueued < 0 ||
      !Number.isSafeInteger(config.maxWaitMs) ||
      config.maxWaitMs < 0 ||
      config.maxWaitMs > 2_147_483_647 ||
      (config.maxQueued > 0 && config.maxWaitMs === 0)
    )
      throw new TypeError("Invalid Antfly client admission limits");
    this.config = { ...config };
  }

  get stats(): Readonly<{ active: number; queued: number }> {
    return { active: this.active, queued: this.queue.length };
  }

  private grant(): Release {
    this.active++;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.active--;
      this.pump();
    };
  }

  private pump(): void {
    while (this.active < this.config.maxInFlight && this.queue.length > 0) {
      const waiter = this.queue.shift()!;
      waiter.cleanup();
      if (waiter.signal?.aborted) waiter.reject(waiter.signal.reason);
      else if (performance.now() >= waiter.deadline) waiter.reject(new ClientBusyError());
      else waiter.resolve(this.grant());
    }
  }

  private acquire(signal?: AbortSignal): Promise<Release> {
    if (signal?.aborted) return Promise.reject(signal.reason);
    if (this.queue.length === 0 && this.active < this.config.maxInFlight) {
      return Promise.resolve(this.grant());
    }
    if (this.queue.length >= this.config.maxQueued || this.config.maxWaitMs === 0) {
      return Promise.reject(new ClientBusyError());
    }
    return new Promise((resolve, reject) => {
      const retire = (error: unknown) => {
        const index = this.queue.indexOf(waiter);
        if (index < 0) return;
        this.queue.splice(index, 1);
        waiter.cleanup();
        reject(error);
        this.pump();
      };
      const abort = () => retire(signal?.reason);
      const timer = setTimeout(() => retire(new ClientBusyError()), this.config.maxWaitMs);
      const waiter: Waiter = {
        deadline: performance.now() + this.config.maxWaitMs,
        signal,
        resolve,
        reject,
        cleanup: () => {
          clearTimeout(timer);
          signal?.removeEventListener("abort", abort);
        },
      };
      this.queue.push(waiter);
      signal?.addEventListener("abort", abort, { once: true });
    });
  }

  /** No automatic retry, including when a dispatched write has an unknown outcome. */
  wrap(
    base: typeof globalThis.fetch = (...args) => globalThis.fetch(...args)
  ): typeof globalThis.fetch {
    return async (input, init) => {
      const signal = init?.signal ?? (input instanceof Request ? input.signal : undefined);
      const release = await this.acquire(signal ?? undefined);
      try {
        signal?.throwIfAborted();
        const response = await base(input, init);
        if (signal?.aborted) {
          await response.body?.cancel(signal.reason);
          signal.throwIfAborted();
        }
        if (!response.body) {
          release();
          return response;
        }
        const reader = response.body.getReader();
        let finished = false;
        const finish = () => {
          if (finished) return;
          finished = true;
          signal?.removeEventListener("abort", abort);
          reader.releaseLock();
          release();
        };
        const cancel = async (reason?: unknown) => {
          try {
            await reader.cancel(reason);
          } finally {
            finish();
          }
        };
        const abort = () => {
          void cancel(signal?.reason).catch(() => {});
        };
        signal?.addEventListener("abort", abort, { once: true });
        const body = new ReadableStream<Uint8Array>(
          {
            async pull(controller) {
              try {
                signal?.throwIfAborted();
                const result = await reader.read();
                signal?.throwIfAborted();
                if (result.done) {
                  controller.close();
                  finish();
                } else controller.enqueue(result.value);
              } catch (error) {
                controller.error(error);
                try {
                  await cancel(error);
                } catch {
                  /* preserve the read error */
                }
              }
            },
            cancel,
          },
          { highWaterMark: 0 }
        );
        const wrapped = new Response(body, {
          status: response.status,
          statusText: response.statusText,
          headers: response.headers,
        });
        // Keep metadata used by redirect/authentication middleware.
        for (const key of ["url", "redirected", "type"] as const) {
          Object.defineProperty(wrapped, key, { value: response[key] });
        }
        return wrapped;
      } catch (error) {
        release();
        throw error;
      }
    };
  }
}

export function admissionFetch(config?: ClientAdmission | AdmissionPool): typeof globalThis.fetch {
  return config
    ? (config instanceof AdmissionPool ? config : new AdmissionPool(config)).wrap()
    : (...args) => globalThis.fetch(...args);
}
