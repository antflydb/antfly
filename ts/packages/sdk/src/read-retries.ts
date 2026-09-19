/** Opt-in query retries. Admission and backoff share the original signal and
 * query-body timeout_ms budget (the shortest submitted NDJSON line wins). */
export interface ReadRetryPolicy {
  maxAttempts: number;
  maxElapsedMs: number;
  initialBackoffMs: number;
  maxBackoffMs: number;
}

const queryPath =
  /^\/db\/v1\/(query|tables\/[^/]+\/query|databases\/[^/]+\/namespaces\/[^/]+\/tables\/[^/]+\/query)$/;

function bodyBudget(
  body: Uint8Array,
  contentType: string | null,
  maximum: number
): number | undefined {
  const text = new TextDecoder("utf-8", { fatal: true }).decode(body);
  const lines =
    contentType?.split(";", 1)[0]?.trim().toLowerCase() === "application/x-ndjson"
      ? text.split("\n").filter((line) => line.trim())
      : [text];
  if (!lines.length) return undefined;
  for (const line of lines) {
    const object = JSON.parse(line);
    if (!object || typeof object !== "object" || Array.isArray(object)) return undefined;
    const timeout = object.timeout_ms;
    if (timeout === undefined || timeout === null) continue;
    if (!Number.isSafeInteger(timeout) || timeout < 0) return undefined;
    maximum = Math.min(maximum, timeout);
  }
  return maximum;
}

function validate(policy: ReadRetryPolicy): void {
  const { maxAttempts, maxElapsedMs, initialBackoffMs, maxBackoffMs } = policy;
  if (
    ![maxAttempts, maxElapsedMs, initialBackoffMs, maxBackoffMs].every(Number.isSafeInteger) ||
    maxAttempts < 2 ||
    maxAttempts > 5 ||
    maxElapsedMs <= 0 ||
    maxElapsedMs > 60_000 ||
    initialBackoffMs <= 0 ||
    maxBackoffMs < initialBackoffMs ||
    maxBackoffMs > maxElapsedMs
  ) {
    throw new TypeError("Invalid Antfly read retry policy");
  }
}

async function wait(ms: number, signal: AbortSignal): Promise<void> {
  signal.throwIfAborted();
  return new Promise((resolve, reject) => {
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, ms);
    signal.addEventListener("abort", abort, { once: true });
  });
}

/** Place outside admission so rejected attempts return slots before backoff.
 * Query input is buffered at most 1 MiB; larger/streaming inputs bypass retries.
 * Query results may observe newer data when eventually admitted.
 */
export function readRetryFetch(
  base: typeof globalThis.fetch,
  policy?: ReadRetryPolicy
): typeof globalThis.fetch {
  if (!policy) return base;
  validate(policy);
  const config = { ...policy };
  return async (input, init) => {
    const request = new Request(input, init);
    if (
      request.method !== "POST" ||
      !queryPath.test(new URL(request.url).pathname) ||
      init?.body instanceof ReadableStream
    )
      return base(input, init);
    let signal = AbortSignal.any([request.signal, AbortSignal.timeout(config.maxElapsedMs)]);
    const started = performance.now();
    let deadline = started + config.maxElapsedMs;
    // A Request can hide an arbitrary stream. Only inspect a bounded prefix of
    // a clone and preserve the untouched original when it cannot be replayed.
    const copy = request.clone();
    const reader = copy.body?.getReader();
    const chunks: Uint8Array[] = [];
    let size = 0;
    const abort = () => {
      void reader?.cancel(signal.reason).catch(() => {});
    };
    signal.addEventListener("abort", abort, { once: true });
    try {
      if (reader)
        while (true) {
          signal.throwIfAborted();
          const chunk = await reader.read();
          signal.throwIfAborted();
          if (chunk.done) break;
          size += chunk.value.byteLength;
          if (size > 1 << 20) {
            void reader.cancel().catch(() => {});
            return base(request);
          }
          chunks.push(chunk.value);
        }
    } catch (error) {
      void request.body?.cancel(error).catch(() => {});
      void reader?.cancel(error).catch(() => {});
      throw error;
    } finally {
      signal.removeEventListener("abort", abort);
      reader?.releaseLock();
    }
    // Both tee branches must retire; cancellation is not awaited while its
    // sibling might still be draining.
    void request.body?.cancel().catch(() => {});
    const body = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      body.set(chunk, offset);
      offset += chunk.byteLength;
    }
    let budget: number | undefined;
    try {
      budget = bodyBudget(body, request.headers.get("Content-Type"), config.maxElapsedMs);
    } catch {
      /* Unknown body contracts bypass retries. */
    }
    if (budget === undefined) return base(new Request(request, { body, signal: request.signal }));
    deadline = started + budget;
    const remaining = deadline - performance.now();
    if (remaining <= 0) throw new DOMException("Antfly query deadline expired", "TimeoutError");
    signal = AbortSignal.any([signal, AbortSignal.timeout(Math.ceil(remaining))]);
    for (let attempt = 1; ; attempt++) {
      signal.throwIfAborted();
      const response = await base(new Request(request, { body, signal }));
      if (signal.aborted) {
        void response.body?.cancel(signal.reason).catch(() => {});
        signal.throwIfAborted();
      }
      if (attempt >= config.maxAttempts || response.status !== 429) return response;
      const length = response.headers.get("Content-Length");
      if (length === null || !/^\d+$/.test(length) || Number(length) > 16_384) return response;
      // The declared error bound is verified before parsing. Unknown/chunked
      // responses remain visible to callers without speculative retries.
      const copy = response.clone();
      const errorReader = copy.body?.getReader();
      if (!errorReader) return response;
      let errorSize = 0;
      const errors: Uint8Array[] = [];
      const cancelError = (reason: unknown) => {
        // Both tee branches must cancel together. Awaiting either first can
        // deadlock behind its unread sibling, especially with custom fetch.
        void errorReader.cancel(reason).catch(() => {});
        void response.body?.cancel(reason).catch(() => {});
      };
      const abortError = () => cancelError(signal.reason);
      signal.addEventListener("abort", abortError, { once: true });
      try {
        while (true) {
          signal.throwIfAborted();
          const next = await errorReader.read();
          signal.throwIfAborted();
          if (next.done) break;
          errorSize += next.value.byteLength;
          if (errorSize > 16_384) {
            void errorReader.cancel().catch(() => {});
            return response;
          }
          errors.push(next.value);
        }
      } catch (error) {
        cancelError(error);
        throw error;
      } finally {
        signal.removeEventListener("abort", abortError);
        errorReader.releaseLock();
      }
      const encoded = new Uint8Array(errorSize);
      let position = 0;
      for (const chunk of errors) {
        encoded.set(chunk, position);
        position += chunk.byteLength;
      }
      let detail: Record<string, unknown>;
      try {
        detail = JSON.parse(new TextDecoder().decode(encoded));
      } catch {
        return response;
      }
      if (
        detail?.reason !== "instance_busy" ||
        detail.stage !== "admission" ||
        detail.execution_started !== false
      )
        return response;
      let delay = Math.min(config.initialBackoffMs * 2 ** (attempt - 1), config.maxBackoffMs);
      const after = response.headers.get("Retry-After");
      if (after !== null) {
        if (!/^\d+$/.test(after) || Number(after) * 1_000 > config.maxBackoffMs) return response;
        delay = Math.max(delay, Number(after) * 1_000);
      }
      if (performance.now() + delay >= deadline) return response;
      await response.body?.cancel();
      await wait(delay, signal);
    }
  };
}
