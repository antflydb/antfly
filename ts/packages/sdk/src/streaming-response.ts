/** Await headers with explicit ownership of responses racing caller cancellation. */
export async function awaitResponse(
  pending: Promise<Response>,
  signal?: AbortSignal
): Promise<Response> {
  let aborted: (() => void) | undefined;
  try {
    if (!signal) return await pending;
    signal.throwIfAborted();
    const response = await new Promise<Response>((resolve, reject) => {
      aborted = () => reject(signal.reason);
      signal.addEventListener("abort", aborted, { once: true });
      pending.then(resolve, reject);
    });
    signal.throwIfAborted();
    return response;
  } catch (error) {
    // Always observe fulfillment too: cancellation can win after fetch returned
    // a response but before the awaiting continuation received ownership.
    void pending.then((response) => response.body?.cancel()).catch(() => {});
    throw error;
  } finally {
    if (aborted) signal?.removeEventListener("abort", aborted);
  }
}

/** A pull-driven decoded stream that owns its HTTP reader before the first pull. */
export function decodedResponseStream<T>(
  body: ReadableStream<Uint8Array>,
  decode: (bytes: ReadableStream<Uint8Array>) => AsyncGenerator<T, void, void>,
  signal?: AbortSignal
): ReadableStream<T> {
  const reader = body.getReader();
  let disposed = false;
  let terminated = false;
  let controller: ReadableStreamDefaultController<T>;
  const dispose = () => {
    if (disposed) return;
    disposed = true;
    signal?.removeEventListener("abort", abort);
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  };
  const bytes = new ReadableStream<Uint8Array>(
    {
      async pull(output) {
        try {
          const next = await reader.read();
          if (next.done) output.close();
          else output.enqueue(next.value);
        } catch (error) {
          output.error(error);
        }
      },
      cancel: dispose,
    },
    { highWaterMark: 0 }
  );
  const iterator = decode(bytes);
  const abort = () => {
    if (terminated) return;
    terminated = true;
    controller.error(signal?.reason);
    dispose();
    void iterator.return().catch(() => {});
  };
  const stream = new ReadableStream<T>(
    {
      start(output) {
        controller = output;
      },
      async pull(output) {
        try {
          const next = await iterator.next();
          if (terminated) return;
          if (next.done) {
            terminated = true;
            dispose();
            output.close();
          } else output.enqueue(next.value);
        } catch (error) {
          if (!terminated) {
            terminated = true;
            dispose();
            output.error(signal?.aborted ? signal.reason : error);
          }
        }
      },
      cancel() {
        terminated = true;
        dispose();
        // Never wait for a generator return queued behind an uncooperative read.
        void iterator.return().catch(() => {});
      },
    },
    { highWaterMark: 0 }
  );
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  return stream;
}
