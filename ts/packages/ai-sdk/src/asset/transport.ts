import { abortable } from "../abort.js";

/** Public errors never retain a capability URL or an underlying transport cause. */
export class AssetFetchError extends Error {
  constructor(
    readonly status: number,
    readonly reason:
      | "version-mismatch"
      | "fetch-failed"
      | "length-mismatch"
      | "invalid-length"
      | "unsupported-encoding"
  ) {
    super(
      reason === "version-mismatch"
        ? "Asset fetch failed: the pinned version no longer matches (If-Match failed)."
        : reason === "length-mismatch"
          ? "Asset fetch length differs from its pinned descriptor."
          : reason === "invalid-length"
            ? "Asset descriptor has an invalid byte length."
            : reason === "unsupported-encoding"
              ? "Asset fetch returned unsupported content encoding."
              : status > 0
                ? `Asset fetch failed with HTTP ${status}.`
                : "Asset fetch failed."
    );
    this.name = "AssetFetchError";
  }
}

export class AssetByteLimitError extends Error {
  constructor(
    readonly scope: "asset" | "request",
    readonly limit: number
  ) {
    super(`Asset ${scope} exceeds the configured ${limit} byte limit.`);
    this.name = "AssetByteLimitError";
  }
}

function transportError(signal?: AbortSignal): unknown {
  return signal?.aborted ? signal.reason : new AssetFetchError(0, "fetch-failed");
}

/** Best-effort disposal must not replace a useful HTTP error with a raw cancellation error. */
export async function discardSourceBody(
  body: ReadableStream<Uint8Array> | null,
  signal?: AbortSignal
): Promise<void> {
  try {
    if (body) await abortable(body.cancel(), signal);
  } catch {
    /* No transport details escape. */
  }
}

/** Preserve backpressure and bytes while sanitizing late read/cancel failures too. */
function sourceStream(
  body: ReadableStream<Uint8Array>,
  signal?: AbortSignal
): ReadableStream<Uint8Array> {
  const reader = body.getReader();
  let finished = false;
  let output: ReadableStreamDefaultController<Uint8Array>;
  const release = () => {
    signal?.removeEventListener("abort", abort);
    reader.releaseLock();
  };
  const dispose = (reason?: unknown) => {
    if (finished) return;
    finished = true;
    // Cancellation must not wait for an injected transport to settle.
    void reader.cancel(reason).catch(() => {});
    release();
  };
  const abort = () => {
    if (finished) return;
    output.error(transportError(signal));
    dispose(signal?.reason);
  };
  return new ReadableStream<Uint8Array>({
    start(controller) {
      output = controller;
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted) abort();
    },
    async pull(controller) {
      try {
        const next = await reader.read();
        if (finished) return;
        if (next.done) {
          finished = true;
          release();
          controller.close();
        } else controller.enqueue(next.value);
      } catch {
        if (finished) return;
        controller.error(transportError(signal));
        dispose();
      }
    },
    cancel(reason) {
      dispose(reason);
    },
  });
}

/** The capability is used only here; neither thrown messages nor causes retain it. */
export async function fetchSourceResponse(
  fetchFn: typeof fetch,
  uri: string,
  init: RequestInit
): Promise<Response> {
  const signal = init.signal ?? undefined;
  signal?.throwIfAborted();
  try {
    const headers = new Headers(init.headers);
    headers.set("accept-encoding", "identity");
    const pending = Promise.resolve().then(() => {
      signal?.throwIfAborted();
      return fetchFn(uri, { ...init, headers });
    });
    let response: Response;
    try {
      response = await abortable(pending, signal);
      signal?.throwIfAborted();
    } catch (error) {
      void pending.then((value) => discardSourceBody(value.body)).catch(() => {});
      throw error;
    }
    // Callers discard HTTP error/HEAD bodies; do not speculatively pull them.
    if (!response.body || init.method === "HEAD" || !response.ok) return response;
    return new Response(sourceStream(response.body, signal), {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  } catch {
    throw transportError(signal);
  }
}

/** One bounded allocation; reject dishonest or truncated responses without buffering their tail. */
export async function readSourceBytes(
  response: Response,
  expectedLength: number,
  signal?: AbortSignal
): Promise<Uint8Array> {
  const encoding = response.headers.get("content-encoding")?.trim().toLowerCase();
  if (encoding && encoding !== "identity") {
    await discardSourceBody(response.body, signal);
    throw new AssetFetchError(response.status, "unsupported-encoding");
  }
  const declared = response.headers.get("content-length");
  if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) !== expectedLength)) {
    await discardSourceBody(response.body, signal);
    throw new AssetFetchError(response.status, "length-mismatch");
  }
  const bytes = new Uint8Array(expectedLength);
  if (!response.body) {
    if (expectedLength !== 0) throw new AssetFetchError(response.status, "length-mismatch");
    return bytes;
  }
  const reader = response.body.getReader();
  let offset = 0;
  try {
    while (true) {
      const next = await abortable(reader.read(), signal);
      if (next.done) break;
      if (next.value.byteLength > expectedLength - offset)
        throw new AssetByteLimitError("asset", expectedLength);
      bytes.set(next.value, offset);
      offset += next.value.byteLength;
    }
    if (offset !== expectedLength) throw new AssetFetchError(response.status, "length-mismatch");
    return bytes;
  } catch (error) {
    void reader.cancel().catch(() => {});
    signal?.throwIfAborted();
    if (error instanceof AssetByteLimitError || error instanceof AssetFetchError) throw error;
    throw new AssetFetchError(0, "fetch-failed");
  } finally {
    reader.releaseLock();
  }
}
