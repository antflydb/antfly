import { afterEach, expect, it, vi } from "vitest";
import { InferenceClient } from "../src/inference-client.js";
import { RequestAdmission } from "../src/request-admission.js";

const base = "http://test";
const request = { input: "hello", embedding: { model: "model" } };
const context = {
  object: "chunk.embedding.list",
  data: [],
  status: "complete",
  source_fingerprint: "2".repeat(64),
  source_version: "v1",
  plan_id: "1".repeat(64),
  model: "model",
  model_identity: "3".repeat(64),
  model_revision: "4".repeat(64),
  task_type: "RETRIEVAL_DOCUMENT",
  dimensions: 2,
  policy: {},
  start_ordinal: 0,
  covered_through_ordinal: 0,
  gaps: [],
};
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((ok) => {
    resolve = ok;
  });
  return { promise, resolve };
}
function stream(cancel?: () => Promise<void> | void) {
  return new Response(
    new ReadableStream<Uint8Array>(
      {
        start(controller) {
          controller.enqueue(
            new TextEncoder().encode(
              `${JSON.stringify({ object: "chunk.embedding.start", context })}\n`
            )
          );
        },
        cancel,
      },
      { highWaterMark: 0 }
    ),
    { headers: { "Content-Type": "application/x-ndjson" } }
  );
}
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

it.each([
  "queue_full",
  "closed",
])("chunk streaming refuses %s before sending through the shared owner", async (reason) => {
  const owner = new RequestAdmission({ maxConcurrent: 1, maxQueued: 0 });
  const client = new InferenceClient({ baseUrl: base, requestAdmission: owner });
  const fetch = vi
    .fn()
    .mockResolvedValueOnce(new Response("held"))
    .mockImplementation(async () => stream());
  vi.stubGlobal("fetch", fetch);
  const held = await owner.fetch(`${base}/held`);
  const iterator = client.chunkAndEmbedStream(request);
  if (reason === "closed") client.close();
  try {
    await expect(iterator.next()).rejects.toMatchObject({ reason, requestSent: false });
    expect(fetch).toHaveBeenCalledTimes(1);
  } finally {
    await held.body?.cancel();
    await iterator.return(undefined);
    client.close();
  }
  expect(owner.stats.active).toBe(0);
});

it("chunk streaming unlinks an aborted queued request without sending", async () => {
  const owner = new RequestAdmission({ maxConcurrent: 1, maxQueued: 1 });
  const client = new InferenceClient({ baseUrl: base, requestAdmission: owner });
  const fetch = vi
    .fn()
    .mockResolvedValueOnce(new Response("held"))
    .mockImplementation(async () => stream());
  vi.stubGlobal("fetch", fetch);
  const held = await owner.fetch(`${base}/held`);
  const controller = new AbortController();
  const iterator = client.chunkAndEmbedStream(request, { signal: controller.signal });
  const pending = iterator.next().catch((error: unknown) => error);
  const reason = new Error("caller abandoned queued chunks");
  try {
    await vi.waitFor(() => expect(owner.stats.queued).toBe(1));
    controller.abort(reason);
    expect(await pending).toBe(reason);
    expect(owner.stats.queued).toBe(0);
    expect(fetch).toHaveBeenCalledTimes(1);
  } finally {
    controller.abort(reason);
    await held.body?.cancel();
    await pending;
    await iterator.return(undefined);
    client.close();
  }
  expect(owner.stats.active).toBe(0);
});

it("chunk streaming retains capacity until source cancellation physically completes", async () => {
  const owner = new RequestAdmission({ maxConcurrent: 1, maxQueued: 0 });
  const client = new InferenceClient({ baseUrl: base, requestAdmission: owner });
  const finished = deferred();
  const cancel = vi.fn(() => finished.promise);
  const fetch = vi.fn(async () => stream(cancel));
  vi.stubGlobal("fetch", fetch);
  const iterator = client.chunkAndEmbedStream(request);
  expect((await iterator.next()).value?.object).toBe("chunk.embedding.start");
  const closing = iterator.return(undefined);
  try {
    await vi.waitFor(() => expect(cancel).toHaveBeenCalledOnce());
    expect(owner.stats.active).toBe(1);
    await expect(owner.fetch(`${base}/must-not-send`)).rejects.toMatchObject({
      reason: "queue_full",
      requestSent: false,
    });
    expect(fetch).toHaveBeenCalledTimes(1);
  } finally {
    finished.resolve();
    await closing;
    client.close();
  }
  expect(owner.stats.active).toBe(0);
});

it.each([
  false,
  true,
])("chunk streaming retires invalid content type and preserves the error when cancel rejects=%s", async (reject) => {
  const owner = new RequestAdmission({ maxConcurrent: 1, maxQueued: 0 });
  const client = new InferenceClient({ baseUrl: base, requestAdmission: owner });
  const cancel = vi.fn(async () => {
    if (reject) throw new Error("source close failed");
  });
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(new ReadableStream({ cancel }, { highWaterMark: 0 }), {
          headers: { "Content-Type": "text/plain" },
        })
    )
  );
  await expect(client.chunkAndEmbedStream(request).next()).rejects.toThrow(
    "Expected chunk embedding NDJSON stream"
  );
  expect(cancel).toHaveBeenCalledOnce();
  expect(owner.stats.active).toBe(0);
  client.close();
});
