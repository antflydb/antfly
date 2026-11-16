import { inspect } from "node:util";
import type { LanguageModelV4, LanguageModelV4CallOptions } from "@ai-sdk/provider";
import { expect, it, vi } from "vitest";
import { createAssetDisplayHandler } from "../src/asset/display-route.js";
import { AssetFetchError, createAssetMiddleware } from "../src/asset/middleware.js";
import { assetProviderReference, encodeAssetReference } from "../src/asset/reference.js";

const reference = { table: "assets", key: "a" };
const secret = "PRIVATE-CAPABILITY";
const resource = {
  type: "source",
  uri: `https://source.test/${secret}`,
  mime_type: "text/plain",
  version: '"v1"',
  length: 1,
};
const client = {
  getRawClient: () => ({
    GET: async () => ({ response: new Response(), data: { content: { resource } } }),
  }),
} as never;
const leaked = () =>
  new Error(`Request failed at ${resource.uri}`, { cause: { url: resource.uri } });
const params: LanguageModelV4CallOptions = {
  prompt: [
    {
      role: "user",
      content: [
        {
          type: "file",
          mediaType: "text/plain",
          data: { type: "reference", reference: assetProviderReference(reference) },
        },
      ],
    },
  ],
};
const model = { provider: "vendor" } as LanguageModelV4;
async function sanitized(pending: PromiseLike<unknown> | undefined) {
  let error: unknown;
  try {
    await pending;
  } catch (caught) {
    error = caught;
  }
  expect(error).toBeInstanceOf(AssetFetchError);
  expect(inspect(error, { depth: 10 })).not.toContain(secret);
  expect((error as Error).cause).toBeUndefined();
}
it.each([
  "fetch",
  "read",
] as const)("sanitizes %s failures for middleware and display streams", async (phase) => {
  const fetch: typeof globalThis.fetch = async () => {
    if (phase === "fetch") throw leaked();
    return new Response(
      new ReadableStream({
        pull() {
          throw leaked();
        },
      })
    );
  };
  const middleware = createAssetMiddleware({ client, authorize: () => true, fetch });
  await sanitized(middleware.transformParams?.({ type: "generate", params, model }));
  const handler = createAssetDisplayHandler({
    client,
    authorize: () => true,
    referenceId: () => encodeAssetReference(reference),
    fetch,
  });
  await sanitized(
    handler(new Request("https://app.test/file")).then((response) => response.text())
  );
});
it("settles display cancellation even when upstream cleanup fails", async () => {
  const cancel = vi.fn(() => {
    throw leaked();
  });
  const handler = createAssetDisplayHandler({
    client,
    authorize: () => true,
    referenceId: () => encodeAssetReference(reference),
    fetch: async () =>
      new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new Uint8Array([1]));
          },
          cancel,
        })
      ),
  });
  const response = await handler(new Request("https://app.test/file"));
  const reader = response.body?.getReader();
  expect((await reader?.read())?.value).toEqual(new Uint8Array([1]));
  await expect(reader?.cancel()).resolves.toBeUndefined();
  expect(cancel).toHaveBeenCalledOnce();
});
it.each([
  "GET",
  "HEAD",
])("sanitizes cancellation errors on rejected %s encodings", async (method) => {
  const handler = createAssetDisplayHandler({
    client,
    authorize: () => true,
    referenceId: () => encodeAssetReference(reference),
    fetch: async () =>
      new Response(
        new ReadableStream({
          cancel() {
            throw leaked();
          },
        }),
        { headers: { "content-encoding": "gzip" } }
      ),
  });
  const response = await handler(new Request("https://app.test/file", { method }));
  expect(response.status).toBe(502);
  expect(await response.text()).not.toContain(secret);
});
it("keeps the HTTP failure even if discarding its body fails", async () => {
  const middleware = createAssetMiddleware({
    client,
    authorize: () => true,
    fetch: async () =>
      new Response(
        new ReadableStream({
          cancel() {
            throw leaked();
          },
        }),
        { status: 412 }
      ),
  });
  await expect(
    middleware.transformParams?.({ type: "generate", params, model })
  ).rejects.toMatchObject({ status: 412, reason: "version-mismatch" });
});
it("preserves caller cancellation and disposes a late transport response", async () => {
  const controller = new AbortController();
  const reason = new Error("caller cancelled");
  let finish!: (response: Response) => void;
  const cancel = vi.fn();
  const fetch = vi.fn(
    async () =>
      await new Promise<Response>((resolve) => {
        finish = resolve;
      })
  );
  const middleware = createAssetMiddleware({ client, authorize: () => true, fetch });
  const pending = middleware.transformParams?.({
    type: "generate",
    params: { ...params, abortSignal: controller.signal },
    model,
  });
  const rejected = expect(pending).rejects.toBe(reason);
  await vi.waitFor(() => expect(fetch).toHaveBeenCalledOnce());
  controller.abort(reason);
  await rejected;
  finish(new Response(new ReadableStream({ cancel })));
  await vi.waitFor(() => expect(cancel).toHaveBeenCalledOnce());
});

it.each(["GET", "HEAD"])("does not expose upstream error diagnostics for %s", async (method) => {
  const cancel = vi.fn();
  const handler = createAssetDisplayHandler({
    client,
    authorize: () => true,
    referenceId: () => encodeAssetReference(reference),
    fetch: async () =>
      new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(resource.uri));
          },
          cancel,
        }),
        { status: 416, headers: { "content-length": "999", "content-range": "bytes */1" } }
      ),
  });
  const response = await handler(new Request("https://app.test/file", { method }));
  expect(response.status).toBe(416);
  expect(response.headers.get("content-range")).toBe("bytes */1");
  expect(response.headers.has("content-length")).toBe(false);
  expect(await response.text()).not.toContain(secret);
  expect(cancel).toHaveBeenCalledOnce();
});
it("does not pull a discarded HEAD body", async () => {
  const pull = vi.fn();
  const cancel = vi.fn();
  const handler = createAssetDisplayHandler({
    client,
    authorize: () => true,
    referenceId: () => encodeAssetReference(reference),
    fetch: async () => new Response(new ReadableStream({ pull, cancel }, { highWaterMark: 0 })),
  });
  const response = await handler(new Request("https://app.test/file", { method: "HEAD" }));
  expect(response.body).toBeNull();
  expect(pull).not.toHaveBeenCalled();
  expect(cancel).toHaveBeenCalledOnce();
});

it.each([
  false,
  true,
])("aborts a backpressured display stream (stalled cleanup: %s)", async (stalled) => {
  const controller = new AbortController();
  const reason = new Error("browser disconnected");
  const cancel = vi.fn(() => (stalled ? new Promise<void>(() => {}) : undefined));
  const pull = vi.fn((output: ReadableStreamDefaultController<Uint8Array>) =>
    output.enqueue(new Uint8Array([1]))
  );
  const handler = createAssetDisplayHandler({
    client,
    authorize: () => true,
    referenceId: () => encodeAssetReference(reference),
    fetch: async () => new Response(new ReadableStream({ pull, cancel }, { highWaterMark: 0 })),
  });
  const response = await handler(
    new Request("https://app.test/file", { signal: controller.signal })
  );
  await vi.waitFor(() => expect(pull).toHaveBeenCalledOnce());
  controller.abort(reason);
  expect(cancel).toHaveBeenCalledOnce();
  await expect(response.body?.getReader().read()).rejects.toBe(reason);
});

it("settles consumer cancellation without waiting for uncooperative cleanup", async () => {
  const cancel = vi.fn(() => new Promise<void>(() => {}));
  const handler = createAssetDisplayHandler({
    client,
    authorize: () => true,
    referenceId: () => encodeAssetReference(reference),
    fetch: async () => new Response(new ReadableStream({ cancel }, { highWaterMark: 0 })),
  });
  const response = await handler(new Request("https://app.test/file"));
  await response.body?.cancel();
  expect(cancel).toHaveBeenCalledOnce();
});
