import type { LanguageModelV4, LanguageModelV4CallOptions } from "@ai-sdk/provider";
import { describe, expect, it, vi } from "vitest";
import {
  AssetByteLimitError,
  AssetFetchError,
  createAssetMiddleware,
} from "../src/asset/middleware.js";
import { assetProviderReference } from "../src/asset/reference.js";

const model = { provider: "vendor" } as LanguageModelV4;
function fixture(length = 4, options: Partial<Parameters<typeof createAssetMiddleware>[0]> = {}) {
  const resource = {
    type: "source",
    uri: "https://sources.test/private-capability",
    version: '"v1"',
    mime_type: "video/mp4",
    length,
  };
  const GET = vi.fn(async (_input: unknown, request: { params: { path: { key: string } } }) => ({
    response: new Response(),
    data: {
      content: { resource: { ...resource, uri: `${resource.uri}/${request.params.path.key}` } },
    },
  }));
  const authorize = vi.fn(() => true);
  const fetch = vi.fn(async () => new Response(new Uint8Array(length)));
  const middleware = createAssetMiddleware({
    client: { getRawClient: () => ({ GET }) } as never,
    authorize,
    fetch,
    ...options,
  });
  const invoke = (keys: string[], signal?: AbortSignal) => {
    const params: LanguageModelV4CallOptions = {
      abortSignal: signal,
      prompt: keys.map((key) => ({
        role: "user",
        content: [
          {
            type: "file",
            mediaType: "video/mp4",
            data: {
              type: "reference",
              reference: assetProviderReference({ table: "assets", key }),
            },
          },
        ],
      })),
    };
    return middleware.transformParams?.({ type: "generate", params, model });
  };
  return { invoke, fetch, authorize, GET };
}

describe("asset transfer admission", () => {
  it("shares pinned bytes within a request while authorizing every occurrence, and never across requests", async () => {
    const { invoke, fetch, authorize } = fixture();
    const first = await invoke(Array(24).fill("same"));
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(authorize).toHaveBeenCalledTimes(24);
    const prompt = first?.prompt as { content: { data: { data: Uint8Array } }[] }[];
    expect(prompt[0].content[0].data.data).toBe(prompt[23].content[0].data.data);
    await invoke(["same"]);
    expect(fetch).toHaveBeenCalledTimes(2);
  });
  it("bounds concurrent work across messages", async () => {
    let active = 0,
      peak = 0;
    const release: (() => void)[] = [];
    const transfer = vi.fn(async () => {
      peak = Math.max(peak, ++active);
      await new Promise<void>((resolve) => release.push(resolve));
      active--;
      return new Response(new Uint8Array(4));
    });
    const { invoke } = fixture(4, { fetch: transfer, maxConcurrentAssetFetches: 2 });
    const pending = invoke(["a", "b", "c", "d", "e"]);
    await vi.waitFor(() => expect(transfer).toHaveBeenCalledTimes(2));
    release.shift()?.();
    await vi.waitFor(() => expect(transfer).toHaveBeenCalledTimes(3));
    while (transfer.mock.calls.length < 5) {
      release.shift()?.();
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    for (const done of release) done();
    await pending;
    expect(peak).toBe(2);
  });
  it("rejects an oversized descriptor before media fetch", async () => {
    const { invoke, fetch } = fixture(5, { maxAssetBytes: 4 });
    await expect(invoke(["a"])).rejects.toMatchObject({ scope: "asset", limit: 4 });
    expect(fetch).not.toHaveBeenCalled();
  });
  it("charges repeated parts against expanded request size even when their bytes are shared", async () => {
    const { invoke, fetch } = fixture(4, { maxTotalAssetBytes: 8, maxConcurrentAssetFetches: 1 });
    await expect(invoke(["a", "a", "a"])).rejects.toMatchObject({ scope: "request", limit: 8 });
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it("cancels and rejects a body exceeding the pinned size without reading its tail", async () => {
    const cancel = vi.fn();
    const transfer = vi.fn(
      async () =>
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new Uint8Array(5));
            },
            cancel,
          })
        )
    );
    const { invoke } = fixture(4, { fetch: transfer });
    await expect(invoke(["a"])).rejects.toBeInstanceOf(AssetByteLimitError);
    expect(cancel).toHaveBeenCalledOnce();
  });
  it.each([2, 0])("rejects a truncated body of %i bytes", async (size) => {
    const { invoke } = fixture(4, { fetch: async () => new Response(new Uint8Array(size)) });
    await expect(invoke(["a"])).rejects.toMatchObject({ reason: "length-mismatch" });
  });
  it("rejects a conflicting HTTP length before consuming the body", async () => {
    const cancel = vi.fn();
    const { invoke } = fixture(4, {
      fetch: async () =>
        new Response(new ReadableStream({ cancel }), {
          headers: { "content-length": "999999999" },
        }),
    });
    await expect(invoke(["a"])).rejects.toMatchObject({ reason: "length-mismatch" });
    expect(cancel).toHaveBeenCalledOnce();
  });
  it.each([
    -1,
    NaN,
    Infinity,
    Number.MAX_SAFE_INTEGER + 1,
  ])("rejects an invalid pinned length (%s)", async (length) => {
    const { invoke, fetch } = fixture(length);
    await expect(invoke(["a"])).rejects.toBeInstanceOf(AssetFetchError);
    expect(fetch).not.toHaveBeenCalled();
  });
  it.each([
    "maxAssetBytes",
    "maxTotalAssetBytes",
    "maxConcurrentAssetFetches",
  ] as const)("validates %s", (option) => {
    for (const value of [0, -1, NaN, Infinity, 1.5])
      expect(() => fixture(4, { [option]: value })).toThrow(RangeError);
  });
  it("does not authorize a denied occurrence through a previous successful duplicate", async () => {
    const authorize = vi.fn().mockReturnValueOnce(true).mockReturnValueOnce(false);
    const { invoke } = fixture(4, { authorize, maxConcurrentAssetFetches: 1 });
    await expect(invoke(["a", "a"])).rejects.toThrow("denied");
    expect(authorize).toHaveBeenCalledTimes(2);
  });
  it("aborts active transfers and leaves queued files untouched on admission failure", async () => {
    let signal: AbortSignal | undefined;
    const transfer = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      signal = init?.signal ?? undefined;
      return await new Promise<Response>((_resolve, reject) =>
        signal?.addEventListener("abort", () => reject(signal?.reason), { once: true })
      );
    });
    const { invoke, GET } = fixture(4, {
      fetch: transfer,
      maxTotalAssetBytes: 4,
      maxConcurrentAssetFetches: 2,
    });
    await expect(invoke(["a", "b", "c", "d"])).rejects.toBeInstanceOf(AssetByteLimitError);
    expect(signal?.aborted).toBe(true);
    expect(GET).toHaveBeenCalledTimes(2);
  });
});
