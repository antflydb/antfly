import type { LanguageModelV4, LanguageModelV4CallOptions, ProviderV4 } from "@ai-sdk/provider";
import { AntflyClient } from "@antfly/sdk";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AssetFetchError, createAssetMiddleware } from "../src/asset/middleware.js";
import { assetProviderReference } from "../src/asset/reference.js";

afterEach(() => vi.unstubAllGlobals());

const reference = { table: "files", key: "k1" };
const resource = {
  type: "source" as const,
  uri: "https://sources.example.com/v1/media/CAPABILITY-SECRET/sig",
  mime_type: "video/mp4",
  version: '"etag-1"',
  length: 4,
};
const videoBytes = new Uint8Array([1, 2, 3, 4]);

function fakeModel(provider: string): LanguageModelV4 {
  return {
    specificationVersion: "v4",
    provider,
    modelId: "test-model",
    supportedUrls: {},
    doGenerate: vi.fn(),
    doStream: vi.fn(),
  };
}

function paramsWithReferencePart(): LanguageModelV4CallOptions {
  return {
    prompt: [
      {
        role: "user",
        content: [
          { type: "text", text: "describe this" },
          {
            type: "file",
            mediaType: "video/mp4",
            data: { type: "reference", reference: assetProviderReference(reference) },
          },
        ],
      },
    ],
  };
}

function stubCatalogAndMediaFetch(): ReturnType<typeof vi.fn> {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    if (url.includes("/db/v1/tables/")) {
      return new Response(JSON.stringify({ content: { resource } }), {
        headers: { "Content-Type": "application/json", "x-antfly-version": "1" },
      });
    }
    if (url === resource.uri) {
      expect(new Headers(init?.headers).get("if-match")).toBe(resource.version);
      expect(new Headers(init?.headers).get("accept-encoding")).toBe("identity");
      return new Response(videoBytes);
    }
    throw new Error(`Unexpected fetch: ${url}`);
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

describe("createAssetMiddleware", () => {
  it("leaves the prompt untouched for the antfly provider itself", async () => {
    const client = new AntflyClient({ baseUrl: "http://catalog.test" });
    const middleware = createAssetMiddleware({ authorize: () => true, client });
    const params = paramsWithReferencePart();
    const result = await middleware.transformParams?.({
      type: "generate",
      params,
      model: fakeModel("antfly"),
    });
    expect(result).toBe(params);
  });

  it("fetches the asset and inlines it for an external model by default (no vendor upload)", async () => {
    stubCatalogAndMediaFetch();
    const client = new AntflyClient({ baseUrl: "http://catalog.test" });
    const middleware = createAssetMiddleware({ authorize: () => true, client });
    const params = paramsWithReferencePart();
    const result = await middleware.transformParams?.({
      type: "generate",
      params,
      model: fakeModel("openai"),
    });
    const content = result?.prompt[0];
    if (content?.role !== "user") throw new Error("expected user message");
    const filePart = content.content[1];
    if (filePart.type !== "file") throw new Error("expected file part");
    expect(filePart.data).toEqual({ type: "data", data: videoBytes });
    // The capability URL must never appear anywhere in the rewritten params.
    expect(JSON.stringify(result)).not.toContain(resource.uri);
    expect(JSON.stringify(result)).not.toContain("CAPABILITY-SECRET");
  });

  it("uploads to the vendor's files API once per digest when opted in, respecting expiresAt", async () => {
    stubCatalogAndMediaFetch();
    const client = new AntflyClient({ baseUrl: "http://catalog.test" });
    const uploadFile = vi.fn().mockResolvedValue({
      providerReference: { openai: "file-abc" },
      expiresAt: new Date(Date.now() + 60_000),
      warnings: [],
    });
    const files = { specificationVersion: "v4" as const, provider: "openai", uploadFile };
    const middleware = createAssetMiddleware({
      authorize: () => true,
      client,
      allowVendorUpload: true,
      files,
    });
    const params = paramsWithReferencePart();

    const first = await middleware.transformParams?.({
      type: "generate",
      params,
      model: fakeModel("openai"),
    });
    const firstPart = (
      first?.prompt[0] as { role: "user"; content: { type: string; data?: unknown }[] }
    ).content[1];
    expect(firstPart.data).toEqual({
      type: "reference",
      reference: { antfly: expect.any(String), openai: "file-abc" },
    });
    expect(uploadFile).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(first)).not.toContain(resource.uri);

    // A second call for the same digest and provider reuses the cache.
    await middleware.transformParams?.({
      type: "generate",
      params: paramsWithReferencePart(),
      model: fakeModel("openai"),
    });
    expect(uploadFile).toHaveBeenCalledTimes(1);
  });

  it("re-uploads once the cached reference has expired", async () => {
    stubCatalogAndMediaFetch();
    const client = new AntflyClient({ baseUrl: "http://catalog.test" });
    const uploadFile = vi.fn().mockResolvedValue({
      providerReference: { openai: "file-abc" },
      expiresAt: new Date(Date.now() - 1),
      warnings: [],
    });
    const files = { specificationVersion: "v4" as const, provider: "openai", uploadFile };
    const middleware = createAssetMiddleware({
      authorize: () => true,
      client,
      allowVendorUpload: true,
      files,
    });
    await middleware.transformParams?.({
      type: "generate",
      params: paramsWithReferencePart(),
      model: fakeModel("openai"),
    });
    await middleware.transformParams?.({
      type: "generate",
      params: paramsWithReferencePart(),
      model: fakeModel("openai"),
    });
    expect(uploadFile).toHaveBeenCalledTimes(2);
  });

  it("throws AssetFetchError (not a generic error swallowing the status) on a version mismatch", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url =
          typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
        if (url.includes("/db/v1/tables/")) {
          return new Response(JSON.stringify({ content: { resource } }), {
            headers: { "Content-Type": "application/json", "x-antfly-version": "1" },
          });
        }
        return new Response("stale", { status: 412 });
      })
    );
    const client = new AntflyClient({ baseUrl: "http://catalog.test" });
    const middleware = createAssetMiddleware({ authorize: () => true, client });
    await expect(
      middleware.transformParams?.({
        type: "generate",
        params: paramsWithReferencePart(),
        model: fakeModel("openai"),
      })
    ).rejects.toBeInstanceOf(AssetFetchError);
  });

  it("never includes the capability URL in a thrown error's message", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url =
          typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
        if (url.includes("/db/v1/tables/")) {
          return new Response(JSON.stringify({ content: { resource } }), {
            headers: { "Content-Type": "application/json", "x-antfly-version": "1" },
          });
        }
        return new Response("gone", { status: 410 });
      })
    );
    const client = new AntflyClient({ baseUrl: "http://catalog.test" });
    const middleware = createAssetMiddleware({ authorize: () => true, client });
    try {
      await middleware.transformParams?.({
        type: "generate",
        params: paramsWithReferencePart(),
        model: fakeModel("openai"),
      });
      throw new Error("expected AssetFetchError");
    } catch (error) {
      expect(error).toBeInstanceOf(AssetFetchError);
      expect((error as Error).message).not.toContain(resource.uri);
    }
  });

  it("refuses a reference the app doesn't authorize before reading the catalog", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const middleware = createAssetMiddleware({
      client: new AntflyClient({ baseUrl: "http://db.test" }),
      authorize: () => false,
    });
    await expect(
      middleware.transformParams?.({
        type: "generate",
        params: paramsWithReferencePart(),
        model: {} as never,
      })
    ).rejects.toThrow("denied");
    expect(fetchMock).not.toHaveBeenCalled();
  });
});

it("coalesces concurrent references across middleware instances without coupling caller cancellation", async () => {
  const fetchMock = stubCatalogAndMediaFetch();
  const { createInMemoryAssetUploadCache } = await import("../src/asset/middleware.js");
  const cache = createInMemoryAssetUploadCache();
  let finish!: (value: { providerReference: Record<string, string>; warnings: [] }) => void;
  const uploadFile = vi.fn(
    (_options: { abortSignal?: AbortSignal }) =>
      new Promise<{ providerReference: Record<string, string>; warnings: [] }>((resolve) => {
        finish = resolve;
      })
  );
  const authorize = vi.fn(() => true);
  const options = {
    client: new AntflyClient({ baseUrl: "http://catalog.test" }),
    authorize,
    cache,
    allowVendorUpload: true,
    files: { specificationVersion: "v4" as const, provider: "openai", uploadFile },
  };
  const first = createAssetMiddleware(options);
  const second = createAssetMiddleware(options);
  const controller = new AbortController();
  const invoke = (middleware: typeof first, signal?: AbortSignal) =>
    middleware.transformParams?.({
      type: "generate",
      params: { ...paramsWithReferencePart(), abortSignal: signal },
      model: fakeModel("openai"),
    });
  const cancelled = Promise.resolve(invoke(first, controller.signal));
  const reason = new Error("caller cancelled");
  const rejected = expect(cancelled).rejects.toBe(reason);
  const surviving = invoke(second);
  await vi.waitFor(() => expect(uploadFile).toHaveBeenCalledTimes(1));
  controller.abort(reason);
  await rejected;
  const sharedSignal = uploadFile.mock.calls[0]?.[0]?.abortSignal;
  expect(sharedSignal?.aborted).toBe(false);
  finish({ providerReference: { openai: "shared-file" }, warnings: [] });
  await surviving;
  expect(authorize).toHaveBeenCalledTimes(2);
  expect(fetchMock.mock.calls.filter(([url]) => url === resource.uri)).toHaveLength(1);
  await invoke(first);
  expect(uploadFile).toHaveBeenCalledTimes(1);
  expect(authorize).toHaveBeenCalledTimes(3);
});

it("bounds the cache with LRU eviction and drops expired references", async () => {
  const { createInMemoryAssetUploadCache } = await import("../src/asset/middleware.js");
  const cache = createInMemoryAssetUploadCache(2);
  const entry = { reference: { openai: "file" } };
  cache.set("a", "openai", entry);
  cache.set("b", "openai", entry);
  expect(cache.get("a", "openai")).toBe(entry);
  cache.set("c", "openai", entry);
  expect(cache.get("b", "openai")).toBeUndefined();
  expect(cache.get("a", "openai")).toBe(entry);
  cache.set("a", "openai", { ...entry, expiresAt: new Date(0) });
  expect(cache.get("a", "openai")).toBeUndefined();
  expect(() => createInMemoryAssetUploadCache(0)).toThrow(RangeError);
});

it("aborts an abandoned upload and clears cancelled or failed flights for retry", async () => {
  stubCatalogAndMediaFetch();
  let sharedSignal: AbortSignal | undefined;
  const uploadFile = vi.fn(async (options: { abortSignal?: AbortSignal }) => {
    sharedSignal = options.abortSignal;
    if (uploadFile.mock.calls.length === 1) {
      return await new Promise<{ providerReference: Record<string, string>; warnings: [] }>(
        (_resolve, reject) => {
          options.abortSignal?.addEventListener(
            "abort",
            () => reject(options.abortSignal?.reason),
            { once: true }
          );
        }
      );
    }
    if (uploadFile.mock.calls.length === 2) throw new Error("vendor unavailable");
    return { providerReference: { openai: "retried-file" }, warnings: [] };
  });
  const middleware = createAssetMiddleware({
    client: new AntflyClient({ baseUrl: "http://catalog.test" }),
    authorize: () => true,
    allowVendorUpload: true,
    files: { specificationVersion: "v4", provider: "openai", uploadFile },
  });
  const invoke = (signal?: AbortSignal) =>
    Promise.resolve(
      middleware.transformParams?.({
        type: "generate",
        params: { ...paramsWithReferencePart(), abortSignal: signal },
        model: fakeModel("openai"),
      })
    );
  const caller = new AbortController();
  const pending = invoke(caller.signal);
  const rejected = expect(pending).rejects.toThrow("cancelled");
  await vi.waitFor(() => expect(uploadFile).toHaveBeenCalledTimes(1));
  caller.abort(new Error("cancelled"));
  await rejected;
  await vi.waitFor(() => expect(sharedSignal?.aborted).toBe(true));
  await expect(invoke()).rejects.toThrow("vendor unavailable");
  await invoke();
  expect(uploadFile).toHaveBeenCalledTimes(3);
});

it.each([
  false,
  true,
])("uses catalog MIME for resolved bytes and vendor references (upload=%s)", async (allowVendorUpload) => {
  stubCatalogAndMediaFetch();
  const upload = vi
    .fn()
    .mockResolvedValue({ providerReference: { openai: "file-video" }, warnings: [] });
  const middleware = createAssetMiddleware({
    authorize: () => true,
    client: new AntflyClient({ baseUrl: "http://catalog.test" }),
    allowVendorUpload,
    files: { specificationVersion: "v4", provider: "openai", uploadFile: upload },
  });
  const params = paramsWithReferencePart();
  const message = params.prompt[0];
  if (message.role !== "user") throw new Error("Expected user message");
  const file = message.content[1];
  if (file.type !== "file") throw new Error("Expected file");
  file.mediaType = "image/png";
  const result = await middleware.transformParams?.({
    type: "generate",
    params,
    model: fakeModel("openai"),
  });
  const transformed = result?.prompt[0];
  if (transformed?.role !== "user") throw new Error("Expected user message");
  const part = transformed.content[1];
  if (part.type !== "file") throw new Error("Expected file");
  expect(part.mediaType).toBe(resource.mime_type);
  expect(file.mediaType).toBe("image/png");
  if (allowVendorUpload) {
    expect(part.data.type).toBe("reference");
    expect(upload.mock.calls[0][0].mediaType).toBe(resource.mime_type);
  } else expect(part.data).toEqual({ type: "data", data: videoBytes });
});

it("inlines assets when the supplied provider has no optional files API", async () => {
  stubCatalogAndMediaFetch();
  const provider: ProviderV4 = {
    specificationVersion: "v4",
    languageModel: vi.fn(),
    embeddingModel: vi.fn(),
    imageModel: vi.fn(),
  };
  const middleware = createAssetMiddleware({
    client: new AntflyClient({ baseUrl: "http://catalog.test" }),
    authorize: () => true,
    allowVendorUpload: true,
    files: provider,
  });
  const result = await middleware.transformParams?.({
    type: "generate",
    params: paramsWithReferencePart(),
    model: fakeModel("external"),
  });
  expect(result?.prompt[0]).toMatchObject({
    content: [{ type: "text" }, { type: "file", data: { type: "data", data: videoBytes } }],
  });
  expect(JSON.stringify(result)).not.toContain(resource.uri);
});

it("resolves provider files lazily on cache misses and preserves the factory receiver", async () => {
  const fetch = stubCatalogAndMediaFetch();
  const uploadFile = vi
    .fn()
    .mockResolvedValue({ providerReference: { vendor: "file-1" }, warnings: [] });
  const api = { specificationVersion: "v4" as const, provider: "vendor", uploadFile };
  const provider = {
    specificationVersion: "v4" as const,
    languageModel: vi.fn(),
    embeddingModel: vi.fn(),
    imageModel: vi.fn(),
    files: vi.fn(function (this: ProviderV4) {
      expect(this).toBe(provider);
      return api;
    }),
  } satisfies ProviderV4;
  const authorize = vi.fn(() => true);
  const middleware = createAssetMiddleware({
    client: new AntflyClient({ baseUrl: "http://catalog.test" }),
    authorize,
    allowVendorUpload: true,
    files: provider,
  });
  expect(provider.files).not.toHaveBeenCalled();
  for (let call = 0; call < 2; call++) {
    await middleware.transformParams?.({
      type: "generate",
      params: paramsWithReferencePart(),
      model: fakeModel("vendor"),
    });
  }
  expect(authorize).toHaveBeenCalledTimes(2);
  expect(provider.files).toHaveBeenCalledOnce();
  expect(uploadFile).toHaveBeenCalledOnce();
  expect(fetch.mock.calls.filter(([input]) => input === resource.uri)).toHaveLength(1);
});

it.each([
  "factory",
  "upload",
] as const)("preserves genuine %s failures without an inline fallback", async (phase) => {
  const fetch = stubCatalogAndMediaFetch();
  const failure = new Error("vendor operation failed");
  const uploadFile = vi.fn().mockRejectedValue(failure);
  const provider: ProviderV4 = {
    specificationVersion: "v4",
    languageModel: vi.fn(),
    embeddingModel: vi.fn(),
    imageModel: vi.fn(),
    files() {
      if (phase === "factory") throw failure;
      return { specificationVersion: "v4", provider: "vendor", uploadFile };
    },
  };
  const middleware = createAssetMiddleware({
    client: new AntflyClient({ baseUrl: "http://catalog.test" }),
    authorize: () => true,
    allowVendorUpload: true,
    files: provider,
  });
  await expect(
    middleware.transformParams?.({
      type: "generate",
      params: paramsWithReferencePart(),
      model: fakeModel("vendor"),
    })
  ).rejects.toBe(failure);
  expect(fetch.mock.calls.filter(([input]) => input === resource.uri)).toHaveLength(
    phase === "factory" ? 0 : 1
  );
});

it("does not resolve vendor file capabilities before asset authorization", async () => {
  const fetch = stubCatalogAndMediaFetch();
  const files = vi.fn();
  const provider: ProviderV4 = {
    specificationVersion: "v4",
    languageModel: vi.fn(),
    embeddingModel: vi.fn(),
    imageModel: vi.fn(),
    files,
  };
  const middleware = createAssetMiddleware({
    client: new AntflyClient({ baseUrl: "http://catalog.test" }),
    authorize: () => false,
    allowVendorUpload: true,
    files: provider,
  });
  await expect(
    middleware.transformParams?.({
      type: "generate",
      params: paramsWithReferencePart(),
      model: fakeModel("vendor"),
    })
  ).rejects.toThrow(/denied/);
  expect(files).not.toHaveBeenCalled();
  expect(fetch).not.toHaveBeenCalled();
});
