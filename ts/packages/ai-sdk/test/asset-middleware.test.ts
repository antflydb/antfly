import type { LanguageModelV4, LanguageModelV4CallOptions } from "@ai-sdk/provider";
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
      expect(init?.headers).toMatchObject({ "If-Match": resource.version });
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
    const middleware = createAssetMiddleware({ client });
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
    const middleware = createAssetMiddleware({ client });
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
    const middleware = createAssetMiddleware({ client, allowVendorUpload: true, files });
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
    const middleware = createAssetMiddleware({ client, allowVendorUpload: true, files });
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
    const middleware = createAssetMiddleware({ client });
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
    const middleware = createAssetMiddleware({ client });
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
});
