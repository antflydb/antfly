import { AntflyClient } from "@antfly/sdk";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createAssetDisplayHandler } from "../src/asset/display-route.js";
import { encodeAssetReference } from "../src/asset/reference.js";

afterEach(() => vi.unstubAllGlobals());

const reference = { table: "files", key: "k1" };
const resource = {
  type: "source" as const,
  uri: "https://sources.example.com/v1/media/CAPABILITY-SECRET/sig",
  mime_type: "video/mp4",
  version: '"etag-1"',
  length: 4,
};

function stubCatalogAndMedia(mediaResponse: () => Response) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
      if (url.includes("/db/v1/tables/")) {
        return new Response(JSON.stringify({ content: { resource } }), {
          headers: { "Content-Type": "application/json", "x-antfly-version": "1" },
        });
      }
      if (url === resource.uri) return mediaResponse();
      throw new Error(`Unexpected fetch ${init?.method ?? "GET"} ${url}`);
    })
  );
}

describe("createAssetDisplayHandler", () => {
  it("403s when the app's own authorize() refuses", async () => {
    const client = new AntflyClient({ baseUrl: "http://catalog.test" });
    const handler = createAssetDisplayHandler({
      client,
      authorize: () => false,
      referenceId: () => encodeAssetReference(reference),
    });
    const response = await handler(new Request("https://app.example/assets/x"));
    expect(response.status).toBe(403);
  });

  it("404s on an undecodable reference id without touching the catalog", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const client = new AntflyClient({ baseUrl: "http://catalog.test" });
    const handler = createAssetDisplayHandler({
      client,
      authorize: () => true,
      referenceId: () => "not-a-valid-reference",
    });
    const response = await handler(new Request("https://app.example/assets/x"));
    expect(response.status).toBe(404);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("streams the asset through, forwarding Range/If-Match and never leaking the capability URL", async () => {
    stubCatalogAndMedia(
      () =>
        new Response(new Uint8Array([1, 2, 3, 4]), {
          status: 206,
          headers: {
            "content-type": "video/mp4",
            "content-range": "bytes 0-3/4",
            etag: resource.version,
          },
        })
    );
    const client = new AntflyClient({ baseUrl: "http://catalog.test" });
    const handler = createAssetDisplayHandler({
      client,
      authorize: () => true,
      referenceId: () => encodeAssetReference(reference),
    });
    const request = new Request("https://app.example/assets/x", {
      headers: { Range: "bytes=0-3" },
    });
    const response = await handler(request);
    expect(response.status).toBe(206);
    expect(response.headers.get("content-range")).toBe("bytes 0-3/4");
    response.headers.forEach((value) => {
      expect(value).not.toContain("CAPABILITY-SECRET");
    });
    const body = new Uint8Array(await response.arrayBuffer());
    expect([...body]).toEqual([1, 2, 3, 4]);
  });

  it("404s when the catalog row is missing", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("not found", { status: 404 }))
    );
    const client = new AntflyClient({ baseUrl: "http://catalog.test" });
    const handler = createAssetDisplayHandler({
      client,
      authorize: () => true,
      referenceId: () => encodeAssetReference(reference),
    });
    const response = await handler(new Request("https://app.example/assets/x"));
    expect(response.status).toBe(404);
  });
});
