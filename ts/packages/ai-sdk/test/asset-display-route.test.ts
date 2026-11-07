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
    expect(response.headers.get("content-disposition")).toBe("inline");
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

it.each([
  "text/html",
  "image/svg+xml",
  "application/xhtml+xml",
  "application/octet-stream",
])("downloads active or unknown content: %s", async (contentType) => {
  stubCatalogAndMedia(
    () => new Response("<script>alert(1)</script>", { headers: { "content-type": contentType } })
  );
  const handler = createAssetDisplayHandler({
    client: new AntflyClient({ baseUrl: "http://catalog.test" }),
    authorize: () => true,
    referenceId: () => encodeAssetReference(reference),
  });
  const response = await handler(new Request("https://app.example/assets/x"));
  expect(response.headers.get("content-disposition")).toBe("attachment");
  expect(response.headers.get("content-security-policy")).toContain("sandbox");
  expect(response.headers.get("cache-control")).toBe("private, no-store");
});

it("HEAD display uses HEAD upstream and returns no body", async () => {
  let method: string | undefined;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const req = input instanceof Request ? input : new Request(input, init);
      if (req.url.includes("/documents/")) return Response.json({ content: { resource } });
      method = req.method;
      return new Response(method === "HEAD" ? null : "abc");
    })
  );
  const handler = createAssetDisplayHandler({
    client: new AntflyClient({ baseUrl: "http://db.test" }),
    authorize: () => true,
    referenceId: () => encodeAssetReference(reference),
  });
  const response = await handler(new Request("http://app.test/file", { method: "HEAD" }));
  expect(method).toBe("HEAD");
  expect(response.body).toBeNull();
});
it("display cancellation aborts an in-flight catalog lookup", async () => {
  let finish!: (response: Response) => void;
  let signal: AbortSignal | undefined;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const req = input instanceof Request ? input : new Request(input, init);
      if (!req.url.includes("/documents/")) {
        req.signal.throwIfAborted();
        return new Response("abc");
      }
      signal = req.signal;
      return new Promise<Response>((resolve) => {
        finish = resolve;
      });
    })
  );
  const controller = new AbortController();
  const handler = createAssetDisplayHandler({
    client: new AntflyClient({ baseUrl: "http://db.test" }),
    authorize: () => true,
    referenceId: () => encodeAssetReference(reference),
  });
  const pending = handler(new Request("http://app.test/file", { signal: controller.signal })).catch(
    () => undefined
  );
  await vi.waitFor(() => expect(signal).toBeDefined());
  controller.abort();
  const aborted = signal?.aborted;
  finish(Response.json({ content: { resource } }));
  await pending;
  expect(aborted).toBe(true);
});

it.each([
  "GET",
  "HEAD",
])("rejects encoded upstream %s responses and cancels their body", async (method) => {
  const cancel = vi.fn();
  const media = vi.fn(async (_url: unknown, init?: RequestInit) => {
    expect(new Headers(init?.headers).get("accept-encoding")).toBe("identity");
    return new Response(new ReadableStream({ cancel }), {
      status: 206,
      headers: {
        "content-encoding": "gzip",
        "content-length": "49",
        "content-range": "bytes 0-48/49",
      },
    });
  });
  stubCatalogAndMedia(() => new Response());
  const handler = createAssetDisplayHandler({
    client: new AntflyClient({ baseUrl: "http://catalog.test" }),
    authorize: () => true,
    referenceId: () => encodeAssetReference(reference),
    fetch: media as typeof fetch,
  });
  const response = await handler(
    new Request("http://app.test/asset", { method, headers: { Range: "bytes=0-48" } })
  );
  expect(response.status).toBe(502);
  expect(response.headers.has("content-range")).toBe(false);
  expect(response.headers.has("content-length")).toBe(false);
  expect(cancel).toHaveBeenCalledOnce();
  if (method === "HEAD") expect(await response.text()).toBe("");
});
