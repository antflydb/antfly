import { AntflyClient } from "@antfly/sdk";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  AssetMissingResourceError,
  AssetNotFoundError,
  resolveCatalogResource,
} from "../src/asset/catalog.js";

afterEach(() => vi.unstubAllGlobals());

const resource = {
  type: "source" as const,
  uri: "https://sources.example.com/v1/media/abc/def",
  mime_type: "video/mp4",
  version: '"etag-1"',
  version_kind: "strong_etag" as const,
  length: 12345,
};

describe("resolveCatalogResource", () => {
  it("reads content.resource from the catalog row", async () => {
    let captured: Request | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (request: Request) => {
        captured = request.clone();
        return new Response(JSON.stringify({ content: { resource } }), {
          headers: { "Content-Type": "application/json", "x-antfly-version": "1" },
        });
      })
    );
    const client = new AntflyClient({ baseUrl: "http://catalog.test" });
    const result = await resolveCatalogResource(client, { table: "files", key: "k1" });
    expect(result).toEqual(resource);
    expect(captured?.url).toContain("/db/v1/tables/files/documents/k1");
  });

  it("throws AssetNotFoundError on 404 instead of a generic error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("not found", { status: 404 }))
    );
    const client = new AntflyClient({ baseUrl: "http://catalog.test" });
    await expect(
      resolveCatalogResource(client, { table: "files", key: "missing" })
    ).rejects.toBeInstanceOf(AssetNotFoundError);
  });

  it("throws AssetMissingResourceError when the row has no content.resource", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(JSON.stringify({ content: { text: "hello" } }), {
            headers: { "Content-Type": "application/json", "x-antfly-version": "1" },
          })
      )
    );
    const client = new AntflyClient({ baseUrl: "http://catalog.test" });
    await expect(
      resolveCatalogResource(client, { table: "files", key: "text-only" })
    ).rejects.toBeInstanceOf(AssetMissingResourceError);
  });
});
