import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AntflyClient } from "@antfly/sdk";
import { StandaloneSourcesClient } from "@antfly/sdk/sources";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SourceSyncConflictError, SourceSyncFailedError, uploadFile } from "../src/asset/upload.js";

afterEach(() => vi.unstubAllGlobals());

let localRoot: string;
beforeEach(async () => {
  localRoot = await mkdtemp(join(tmpdir(), "aisdk-asset-upload-"));
});
afterEach(async () => {
  await rm(localRoot, { recursive: true, force: true });
});

const sourceId = "11111111-1111-1111-1111-111111111111";
const data = new TextEncoder().encode("fake mp4 bytes");
const digest = createHash("sha256").update(data).digest("hex");

function stubFetch(handlers: {
  sync?: () => Response;
  job?: () => Response;
  query?: () => Response;
  lookup?: () => Response;
  namespace?: string;
  prefix?: string;
}) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
      if (url.endsWith("/v1/profile"))
        return new Response(
          JSON.stringify({
            version: 1,
            connections: { local: { provider: "local" } },
            sources: [
              {
                id: sourceId,
                connection: "local",
                table: "files",
                publication: "media",
                config: { prefix: handlers.prefix ?? "" },
                identity: handlers.namespace ? { namespace: handlers.namespace } : undefined,
              },
            ],
          })
        );
      if (url.includes("/documents/")) return (handlers.lookup ?? defaultLookup)();
      if (url.endsWith("/v1/sync")) return (handlers.sync ?? defaultSync)();
      if (url.includes("/v1/jobs/")) return (handlers.job ?? defaultJob)();
      if (url.includes("/db/v1/tables/") && url.includes("/query"))
        return (handlers.query ?? defaultQuery)();
      throw new Error(`Unexpected fetch ${init?.method ?? "GET"} ${url}`);
    })
  );
}
const defaultSync = () =>
  new Response(JSON.stringify({ version: 1, id: "job-1", state: "running", started_at: 1 }), {
    status: 202,
  });
const defaultJob = () =>
  new Response(JSON.stringify({ version: 1, id: "job-1", state: "completed", started_at: 1 }));
const defaultLookup = () =>
  new Response(
    JSON.stringify({
      source: { id: sourceId, relative_key: `objects/sha256/${digest}.mp4` },
      metadata: { sha256: digest },
      content: { resource: { type: "source", version: `"${digest}"` } },
    })
  );
const defaultQuery = () => new Response(JSON.stringify({ responses: [{ hits: { hits: [] } }] }));
function expectedKey(namespace?: string, prefix = "") {
  const relative = `objects/sha256/${digest}.mp4`;
  const identity = namespace
    ? `${namespace}/${relative}`
    : `local://job/${sourceId}/${prefix ? `${prefix}/` : ""}${relative}`;
  return `source://job/${sourceId}/records/${createHash("sha256").update(identity).digest("hex")}`;
}

function newClients() {
  const client = new AntflyClient({ baseUrl: "http://db.test" });
  const sources = new StandaloneSourcesClient({
    baseUrl: "https://sources.test",
    token: "operator",
  });
  return { client, sources };
}

describe("uploadFile", () => {
  it("writes the content-addressed object and waits for the catalog row (published)", async () => {
    stubFetch({ prefix: "uploads" });
    const { client, sources } = newClients();
    const result = await uploadFile({
      client,
      sources,
      sourceId: "11111111-1111-1111-1111-111111111111",
      localRoot,
      prefix: "uploads",
      table: "files",
      data,
      mimeType: "video/mp4",
    });
    expect(result.digest).toBe(digest);
    expect(result.state).toBe("published");
    expect(result.reference).toEqual({ table: "files", key: expectedKey(undefined, "uploads") });
    const written = await readFile(
      join(localRoot, "uploads", "objects", "sha256", `${digest}.mp4`)
    );
    expect(written.toString()).toBe("fake mp4 bytes");
  });

  it("retries a 409 sync conflict and then throws SourceSyncConflictError if it never clears", async () => {
    let calls = 0;
    stubFetch({
      sync: () => {
        calls++;
        return new Response(JSON.stringify({ version: 1, code: "sync_in_progress" }), {
          status: 409,
        });
      },
    });
    const { client, sources } = newClients();
    await expect(
      uploadFile({
        client,
        sources,
        sourceId: "11111111-1111-1111-1111-111111111111",
        localRoot,
        table: "files",
        data,
        mimeType: "video/mp4",
        maxSyncConflictRetries: 2,
        syncConflictRetryDelayMs: 1,
      })
    ).rejects.toBeInstanceOf(SourceSyncConflictError);
    expect(calls).toBe(3);
  });

  it("throws SourceSyncFailedError when the sync job fails", async () => {
    stubFetch({
      job: () =>
        new Response(
          JSON.stringify({ version: 1, id: "job-1", state: "failed", started_at: 1, code: "boom" })
        ),
    });
    const { client, sources } = newClients();
    await expect(
      uploadFile({
        client,
        sources,
        sourceId: "11111111-1111-1111-1111-111111111111",
        localRoot,
        table: "files",
        data,
        mimeType: "video/mp4",
      })
    ).rejects.toBeInstanceOf(SourceSyncFailedError);
  });
});

// No unfiltered table query is permitted: identity lookup scales independently
// of table cardinality and cannot match another source's digest-bearing text.
it.each([
  undefined,
  "stable-namespace",
])("looks up the exact upload identity (%s)", async (namespace) => {
  stubFetch({
    namespace,
    query: () => {
      throw new Error("unexpected table scan");
    },
  });
  const { client, sources } = newClients();
  const result = await uploadFile({
    client,
    sources,
    sourceId,
    localRoot,
    table: "files",
    data,
    mimeType: "video/mp4",
  });
  expect(result.reference.key).toBe(expectedKey(namespace));
  const calls = vi.mocked(fetch).mock.calls;
  const lookup = calls.find(([input]) =>
    (input instanceof Request ? input.url : String(input)).includes("/documents/")
  );
  const url = new URL(lookup?.[0] instanceof Request ? lookup[0].url : String(lookup?.[0]));
  expect(decodeURIComponent(url.pathname)).toContain(expectedKey(namespace));
  expect(url.searchParams.get("consistency")).toBe("read_index");
});

it("does not accept a digest on another source's row", async () => {
  stubFetch({
    lookup: () =>
      new Response(JSON.stringify({ source: { id: "other" }, metadata: { sha256: digest } })),
  });
  const { client, sources } = newClients();
  await expect(
    uploadFile({
      client,
      sources,
      sourceId,
      localRoot,
      table: "files",
      data,
      mimeType: "video/mp4",
      timeoutMs: 10,
      pollIntervalMs: 1,
    })
  ).rejects.toThrow("Timed out");
});

it("restricts searchable polling to the uploaded record", async () => {
  stubFetch({
    query: () =>
      new Response(JSON.stringify({ responses: [{ hits: { hits: [{ _id: expectedKey() }] } }] })),
  });
  const { client, sources } = newClients();
  await uploadFile({
    client,
    sources,
    sourceId,
    localRoot,
    table: "files",
    data,
    mimeType: "video/mp4",
    waitFor: "searchable",
  });
  const call = vi
    .mocked(fetch)
    .mock.calls.find(([input]) =>
      (input instanceof Request ? input.url : String(input)).includes("/query")
    );
  const request = call?.[0] as Request;
  expect((await request.clone().json()).filter_query).toEqual({ ids: [expectedKey()] });
});
