import { AntflyClient, InferenceClient } from "@antfly/sdk";
import { Document } from "@langchain/core/documents";
import { afterEach, expect, it, vi } from "vitest";
import {
  AntflyByteStore,
  AntflyDocumentStore,
  AntflyEmbeddings,
  AntflyGraphStore,
  AntflyJSONStore,
  WriteOutcomeError,
} from "../src/index.js";

const client = () => new AntflyClient({ baseUrl: "http://antfly.test" });
const json = (value: unknown, status = 200) =>
  new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
afterEach(() => vi.unstubAllGlobals());
it("validates every value before writing and bounds byte codecs", async () => {
  const fetch = vi.fn();
  vi.stubGlobal("fetch", fetch);
  const store = new AntflyJSONStore({ client: client(), table: "s", batchSize: 1 });
  await expect(
    store.mset([
      ["ok", {}],
      ["bad", { n: NaN }],
    ])
  ).rejects.toThrow("JSON");
  await expect(
    store.mset([
      ["same", 1],
      ["same", 2],
    ])
  ).rejects.toThrow("Duplicate");
  const bytes = new AntflyByteStore({ client: client(), table: "s", maxValueBytes: 2 });
  await expect(bytes.mset([["large", new Uint8Array(3)]])).rejects.toThrow("maxValueBytes");
  expect(fetch).not.toHaveBeenCalled();
});
it("rejects wrong codecs and corrupt base64", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => json({ codec: { version: 1, kind: "bytes" }, key: "x", value: "%%%" }))
  );
  await expect(new AntflyByteStore({ client: client(), table: "s" }).mget(["x"])).rejects.toThrow(
    "base64"
  );
  await expect(
    new AntflyDocumentStore({ client: client(), table: "s" }).mget(["x"])
  ).rejects.toThrow("codec");
});
it("distinguishes absent records from absent tables", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => json({ error: "missing" }, 404))
  );
  await expect(new AntflyJSONStore({ client: client(), table: "s" }).mget(["x"])).rejects.toThrow();
});
it("reports earlier committed batches without retrying uncertain writes", async () => {
  let batches = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => {
      if (++batches === 2) throw new Error("lost response");
      return json({ status: "committed" });
    })
  );
  try {
    await new AntflyJSONStore({ client: client(), table: "s", batchSize: 1 }).mset([
      ["a", 1],
      ["b", 2],
    ]);
    throw new Error("expected failure");
  } catch (error) {
    expect(error).toBeInstanceOf(WriteOutcomeError);
    expect((error as WriteOutcomeError).committedKeys).toHaveLength(1);
    expect((error as WriteOutcomeError).status).toBe("unknown");
  }
  expect(batches).toBe(2);
});
it("rejects nonadvancing key enumeration rather than truncating", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => json({ responses: [{ hits: { hits: [{ _id: "lc//61" }] } }] }))
  );
  const generator = new AntflyJSONStore({ client: client(), table: "s", pageSize: 1 }).yieldKeys();
  expect((await generator.next()).value).toBe("a");
  await expect(generator.next()).rejects.toThrow("cursor");
});
it("subclass creation returns the actual document store", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => json({}))
  );
  expect(await AntflyDocumentStore.create({ client: client(), table: "s" })).toBeInstanceOf(
    AntflyDocumentStore
  );
});
it("refuses graph scope overrides before transport", async () => {
  const fetch = vi.fn();
  vi.stubGlobal("fetch", fetch);
  const graph = new AntflyGraphStore({ client: client(), table: "s", namespace: "a" });
  await expect(
    graph.queryGraph({ x: { traverse: { start: { keys: ["foreign"] } } } })
  ).rejects.toThrow("namespace");
  await expect(
    graph.queryGraph({ x: { index: "other", traverse: { start: { keys: [] } } } })
  ).rejects.toThrow("fixed");
  await expect(graph.query("MATCH (n) RETURN n")).rejects.toThrow();
  expect(fetch).not.toHaveBeenCalled();
});
it("rejects a traversal that leaves scope at an intermediate hop", async () => {
  const graph = new AntflyGraphStore({ client: client(), table: "s", namespace: "a" });
  const key = graph.nodeKey("Person", "a");
  vi.stubGlobal(
    "fetch",
    vi.fn(async () =>
      json({
        responses: [
          {
            graph_results: {
              x: {
                kind: "nodes",
                nodes: [
                  {
                    table: "s",
                    key,
                    depth: 2,
                    path: [
                      { table: "s", key },
                      { table: "other", key },
                      { table: "s", key },
                    ],
                    path_edges: [
                      { from: { table: "s", key }, to: { table: "other", key } },
                      { from: { table: "other", key }, to: { table: "s", key } },
                    ],
                  },
                ],
              },
            },
          },
        ],
      })
    )
  );
  await expect(graph.queryGraph({ x: { traverse: { start: { keys: [key] } } } })).rejects.toThrow();
});
it.each([
  true,
  false,
  undefined,
])("sends one multimodal item per request with truncate=%s", async (truncate) => {
  const calls: unknown[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const req = input instanceof Request ? input : new Request(input, init);
      calls.push(await req.json());
      return json({ data: [{ index: 0, embedding: [1, 0] }] });
    })
  );
  const embeddings = new AntflyEmbeddings({
    client: new InferenceClient({ baseUrl: "http://antfly.test" }),
    model: "explicit-vision",
    truncate,
  });
  expect(
    await embeddings.embedMultimodal([
      [
        { type: "text", text: "caption" },
        { type: "image_url", image_url: { url: "data:image/png;base64,AA==" } },
      ],
      [{ type: "text", text: "another" }],
    ])
  ).toEqual([
    [1, 0],
    [1, 0],
  ]);
  expect(calls).toHaveLength(2);
  for (const call of calls) {
    if (truncate === undefined) expect(call).not.toHaveProperty("truncate");
    else expect(call).toHaveProperty("truncate", truncate);
  }
});
it("does not accept a committed graph response without table outcomes", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const req = input instanceof Request ? input : new Request(input, init);
      if (req.url.endsWith("/transactions/commit")) return json({ status: "committed" });
      if (req.url.includes("/documents/")) return json({ error: "missing" }, 404);
      return json({});
    })
  );
  const graph = new AntflyGraphStore({ client: client(), table: "s" });
  await expect(
    graph.addGraphDocuments([
      {
        nodes: [{ id: "a", type: "Person" }],
        relationships: [],
        source: new Document({ id: "s", pageContent: "source" }),
      },
    ])
  ).rejects.toBeInstanceOf(WriteOutcomeError);
});
