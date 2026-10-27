import { AntflyClient, AntflyHTTPError, InferenceClient } from "@antfly/sdk";
import { ContextualCompressionRetriever } from "@langchain/classic/retrievers/contextual_compression";
import { Document } from "@langchain/core/documents";
import { FakeEmbeddings } from "@langchain/core/utils/testing";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  AntflyEmbeddings,
  AntflyReranker,
  AntflyRetriever,
  AntflyVectorStore,
  WriteOutcomeError,
} from "../src/index.js";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const client = () =>
  new AntflyClient({ baseUrl: "http://antfly.test", headers: { Authorization: "Bearer test" } });
const inference = () =>
  new InferenceClient({ baseUrl: "http://antfly.test", headers: { Authorization: "Bearer test" } });
const embeddings = {
  embedDocuments: vi.fn(async (texts: string[]) => texts.map(() => [1, 0])),
  embedQuery: vi.fn(async () => [1, 0]),
};
const doc = (id: string, pageContent = id) =>
  new Document({ id, pageContent, metadata: { source: "manual" } });
let calls: { path: string; method: string; body: Record<string, unknown> }[];
let tableExists: boolean;
let indexConfig: Record<string, unknown> | undefined;
let documents: Map<string, Record<string, unknown>>;
let batchStatus: string | undefined;
let resultHits: Record<string, unknown>[] | undefined;
let rerankData: { index: number; score: number }[];
let failBatch: number;
let batches: number;
beforeEach(() => {
  calls = [];
  tableExists = true;
  indexConfig = { type: "embeddings", external: true, dimension: 2, distance_metric: "cosine" };
  documents = new Map();
  batchStatus = "committed";
  resultHits = undefined;
  rerankData = [];
  failBatch = 0;
  batches = 0;
  vi.clearAllMocks();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(input, init);
      const path = new URL(request.url).pathname;
      const method = request.method;
      const body = method === "POST" ? JSON.parse((await request.text()) || "{}") : {};
      calls.push({ path, method, body });
      expect(request.headers.get("Authorization")).toBe("Bearer test");
      if (path.endsWith("/ai/v1/embed"))
        return json({
          data: body.input.map((_: string, index: number) => ({ index, embedding: [1, 0] })),
        });
      if (path.endsWith("/ai/v1/rerank")) return json({ data: rerankData });
      if (path.endsWith("/indexes/vectors")) {
        if (method === "POST") {
          indexConfig = body;
          return json(body);
        }
        return indexConfig ? json({ config: indexConfig }) : json({ error: "missing index" }, 404);
      }
      if (path.endsWith("/batch")) {
        batches++;
        if (batches === failBatch) throw new TypeError("connection lost after send");
        for (const [key, value] of Object.entries(body.inserts ?? {}))
          documents.set(key, value as Record<string, unknown>);
        for (const key of body.deletes ?? []) documents.delete(key);
        return json({ status: batchStatus });
      }
      if (path.endsWith("/query"))
        return json({
          responses: [
            {
              status: 200,
              hits: {
                hits:
                  resultHits ??
                  [...documents].map(([id, source], i) => ({
                    _id: id,
                    _source: source,
                    _distance: i * 0.2,
                    _score: 1 - i * 0.2,
                  })),
              },
            },
          ],
        });
      if (path.includes("/documents/")) {
        const key = decodeURIComponent(path.split("/documents/")[1]);
        return documents.has(key)
          ? json(documents.get(key))
          : json({ error: "missing document" }, 404);
      }
      if (method === "POST") {
        tableExists = true;
        return json({ name: "test" });
      }
      return tableExists ? json({ name: "test" }) : json({ error: "missing table" }, 404);
    })
  );
});
afterEach(() => vi.unstubAllGlobals());
const store = (extra = {}) =>
  new AntflyVectorStore(embeddings, { client: client(), table: "test", ...extra });
it("attaches without side effects", () => {
  store();
  expect(calls).toEqual([]);
});
it("provisions factories and waits for index visibility", async () => {
  tableExists = false;
  indexConfig = undefined;
  const s = await AntflyVectorStore.fromDocuments([doc("owner")], embeddings, {
    client: client(),
    table: "test",
  });
  expect(indexConfig).toMatchObject({ external: true, dimension: 2, distance_metric: "cosine" });
  expect(calls.find((c) => c.path.endsWith("/batch"))?.body.sync_level).toBe("full_index");
  expect((await s.asRetriever(1).invoke("owner"))[0].id).toBe("owner");
});
it("reuses resources with unchanged third-party embeddings", async () => {
  indexConfig = { ...indexConfig, dimension: 4 };
  await AntflyVectorStore.fromTexts(["hello"], [{ topic: "test" }], new FakeEmbeddings(), {
    client: client(),
    table: "test",
    ids: ["hello"],
  });
  expect(calls.filter((c) => c.method === "POST" && !c.path.endsWith("/batch"))).toHaveLength(0);
});
it("refuses missing attached index before embedding", async () => {
  indexConfig = undefined;
  await expect(store().addDocuments([doc("x")])).rejects.toBeInstanceOf(AntflyHTTPError);
  expect(embeddings.embedDocuments).not.toHaveBeenCalled();
});
it.each([
  { external: false },
  { sparse: true },
  { dimension: 3 },
])("rejects incompatible indexes %j", async (config) => {
  indexConfig = { ...indexConfig, ...config };
  await expect(store({ dimensions: 2 }).validate()).rejects.toThrow();
  expect(calls.every((c) => c.method === "GET")).toBe(true);
});
it("upserts stable explicit IDs", async () => {
  const s = store();
  await s.addDocuments([doc("old")], { ids: ["new"] });
  await s.addDocuments([doc("new", "updated")]);
  expect(documents.size).toBe(1);
  expect(documents.get("new")?.text).toBe("updated");
});
it("rejects malformed vectors and metadata before writes", async () => {
  await expect(store().addVectors([[NaN, 0]], [doc("a")])).rejects.toThrow();
  await expect(store().addDocuments([doc("a"), doc("a")])).rejects.toThrow();
  await expect(
    store().addDocuments([new Document({ pageContent: "x", metadata: { when: new Date() } })])
  ).rejects.toThrow();
  expect(calls).toEqual([]);
});
it("reports partial progress without mutation replay", async () => {
  failBatch = 2;
  const error = await store({ batchSize: 1 })
    .addDocuments([doc("a"), doc("b"), doc("c")])
    .catch((e) => e);
  expect(error).toBeInstanceOf(WriteOutcomeError);
  expect(error).toMatchObject({ status: "unknown", keys: ["b"], committedKeys: ["a"] });
  expect(batches).toBe(2);
});
it.each([
  "committed_pending",
  "committed_repair_required",
  undefined,
])("rejects unresolved outcome %s", async (status) => {
  batchStatus = status;
  await expect(store().addDocuments([doc("a")])).rejects.toBeInstanceOf(WriteOutcomeError);
  expect(batches).toBe(1);
});
it("allows explicit deferred visibility but refuses repair", async () => {
  batchStatus = "committed_pending";
  await store({ wait: false }).addDocuments([doc("a")]);
  batchStatus = "committed_repair_required";
  await expect(store({ wait: false }).addDocuments([doc("b")])).rejects.toThrow(WriteOutcomeError);
});
it("returns distances and refuses fused scores as distances", async () => {
  resultHits = [{ _id: "a", _source: { text: "a", metadata: {} }, _distance: 0.25, _score: 17 }];
  expect((await store().similaritySearchWithScore("a"))[0][1]).toBe(0.25);
  delete resultHits[0]._distance;
  await expect(store().similaritySearchWithScore("a")).rejects.toThrow("distance");
});
it("intersects fixed and caller filters", async () => {
  await store({
    filter: { term: "tenant", field: "metadata.tenant" },
  }).similaritySearchVectorWithScore([1, 0], 3, { term: "manual", field: "metadata.source" });
  expect(calls.find((c) => c.path.endsWith("/query"))?.body.filter_query).toEqual({
    conjuncts: [
      { term: "tenant", field: "metadata.tenant" },
      { term: "manual", field: "metadata.source" },
    ],
  });
});
it("handles missing IDs and deletes explicit records", async () => {
  const s = store();
  await s.addDocuments([doc("a"), doc("b")]);
  expect((await s.getByIds(["missing", "a", "a"])).map((d) => d.id)).toEqual(["a"]);
  await s.delete({ ids: ["a"] });
  expect([...documents.keys()]).toEqual(["b"]);
  await expect(s.delete({} as { ids: string[] })).rejects.toThrow();
});
it("MMR diversifies using actual candidate vectors", async () => {
  const s = store();
  await s.addVectors(
    [
      [1, 0],
      [0.99, 0.01],
      [0, 1],
    ],
    [doc("a"), doc("duplicate"), doc("diverse")]
  );
  expect(
    (await s.asRetriever({ searchType: "mmr", k: 2, searchKwargs: { lambda: 0 } }).invoke("a")).map(
      (d) => d.id
    )
  ).toEqual(["a", "diverse"]);
});
it("zero k avoids provider calls", async () => {
  expect(await store().similaritySearch("x", 0)).toEqual([]);
  await expect(store().similaritySearch("x", -1)).rejects.toThrow();
  expect(calls).toEqual([]);
});
it("batches embeddings", async () => {
  const e = new AntflyEmbeddings({
    client: inference(),
    model: "text",
    batchSize: 2,
    dimensions: 2,
  });
  expect(await e.embedDocuments(["a", "b", "c"])).toEqual([
    [1, 0],
    [1, 0],
    [1, 0],
  ]);
  expect(calls.map((c) => c.body.input)).toEqual([["a", "b"], ["c"]]);
});
it("cancels before embedding without a request", async () => {
  const e = new AntflyEmbeddings({ client: inference(), model: "text" });
  await expect(e.embedQuery("a", { signal: AbortSignal.abort() })).rejects.toThrow();
  expect(calls).toEqual([]);
});
it("preserves callbacks and concurrent per-call scope", async () => {
  const r = new AntflyRetriever({
    client: client(),
    table: "test",
    mode: "text",
    filter: { term: "fixed", field: "tenant" },
  });
  const start = vi.fn();
  const end = vi.fn();
  await Promise.all([
    r.invoke("first", {
      filter: { term: "a", field: "source" },
      callbacks: [{ handleRetrieverStart: start, handleRetrieverEnd: end }],
      tags: ["test"],
    }),
    r.invoke("second", { filter: { term: "b", field: "source" } }),
  ]);
  const requests = calls.filter((c) => c.path.endsWith("/query"));
  expect(requests[0].body.filter_query).not.toEqual(requests[1].body.filter_query);
  expect(start).toHaveBeenCalledOnce();
  expect(end).toHaveBeenCalledOnce();
});
it("cancels in-flight retrieval through fetch", async () => {
  const controller = new AbortController();
  vi.stubGlobal(
    "fetch",
    vi.fn(
      (request: Request) =>
        new Promise((_resolve, reject) => {
          request.signal.addEventListener("abort", () => reject(request.signal.reason));
          controller.abort(new Error("cancelled"));
        })
    )
  );
  await expect(
    new AntflyRetriever({ client: client(), table: "test", mode: "text" }).invoke("q", {
      signal: controller.signal,
    })
  ).rejects.toThrow("cancelled");
});
it("managed retrieval rejects external indexes", async () => {
  await expect(
    new AntflyRetriever({ client: client(), table: "test" }).invoke("q")
  ).rejects.toThrow("managed");
  indexConfig = { ...indexConfig, external: false };
  await new AntflyRetriever({ client: client(), table: "test" }).invoke("q");
  expect(calls.find((c) => c.path.endsWith("/query"))?.body).toMatchObject({
    semantic_search: "q",
    merge_config: { strategy: "rrf" },
  });
});
it("reranks stably and composes with standard compression", async () => {
  documents.set("a", { text: "a", metadata: { source: "manual" } });
  documents.set("b", { text: "b", metadata: { source: "manual" } });
  rerankData = [
    { index: 1, score: 1 },
    { index: 0, score: 1 },
  ];
  const reranker = new AntflyReranker({ client: inference(), model: "reranker", topN: 1 });
  const r = new ContextualCompressionRetriever({
    baseRetriever: store().asRetriever(),
    baseCompressor: reranker,
  });
  const result = await r.invoke("q");
  expect(result.map((d) => d.id)).toEqual(["a"]);
  expect(result[0].metadata).toMatchObject({
    source: "manual",
    _antfly: { rerank_score: 1, rerank_index: 0 },
  });
});
it.each([
  [
    { index: 0, score: 1 },
    { index: 0, score: 0 },
  ],
  [
    { index: 0, score: Infinity },
    { index: 1, score: 0 },
  ],
])("rejects corrupt reranking responses", async (...data) => {
  rerankData = data;
  await expect(
    new AntflyReranker({ client: inference(), model: "reranker" }).rerank("q", [doc("a"), doc("b")])
  ).rejects.toThrow();
});
it("standard asRetriever respects cancellation before embedding", async () => {
  await expect(
    store().asRetriever().invoke("q", { signal: AbortSignal.abort() })
  ).rejects.toThrow();
  expect(calls).toEqual([]);
});
it("embedding calls distinguish query and document tasks", async () => {
  const e = new AntflyEmbeddings({
    client: inference(),
    model: "text",
    queryInstruction: "Find matching passages",
  });
  await e.embedDocuments(["doc"]);
  await e.embedQuery("query");
  expect(calls[0].body).toMatchObject({
    task_type: "RETRIEVAL_DOCUMENT",
    error_policy: "fail_fast",
  });
  expect(calls[1].body).toMatchObject({
    task_type: "RETRIEVAL_QUERY",
    instruction: "Find matching passages",
  });
});
it("partial-batch cancellation retains committed IDs", async () => {
  const controller = new AbortController();
  const realFetch = globalThis.fetch;
  vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
    const response = await realFetch(input, init);
    const url = input instanceof Request ? input.url : String(input);
    if (url.endsWith("/batch")) controller.abort();
    return response;
  });
  const error = await store({ batchSize: 1 })
    .addDocuments([doc("a"), doc("b")], { signal: controller.signal })
    .catch((error) => error);
  expect(error).toMatchObject({ status: "not_submitted", keys: ["b"], committedKeys: ["a"] });
  expect(batches).toBe(1);
});
it("does not resurrect deleted resources after factory setup", async () => {
  const s = await AntflyVectorStore.fromDocuments([doc("a")], embeddings, {
    client: client(),
    table: "test",
  });
  indexConfig = undefined;
  const before = batches;
  await expect(s.addDocuments([doc("b")])).rejects.toMatchObject({ status: 404 });
  expect(batches).toBe(before);
  expect(
    calls.filter((call) => call.path.endsWith("/indexes/vectors") && call.method === "POST")
  ).toEqual([]);
});
it("factory input validation precedes resource creation", async () => {
  tableExists = false;
  indexConfig = undefined;
  await expect(
    AntflyVectorStore.fromDocuments([doc("a"), doc("a")], embeddings, {
      client: client(),
      table: "test",
    })
  ).rejects.toThrow();
  expect(calls).toEqual([]);
});
