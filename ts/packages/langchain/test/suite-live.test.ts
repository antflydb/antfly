import { randomUUID } from "node:crypto";
import { AntflyClient, InferenceClient } from "@antfly/sdk";
import { MultiVectorRetriever } from "@langchain/classic/retrievers/multi_vector";
import { Document } from "@langchain/core/documents";
import { afterAll, describe, expect, it } from "vitest";
import {
  AntflyByteStore,
  AntflyDocumentStore,
  AntflyEmbeddings,
  AntflyGraphStore,
  AntflyJSONStore,
  AntflyMultimodalCorpus,
  AntflyVectorStore,
  createAntflyGraphTools,
} from "../src/index.js";

const live = process.env.ANTFLY_LIVE === "1";
describe.skipIf(!live)("persistent suite on real Antfly", () => {
  const baseUrl = process.env.ANTFLY_URL ?? "http://127.0.0.1:46181";
  const client = new AntflyClient({ baseUrl });
  const table = `suite_${randomUUID().replaceAll("-", "")}`;
  const graphTable = `${table}_graph`,
    vectorTable = `${table}_vectors`;
  afterAll(async () => {
    if (live)
      for (const name of [table, graphTable, vectorTable])
        await client.tables.drop(name).catch(() => {});
  });
  it("roundtrips paged Unicode keys, documents and bytes across fresh attachments", async () => {
    const store = await AntflyJSONStore.create({
      client,
      table,
      namespace: "a/雪",
      pageSize: 2,
      batchSize: 2,
    });
    await store.mset([
      ["a/1", { v: 1 }],
      ["a/2", null],
      ["a/雪", [1, true]],
      ["b", false],
    ]);
    const attached = new AntflyJSONStore({
      client: new AntflyClient({ baseUrl }),
      table,
      namespace: "a/雪",
      pageSize: 2,
    });
    expect(await attached.mget(["missing", "a/2", "a/1"])).toEqual([undefined, null, { v: 1 }]);
    expect(
      await (async () => {
        const keys: string[] = [];
        for await (const key of attached.yieldKeys("a/")) keys.push(key);
        return keys;
      })()
    ).toEqual(["a/1", "a/2", "a/雪"]);
    expect(await new AntflyJSONStore({ client, table, namespace: "a" }).mget(["a/1"])).toEqual([
      undefined,
    ]);
    const docs = new AntflyDocumentStore({ client, table, namespace: "parents" });
    await docs.mset([
      ["p", new Document({ id: "p", pageContent: "full original", metadata: { n: 1 } })],
    ]);
    expect((await docs.mget(["p"]))[0]).toBeInstanceOf(Document);
    const bytes = new AntflyByteStore({ client, table, namespace: "bytes" });
    await bytes.mset([["raw", new Uint8Array([0, 255, 128])]]);
    expect([...(await bytes.mget(["raw"]))[0]!]).toEqual([0, 255, 128]);
    await attached.mdelete(["a/1"]);
    expect(await attached.mget(["a/1"])).toEqual([undefined]);
  }, 30000);
  it("owns shared graph facts, scopes traversal and exposes actual LangChain tools", async () => {
    const graph = await AntflyGraphStore.create({
      client,
      table: graphTable,
      namespace: "tenant-a",
    });
    const owner = { id: "mira", type: "Person", properties: { name: "Mira" } },
      team = { id: "ops", type: "Team" };
    const doc = (id: string) => ({
      nodes: [owner, team],
      relationships: [{ source: owner, target: team, type: "OWNS" }],
      source: new Document({ id, pageContent: id }),
    });
    await graph.addGraphDocuments([doc("a"), doc("b")]);
    const found = await graph.traverse([graph.nodeKey("Person", "mira")], 1);
    expect(found.some((row) => row.key === graph.nodeKey("Team", "ops"))).toBe(true);
    const aggregate = await graph.queryGraph({
      count: {
        match: { anchor: "n", nodes: { n: {} }, edges: [] },
        return: { aggregates: { total: { count: "*" } } },
      },
    });
    expect(aggregate[0].total).toEqual({ value: "2", exact: true });
    const bindings = await graph.queryGraph({
      nodes: { match: { anchor: "n", nodes: { n: {} }, edges: [] }, return: { bindings: ["n"] } },
    });
    expect(bindings).toHaveLength(2);
    for (const kind of ["shortest_path", "k_shortest_paths"]) {
      const paths = await graph.queryGraph({
        path: {
          [kind]: {
            from: { key: graph.nodeKey("Person", "mira") },
            to: { key: graph.nodeKey("Team", "ops") },
            max_depth: 2,
            ...(kind === "k_shortest_paths" ? { k: 1 } : {}),
          },
        },
      });
      expect(paths[0].path.length).toBe(1);
    }
    const expanded = await graph.expandDocuments([
      new Document({ id: graph.nodeKey("Person", "mira"), pageContent: "Mira" }),
    ]);
    expect(expanded.some((doc) => doc.id === graph.nodeKey("Team", "ops"))).toBe(true);
    await graph.deleteSource("a");
    expect((await graph.traverse([graph.nodeKey("Person", "mira")], 1)).length).toBeGreaterThan(0);
    const tools = createAntflyGraphTools(graph);
    expect(await tools[0].invoke({})).toContain("OWNS");
    const other = new AntflyGraphStore({ client, table: graphTable, namespace: "tenant-b" });
    await expect(other.traverse([graph.nodeKey("Person", "mira")])).rejects.toThrow("namespace");
    await graph.deleteSource("b");
    await graph.refreshSchema();
    expect(graph.structuredSchema.node_types).toEqual([]);
  }, 60000);
  it("retrieves persistent parents, verifies original image bytes, replaces and deletes", async () => {
    const embeddings = new AntflyEmbeddings({
      client: new InferenceClient({ baseUrl }),
      model: process.env.ANTFLY_EMBEDDING_MODEL ?? "BAAI/bge-small-en-v1.5",
    });
    const vectors = await AntflyVectorStore.create(embeddings, {
      client,
      table: vectorTable,
      dimensions: 384,
      distanceMetric: "cosine",
    });
    const parents = new AntflyDocumentStore({ client, table, namespace: "mm-parents" }),
      media = new AntflyByteStore({ client, table, namespace: "mm-media" });
    const corpus = new AntflyMultimodalCorpus(vectors, parents, media);
    const image = Buffer.from(
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a9l8AAAAASUVORK5CYII=",
      "base64"
    );
    const source = {
      id: "orchard",
      text: "Quarter three target is 120 crates.",
      summaries: ["Orchard Q3 shipment target 120 crates"],
      images: [{ bytes: image, mimeType: "image/png" as const }],
    };
    await corpus.ingest(source);
    const reopened = new AntflyMultimodalCorpus(
      new AntflyVectorStore(embeddings, { client, table: vectorTable }),
      new AntflyDocumentStore({ client, table, namespace: "mm-parents" }),
      new AntflyByteStore({ client, table, namespace: "mm-media" })
    );
    const evidence = await reopened.evidence("shipment target");
    expect(evidence[0].id).toBe("orchard");
    const upstream = new MultiVectorRetriever({
      vectorstore: vectors,
      docstore: parents,
      idKey: "doc_id",
    });
    expect((await upstream.invoke("shipment target"))[0].pageContent).toContain("120");
    expect(JSON.stringify((await reopened.message("target?", evidence)).content)).toContain(
      image.toString("base64")
    );
    await corpus.ingest({
      ...source,
      text: "Target is now 130 crates.",
      summaries: ["Shipment target 130 crates"],
    });
    expect((await reopened.evidence("shipment target"))[0].pageContent).toContain("130");
    const current = await reopened.evidence("shipment target");
    await media.mdelete([current[0].metadata.media[0].key]);
    await expect(reopened.message("target?", current)).rejects.toThrow("Missing or corrupt");
    await corpus.delete("orchard");
    expect(await reopened.evidence("shipment target")).toEqual([]);
  }, 120000);
});
