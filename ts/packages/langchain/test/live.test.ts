import { randomUUID } from "node:crypto";
import { AntflyClient, InferenceClient } from "@antfly/sdk";
import { Document } from "@langchain/core/documents";
import { afterAll, describe, expect, it } from "vitest";
import {
  AntflyEmbeddings,
  AntflyReranker,
  AntflyRetriever,
  AntflyVectorStore,
} from "../src/index.js";

const live = process.env.ANTFLY_LIVE === "1";
describe.skipIf(!live)("real Antfly retrieval lifecycle", () => {
  const baseUrl = process.env.ANTFLY_URL ?? "http://127.0.0.1:8080";
  const headers: Record<string, string> = process.env.ANTFLY_TOKEN
    ? { Authorization: `Bearer ${process.env.ANTFLY_TOKEN}` }
    : {};
  const client = new AntflyClient({ baseUrl, headers });
  const inference = new InferenceClient({ baseUrl, headers });
  const model = process.env.ANTFLY_EMBEDDING_MODEL ?? "BAAI/bge-small-en-v1.5";
  const embeddings = new AntflyEmbeddings({ client: inference, model });
  const table = `lcjs_${randomUUID().replaceAll("-", "")}`;
  const managed = `${table}_managed`;
  let store: AntflyVectorStore;
  const docs = [
    new Document({
      id: "owner",
      pageContent: "Mira Chen owns operations and handles shipment escalation.",
      metadata: { source: "handbook" },
    }),
    new Document({
      id: "apples",
      pageContent: "Apples are stored in the cool orchard warehouse.",
      metadata: { source: "handbook" },
    }),
    new Document({
      id: "shipment",
      pageContent: "The Q3 shipment target is 120 crates.",
      metadata: { source: "forecast" },
    }),
  ];
  afterAll(async () => {
    if (live)
      for (const name of [table, managed]) {
        try {
          await client.tables.drop(name);
        } catch (error) {
          console.error(`Cleanup ${name}:`, error);
        }
      }
  });
  it("ingests and finds the expected document", async () => {
    store = await AntflyVectorStore.fromDocuments(docs, embeddings, {
      client,
      table,
      distanceMetric: "cosine",
    });
    expect((await store.asRetriever(1).invoke("Who owns operations?"))[0].id).toBe("owner");
  }, 120000);
  it("attaches from a fresh client, scores, filters, MMR and hybrid retrieval", async () => {
    const attached = new AntflyVectorStore(embeddings, {
      client: new AntflyClient({ baseUrl, headers }),
      table,
    });
    const scored = await attached.similaritySearchWithScore("Who owns operations?", 2);
    expect(scored[0][0].id).toBe("owner");
    expect(Number.isFinite(scored[0][1])).toBe(true);
    const filtered = await attached.similaritySearch("weather", 3, {
      term: "handbook",
      field: "metadata.source",
    });
    expect(filtered.every((doc) => doc.metadata.source === "handbook")).toBe(true);
    expect(filtered.length).toBe(2);
    expect(
      (await attached.asRetriever({ searchType: "mmr", k: 2 }).invoke("operations")).length
    ).toBe(2);
    expect(
      (await new AntflyRetriever({ vectorStore: attached, k: 1 }).invoke("Mira operations"))[0].id
    ).toBe("owner");
  }, 120000);
  it("reranks actual documents using a local model", async () => {
    const reranker = new AntflyReranker({
      client: inference,
      model: process.env.ANTFLY_RERANKER_MODEL ?? "antflydb/mxbai-rerank-base-v1",
      topN: 1,
    });
    const result = await reranker
      .asRunnable()
      .invoke({ query: "Who owns operations?", documents: [docs[2], docs[0]] });
    expect(result[0].id).toBe("owner");
    expect(result[0].metadata._antfly.rerank_index).toBe(1);
  }, 120000);
  it("updates and deletes with immediate search visibility", async () => {
    await store.addDocuments([
      new Document({
        id: "owner",
        pageContent: "Nora Patel owns operations.",
        metadata: { source: "handbook" },
      }),
    ]);
    expect((await store.getByIds(["missing", "owner"]))[0].pageContent).toContain("Nora");
    await store.delete({ ids: ["owner"] });
    expect(await store.getByIds(["owner"])).toEqual([]);
    expect(
      (await store.similaritySearch("Who owns operations?", 3)).some((doc) => doc.id === "owner")
    ).toBe(false);
  }, 120000);
  it("retrieves from an independently provisioned managed index", async () => {
    await client.tables.create(managed, {
      indexes: {
        vectors: {
          type: "embeddings",
          field: "text",
          embedder: { provider: "antfly", model },
          distance_metric: "cosine",
        },
      },
    });
    const result = await client.tables.batch(managed, {
      inserts: { owner: { text: docs[0].pageContent, metadata: { source: "handbook" } } },
      sync_level: "full_index",
    });
    expect(result.status).toBe("committed");
    expect(
      (
        await new AntflyRetriever({ client, table: managed, mode: "dense", k: 1 }).invoke(
          "Who owns operations?"
        )
      )[0].id
    ).toBe("owner");
  }, 120000);
});
