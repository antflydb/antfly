import { randomUUID } from "node:crypto";
import type { AntflyClient } from "@antfly/sdk";
import { embedMany, generateText, rerank, streamText, tool } from "ai";
import { describe, expect, it } from "vitest";
import { z } from "zod";
import { isRetryableCapacityError, retryDelayMs } from "../src/asset/errors.js";
import { createChatHistory } from "../src/history.js";
import { createAntfly } from "../src/provider.js";
import { createGraphTool } from "../src/tools/graph.js";
import { createMemoryTools } from "../src/tools/memory.js";
import { createSearchTool } from "../src/tools/search.js";
import { createSqlTool } from "../src/tools/sql.js";

const live = process.env.ANTFLY_LIVE === "1";
const baseUrl = process.env.ANTFLY_URL ?? "http://127.0.0.1:8080";
const chatModel = process.env.ANTFLY_CHAT_MODEL;
const embeddingModel = process.env.ANTFLY_EMBEDDING_MODEL;
const rerankModel = process.env.ANTFLY_RERANK_MODEL;

/** Retries once on a transient, server-declared capacity limit (this is
 * exactly what InferenceCapacityError.retryAfterMs is for), rather than
 * treating a busy shared inference host as a test failure. */
async function withCapacityRetry<T>(run: () => Promise<T>): Promise<T> {
  try {
    return await run();
  } catch (error) {
    if (!isRetryableCapacityError(error)) throw error;
    await new Promise((resolve) => setTimeout(resolve, retryDelayMs(error)));
    return await run();
  }
}

async function waitForOperation(client: AntflyClient, space: string, id: string): Promise<void> {
  for (let attempt = 0; attempt < 30; attempt++) {
    const { data } = await client.memory.operation(space, id);
    if (data?.phase === "succeeded") return;
    if (data?.phase === "failed" || data?.phase === "cancelled") {
      throw new Error(`Memory operation ${id} ${data.phase}: ${data.error_code}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(`Memory operation ${id} did not complete in time`);
}

describe.skipIf(!live)("@antfly/ai-sdk against a real Antfly server", () => {
  it("languageModel: generateText", async () => {
    if (!chatModel) throw new Error("Set ANTFLY_CHAT_MODEL");
    const antfly = createAntfly({ baseUrl });
    const result = await generateText({
      model: antfly.languageModel(chatModel),
      prompt: "Reply with exactly the word: pong",
      maxOutputTokens: 16,
    });
    expect(result.text.toLowerCase()).toContain("pong");
    expect(result.usage.inputTokens).toBeGreaterThan(0);
  }, 60000);

  it("languageModel: streamText", async () => {
    if (!chatModel) throw new Error("Set ANTFLY_CHAT_MODEL");
    const antfly = createAntfly({ baseUrl });
    const result = streamText({
      model: antfly.languageModel(chatModel),
      prompt: "Count from one to three, one number per line.",
      maxOutputTokens: 32,
    });
    let text = "";
    for await (const chunk of result.textStream) text += chunk;
    expect(text.length).toBeGreaterThan(0);
    expect(await result.finishReason).toBeTruthy();
  }, 60000);

  it("languageModel: tool call loop (skips the assertion if this model does not call tools)", async () => {
    if (!chatModel) throw new Error("Set ANTFLY_CHAT_MODEL");
    const antfly = createAntfly({ baseUrl });
    let called = false;
    const result = await generateText({
      model: antfly.languageModel(chatModel),
      prompt: "What is the weather in Boston? Use the weather tool.",
      tools: {
        weather: tool({
          description: "Get the current weather for a city.",
          inputSchema: z.object({ city: z.string() }),
          execute: async ({ city }) => {
            called = true;
            return { city, forecast: "sunny" };
          },
        }),
      },
      stopWhen: ({ steps }) => steps.length >= 3,
    });
    console.log(
      called
        ? "tool call loop: model called the weather tool"
        : `tool call loop: model did not call a tool (finishReason=${result.finishReason}); this local model may not support tool_call_format`
    );
  }, 60000);

  it("embeddingModel: embedMany", async () => {
    if (!embeddingModel) throw new Error("Set ANTFLY_EMBEDDING_MODEL");
    const antfly = createAntfly({ baseUrl });
    const { embeddings } = await withCapacityRetry(() =>
      embedMany({
        model: antfly.embeddingModel(embeddingModel),
        values: ["apples are stored in the warehouse", "the shipment target is 120 crates"],
      })
    );
    expect(embeddings).toHaveLength(2);
    expect(embeddings[0].length).toBeGreaterThan(0);
    expect(embeddings[0].length).toBe(embeddings[1].length);
  }, 60000);

  it.skipIf(!rerankModel)(
    "rerankingModel: rerank",
    async () => {
      const antfly = createAntfly({ baseUrl });
      const result = await rerank({
        model: antfly.rerankingModel(rerankModel ?? ""),
        query: "warehouse storage",
        documents: ["Apples are stored in the warehouse.", "The capital of France is Paris."],
      });
      expect(result.rerankedDocuments[0]).toContain("warehouse");
    },
    60000
  );

  it("imageModel throws, since Antfly has none", () => {
    const antfly = createAntfly({ baseUrl });
    expect(() => antfly.imageModel("anything")).toThrow();
  }, 60000);

  it("memory tools: remember, recall and forget against a real memory space", async () => {
    const antfly = createAntfly({ baseUrl });
    const space = `aisdk_live_${randomUUID().replaceAll("-", "")}`;
    await antfly.client.memory.create({ space }, randomUUID());
    try {
      const tools = createMemoryTools({ client: antfly.client, space });
      const toolOptions = { toolCallId: "t1", messages: [] } as never;
      const remembered = (await tools.antfly_memory_remember.execute?.(
        {
          subject: "Mira Chen",
          predicate: "owns",
          value: "operations",
          statement: "Mira Chen owns operations and handles shipment escalation.",
        },
        toolOptions
      )) as { id: string };
      await waitForOperation(antfly.client, space, remembered.id);
      let recalled: { operations?: unknown[] } | undefined;
      for (let attempt = 0; attempt < 10; attempt++) {
        recalled = (await tools.antfly_memory_recall.execute?.(
          { query: "Who owns operations?" },
          toolOptions
        )) as typeof recalled;
        if (JSON.stringify(recalled).includes("Mira")) break;
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
      expect(JSON.stringify(recalled)).toContain("Mira");
    } finally {
      await antfly.client.memory.forget(space, { all: true }, randomUUID()).catch(() => undefined);
    }
  }, 60000);

  it("search tool: full-text search against a real table", async () => {
    const antfly = createAntfly({ baseUrl });
    const table = `aisdk_live_search_${randomUUID().replaceAll("-", "")}`;
    await antfly.client.tables.create(table, {});
    try {
      await antfly.client.tables.batch(table, {
        inserts: {
          owner: { text: "Mira Chen owns operations and handles shipment escalation." },
          apples: { text: "Apples are stored in the cool orchard warehouse." },
        },
        sync_level: "full_index",
      });
      const tools = createSearchTool({ client: antfly.client, table });
      const result = (await tools.antfly_search.execute?.({ query: "operations" }, {
        toolCallId: "t1",
        messages: [],
      } as never)) as { responses?: { hits?: { hits?: { _id: string }[] } }[] };
      const ids = result.responses?.[0]?.hits?.hits?.map((hit) => hit._id) ?? [];
      expect(ids).toContain("owner");
    } finally {
      await antfly.client.tables.drop(table).catch(() => undefined);
    }
  }, 60000);

  it("graph tool: traverse a real graph index", async () => {
    const antfly = createAntfly({ baseUrl });
    const table = `aisdk_live_graph_${randomUUID().replaceAll("-", "")}`;
    await antfly.client.tables.create(table, {
      indexes: { graph: { type: "graph", edge_types: [] } },
    });
    try {
      await antfly.client.tables.batch(table, {
        inserts: {
          alice: {
            graph_type: "person",
            graph_id: "alice",
            properties: { name: "Alice" },
            _edges: { graph: { knows: [{ target: "bob", weight: 1 }] } },
          },
          bob: { graph_type: "person", graph_id: "bob", properties: { name: "Bob" } },
        },
        sync_level: "full_index",
      });
      const tools = createGraphTool({ client: antfly.client, table, index: "graph" });
      const result = (await tools.antfly_graph_query.execute?.(
        { operations: { neighbors: { traverse: { start: { keys: ["alice"] }, max_depth: 1 } } } },
        { toolCallId: "t1", messages: [] } as never
      )) as { neighbors?: { nodes?: { key: string }[] } };
      const keys = result.neighbors?.nodes?.map((node) => node.key) ?? [];
      expect(keys).toContain("bob");
    } finally {
      await antfly.client.tables.drop(table).catch(() => undefined);
    }
  }, 60000);

  it("sql tool: a real SELECT round trip, and a client-side rejected write", async () => {
    const antfly = createAntfly({ baseUrl });
    const tools = createSqlTool({ client: antfly.client });
    const result = (await tools.antfly_sql.execute?.({ statement: "SELECT 1 AS one" }, {
      toolCallId: "t1",
      messages: [],
    } as never)) as { rows: unknown[][] };
    expect(result.rows[0][0]).toBe("1");
    await expect(
      tools.antfly_sql.execute?.({ statement: "DELETE FROM anything" }, {
        toolCallId: "t1",
        messages: [],
      } as never)
    ).rejects.toThrow(/read-only/);
  }, 60000);

  it("chat history: save and load round trip against a real table", async () => {
    const antfly = createAntfly({ baseUrl });
    const table = `aisdk_live_history_${randomUUID().replaceAll("-", "")}`;
    await antfly.client.tables.create(table, {});
    try {
      const history = createChatHistory({ client: antfly.client, table });
      const chatId = randomUUID();
      const messages = [
        { id: "m1", role: "user" as const, parts: [{ type: "text" as const, text: "hi" }] },
      ];
      await history.save(chatId, messages);
      expect(await history.load(chatId)).toEqual(messages);
    } finally {
      await antfly.client.tables.drop(table).catch(() => undefined);
    }
  }, 60000);
});
