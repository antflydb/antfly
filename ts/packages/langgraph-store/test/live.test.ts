import { randomUUID } from "node:crypto";
import { AntflyClient } from "@antfly/sdk";
import {
  Annotation,
  END,
  type LangGraphRunnableConfig,
  START,
  StateGraph,
} from "@langchain/langgraph";
import { describe, expect, it } from "vitest";
import { AntflyStore } from "../src/index.js";

const live = process.env.ANTFLY_LIVE === "1";
describe.skipIf(!live)("Antfly memory in LangGraph runtime", () => {
  it("persists across runtimes and isolates two users, with ordered operations and search controls", async () => {
    const client = new AntflyClient({
      baseUrl: process.env.ANTFLY_URL ?? "http://127.0.0.1:46181",
    });
    const table = `memory_${randomUUID().replaceAll("-", "")}`;
    const index = {
      dims: 2,
      embeddings: {
        embedDocuments: async (texts: string[]) =>
          texts.map((t) => (t.includes("apple") ? [1, 0] : [0, 1])),
        embedQuery: async () => [1, 0],
      },
    };
    const store = await AntflyStore.create({ client, table, pageSize: 2, index });
    try {
      const State = Annotation.Root({ text: Annotation<string>, answer: Annotation<string> });
      const build = (store: AntflyStore) =>
        new StateGraph(State)
          .addNode("memory", async (state, config: LangGraphRunnableConfig) => {
            const user = config.configurable?.userId;
            if (!user) throw new Error("Trusted userId required");
            if (state.text)
              await config.store?.put(["users", user], "favorite", { food: state.text });
            const memory = await config.store?.get(["users", user], "favorite");
            return { answer: String(memory?.value.food ?? "unknown") };
          })
          .addEdge(START, "memory")
          .addEdge("memory", END)
          .compile({ store });
      expect(
        (await build(store).invoke({ text: "apple" }, { configurable: { userId: "alice" } })).answer
      ).toBe("apple");
      expect(
        (await build(store).invoke({ text: "pear" }, { configurable: { userId: "bob" } })).answer
      ).toBe("pear");
      const attached = new AntflyStore({ client, table, pageSize: 2, index });
      expect(
        (await build(attached).invoke({ text: "" }, { configurable: { userId: "alice" } })).answer
      ).toBe("apple");
      expect((await attached.search(["users"], { query: "apple", limit: 1 }))[0].namespace).toEqual(
        ["users", "alice"]
      );
      const before = await attached.get(["users", "alice"], "favorite");
      const results = await attached.batch([
        {
          namespace: ["users", "alice"],
          key: "favorite",
          value: { food: "apple", n: 2 },
          index: false,
        },
        { namespace: ["users", "alice"], key: "favorite" },
      ]);
      expect(results[1]?.value.n).toBe(2);
      expect(results[1]?.createdAt).toEqual(before?.createdAt);
      expect(await attached.search(["users", "alice"], { query: "apple" })).toEqual([]);
      expect((await attached.search(["users"], { filter: { n: { $gte: 2 } } })).length).toBe(1);
      expect(await attached.listNamespaces({ prefix: ["users"], maxDepth: 1 })).toEqual([
        ["users"],
      ]);
      await attached.delete(["users", "alice"], "favorite");
      expect(await attached.get(["users", "alice"], "favorite")).toBeNull();
    } finally {
      await client.tables.drop(table);
    }
  }, 60000);
});
