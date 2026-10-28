import { AntflyJSONStore } from "@antfly/langchain";
import { AntflyClient } from "@antfly/sdk";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { AntflyStore } from "../src/index.js";

const rows = new Map<string, unknown>();
const embedDocuments = vi.fn(async (texts: string[]) =>
  texts.map((t) => (t.includes("apple") ? [1, 0] : [0, 1]))
);
const store = () =>
  new AntflyStore({
    client: new AntflyClient({ baseUrl: "http://unused" }),
    table: "memory",
    index: { dims: 2, embeddings: { embedDocuments, embedQuery: async () => [1, 0] } },
  });
beforeEach(() => {
  rows.clear();
  vi.clearAllMocks();
  vi.spyOn(AntflyJSONStore.prototype, "mget").mockImplementation(
    async (keys) => keys.map((key) => rows.get(key)) as never
  );
  vi.spyOn(AntflyJSONStore.prototype, "mset").mockImplementation(async (pairs) => {
    for (const [key, value] of pairs) rows.set(key, structuredClone(value));
  });
  vi.spyOn(AntflyJSONStore.prototype, "mdelete").mockImplementation(async (keys) => {
    for (const key of keys) rows.delete(key);
  });
  vi.spyOn(AntflyJSONStore.prototype, "yieldKeys").mockImplementation(async function* (
    prefix = ""
  ) {
    for (const key of [...rows.keys()].sort()) if (key.startsWith(prefix)) yield key;
  });
});
afterEach(() => vi.restoreAllMocks());
it("extracts whole values, wildcard nested arrays and negative array indices", async () => {
  const memory = store();
  await memory.put(["user", "a"], "k", { parts: [{ text: "apple" }, { text: "pear" }] }, [
    "parts[*].text",
    "parts[-1].text",
  ]);
  expect(embedDocuments).toHaveBeenLastCalledWith(["apple", "pear", "pear"]);
  await memory.put(["user", "a"], "all", { food: "apple" });
  expect(embedDocuments).toHaveBeenLastCalledWith(['{"food":"apple"}']);
});
it("isolates component boundaries and preserves batch order and timestamps", async () => {
  const memory = store();
  const result = await memory.batch([
    { namespace: ["a/b"], key: "c", value: { n: 1 } },
    { namespace: ["a", "b"], key: "c", value: { n: 2 } },
    { namespace: ["a/b"], key: "c" },
  ]);
  expect(result[2]?.value).toEqual({ n: 1 });
  const before = result[2]?.createdAt;
  await memory.put(["a/b"], "c", { n: 3 });
  expect((await memory.get(["a/b"], "c"))?.createdAt).toEqual(before);
  expect((await memory.search(["a"])).map((x) => x.value.n)).toEqual([2]);
});
it("applies filters before pagination and honors index false", async () => {
  const memory = store();
  await memory.put(["a"], "1", { food: "apple", n: 1 });
  await memory.put(["a"], "2", { food: "apple", n: 2 }, false);
  await memory.put(["a"], "3", { food: "pear", n: 3 });
  expect(
    (await memory.search(["a"], { filter: { n: { $gte: 2 } }, offset: 1, limit: 1 }))[0].key
  ).toBe("3");
  expect((await memory.search(["a"], { query: "apple" })).map((x) => x.key)).toEqual(["1", "3"]);
  await expect(memory.search(["a"], { filter: { n: { $unknown: 2 } } })).rejects.toThrow(
    "Unsupported"
  );
});
it("validates vector dimensions, namespaces and TTL", async () => {
  const memory = store();
  await expect(memory.put([], "k", {})).rejects.toThrow();
  await expect(
    memory.batch([{ namespace: ["a"], key: "k", value: {}, ttl: 10 } as never])
  ).rejects.toThrow("TTL");
  embedDocuments.mockResolvedValueOnce([[1]]);
  await expect(memory.put(["a"], "k", {})).rejects.toThrow("embedding");
  expect(rows.size).toBe(0);
});
it("lists prefix/suffix wildcards and collapses maximum depth", async () => {
  const memory = store();
  for (const ns of [
    ["users", "a", "facts"],
    ["users", "b", "facts"],
    ["users", "a", "prefs"],
  ])
    await memory.put(ns, "k", {});
  expect(await memory.listNamespaces({ suffix: ["facts"], maxDepth: 2 })).toEqual([
    ["users", "a"],
    ["users", "b"],
  ]);
  expect(await memory.listNamespaces({ prefix: ["users", "*", "prefs"] })).toEqual([
    ["users", "a", "prefs"],
  ]);
});
