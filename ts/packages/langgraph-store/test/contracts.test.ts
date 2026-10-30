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
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const req = input instanceof Request ? input : new Request(input, init),
        url = new URL(req.url);
      const body = req.method === "POST" ? await req.clone().json() : {};
      const json = (v: unknown, status = 200) =>
        new Response(JSON.stringify(v), {
          status,
          headers: { "Content-Type": "application/json" },
        });
      if (url.pathname.includes("/documents/")) {
        const id = decodeURIComponent(url.pathname.split("/documents/")[1]);
        return rows.has(id) ? json(rows.get(id)) : json({}, 404);
      }
      if (url.pathname.endsWith("/batch")) {
        for (const t of body.transforms ?? []) {
          const row: any = rows.get(t.key) ?? {};
          for (const op of t.operations) {
            const parts = op.path.slice(2).split(".");
            let obj = row;
            for (const part of parts.slice(0, -1)) obj = obj[part] ??= {};
            const name = parts.at(-1)!;
            if (
              op.op === "$set" ||
              (op.op === "$setOnInsert" && obj[name] === undefined) ||
              (op.op === "$max" && (obj[name] === undefined || obj[name] < op.value))
            )
              obj[name] = op.value;
          }
          rows.set(t.key, row);
        }
        for (const id of body.deletes ?? []) rows.delete(id);
        return json({ status: "committed" });
      }
      if (url.pathname.endsWith("/query")) {
        const get = (obj: any, path: string) =>
          path
            .slice(1)
            .split("/")
            .reduce((v, k) => v?.[k], obj);
        // The server admits scalar term operands and matches any array element.
        const term = (actual: unknown, expected: unknown): boolean => {
          if (expected !== null && typeof expected === "object")
            throw new Error("Term operands must be scalar");
          return Array.isArray(actual)
            ? actual.some((value) => term(value, expected))
            : actual === expected;
        };
        const match = (obj: any, q: any): boolean =>
          !q ||
          q.conjuncts?.every((c: any) => match(obj, c)) ||
          q.disjuncts?.some((c: any) => match(obj, c)) ||
          (q.term && term(get(obj, q.term.path), q.term.value)) ||
          (q.range &&
            Object.entries(q.range).every(([path, ops]: any) =>
              Object.entries(ops).every(([op, x]: any) => {
                const v = get(obj, path);
                return op === "gt" ? v > x : op === "gte" ? v >= x : op === "lt" ? v < x : v <= x;
              })
            ));
        let found = [...rows]
          .filter(
            ([id, row]) =>
              id.startsWith(body.filter_prefix ?? "") &&
              match(row, body.filter_query) &&
              (!body.exclusion_query || !match(row, body.exclusion_query))
          )
          .map(([id, row]: any) => ({
            _id: id,
            _source: row,
            _sort: [id],
            _distance: row.value?.vectors?.[0]?.[0] === 1 ? 0 : 1,
          }));
        if (body.embeddings) found = found.sort((a, b) => a._distance - b._distance);
        else
          found = found
            .sort((a, b) => a._id.localeCompare(b._id))
            .filter((h) => !body.search_after || h._id > body.search_after[0]);
        return json({ responses: [{ hits: { hits: found.slice(0, body.limit ?? 100) } }] });
      }
      return json({});
    })
  );
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

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
it.each<[Record<string, unknown>, string[]]>([
  [{ tags: "a" }, ["2"]],
  [{ tags: { $eq: ["a", "b"] } }, ["1", "3"]],
  [{ tags: ["a", "b"] }, ["1", "3"]],
  [{ profile: { x: 1 } }, ["1", "2"]],
  [{ profile: { $eq: { x: 1 } } }, ["1", "2"]],
  [{ tags: { $ne: ["a", "b"] } }, ["2"]],
  [{ tags: { $ne: "a" } }, ["1", "3"]],
  [{ n: { $gte: 2 } }, ["2"]],
  [{ n: { $lt: "4" } }, ["3"]],
  [{ "profile.x": 1 }, ["1", "2"]],
])("preserves filtered semantic search before and after multifield writes: %j", async (filter, expected) => {
  const memory = store();
  await memory.put(["a"], "1", { food: "apple", tags: ["a", "b"], profile: { x: 1 }, n: [2] });
  await memory.put(["a"], "2", { food: "pear", tags: "a", profile: { x: 1 }, n: 2 });
  await memory.put(["a"], "3", { food: "apple", tags: ["a", "b"], profile: { x: 2 }, n: "3" });
  await memory.put(["a"], "4", { tags: "a", profile: { x: 1 }, n: 4 }, false);
  await memory.put(["other"], "foreign", { tags: "a", profile: { x: 1 }, n: 2 });
  const search = async () =>
    (await store().search(["a"], { query: "apple", filter })).map((item) => item.key).sort();
  expect(await search()).toEqual(expected);
  await memory.put(["a"], "multi", { parts: ["apple", "pear"] }, ["parts[*]"]);
  await memory.delete(["a"], "multi");
  expect(await search()).toEqual(expected);
});
it("filters semantic candidates before applying offset and limit across query pages", async () => {
  const memory = new AntflyStore({
    client: new AntflyClient({ baseUrl: "http://unused" }),
    table: "memory",
    pageSize: 1,
    index: { dims: 2, embeddings: { embedDocuments, embedQuery: async () => [1, 0] } },
  });
  await memory.put(["a"], "1", { food: "apple", tags: ["a"] });
  await memory.put(["a"], "2", { food: "apple", tags: "a" });
  await memory.put(["a"], "3", { food: "pear", tags: "a" });
  expect(
    (await memory.search(["a"], { query: "apple", filter: { tags: "a" }, limit: 1 })).map(
      (item) => item.key
    )
  ).toEqual(["2"]);
  expect(
    (
      await memory.search(["a"], { query: "apple", filter: { tags: "a" }, offset: 1, limit: 1 })
    ).map((item) => item.key)
  ).toEqual(["3"]);
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
it("uses native scoped search until a persistent multifield marker selects exact pooling", async () => {
  const memory = store();
  await memory.put(["a"], "one", { food: "apple" });
  await memory.search(["a"], { query: "q" });
  const requests = () =>
    vi
      .mocked(fetch)
      .mock.calls.map(([r]) => r as Request)
      .filter((r) => r instanceof Request && r.url.endsWith("/query"));
  expect(
    (await Promise.all(requests().map((r) => r.clone().json()))).some((b) => b.embeddings)
  ).toBe(true);
  await memory.put(["a"], "two", { parts: ["pear", "apple"] }, ["parts[*]"]);
  vi.mocked(fetch).mockClear();
  expect(await store().search(["a"], { query: "q" })).toHaveLength(2);
  expect(
    (await Promise.all(requests().map((r) => r.clone().json()))).some((b) => b.embeddings)
  ).toBe(false);
  expect(
    vi.mocked(fetch).mock.calls.some(([r]) => r instanceof Request && r.url.includes("/documents/"))
  ).toBe(false);
});
