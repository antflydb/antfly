import { isDeepStrictEqual } from "node:util";
import {
  AntflyJSONStore,
  type AntflyStoreOptions,
  jsonCopy,
  WriteOutcomeError,
} from "@antfly/langchain";
import type { BatchRequest, TableQueryRequest } from "@antfly/sdk";
import type { EmbeddingsInterface } from "@langchain/core/embeddings";
import {
  BaseStore,
  type Item,
  type ListNamespacesOperation,
  type Operation,
  type OperationResults,
  type SearchOperation,
  validateNamespace,
} from "@langchain/langgraph-checkpoint";

export interface AntflyMemoryOptions extends AntflyStoreOptions {
  indexName?: string;
  index?: { embeddings: EmbeddingsInterface; dims: number; fields?: string[] };
}
interface StoredItem {
  namespace: string[];
  key: string;
  value: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
  updatedAtUs?: number;
  indexed?: boolean;
  vectors: number[][];
}
const encoded = (value: string) => {
  if (Buffer.from(value).toString() !== value)
    throw new TypeError("Namespace and key must be valid Unicode");
  return Buffer.from(value).toString("base64url");
};
const prefix = (ns: string[]) => ns.map((p) => `${encoded(p)}/`).join("");
const key = (ns: string[], id: string) => `${prefix(ns)}!${encoded(id)}`;
function queryHits(response: unknown): Record<string, unknown>[] {
  const body = response as {
    responses?: { status?: number; error?: unknown; hits?: { hits?: Record<string, unknown>[] } }[];
  };
  if (body?.responses?.length !== 1) throw new Error("Expected one memory query response");
  const result = body.responses[0];
  if (
    result.error ||
    (result.status !== undefined && result.status !== 200) ||
    !Array.isArray(result.hits?.hits)
  )
    throw new Error("Invalid memory query response");
  return result.hits.hits;
}
const markerPrefix = (scope: string, ns: string[]) => `lgm1/${encoded(scope)}/${prefix(ns)}`;
class MemoryRecords extends AntflyJSONStore<StoredItem> {
  constructor(
    options: AntflyStoreOptions,
    private readonly vectorIndex?: string
  ) {
    super(options);
  }
  async mset(pairs: [string, StoredItem][]): Promise<void> {
    // Match Python: transforms preserve original creation and monotonic update time,
    // and the sticky multifield marker is committed with its owning memory.
    const transforms: NonNullable<BatchRequest["transforms"]> = [];
    for (const [key, row] of pairs) {
      jsonCopy(row);
      const fields = {
        namespace: row.namespace,
        key: row.key,
        value: row.value,
        vectors: row.vectors,
        indexed: row.vectors.length > 0,
      };
      const operations: NonNullable<BatchRequest["transforms"]>[number]["operations"] = [
        { op: "$setOnInsert", path: "$.value.createdAt", value: row.createdAt },
        { op: "$max", path: "$.value.updatedAtUs", value: Date.parse(row.updatedAt) * 1000 },
        { op: "$set", path: "$.codec", value: { version: 1, kind: "json" } },
        { op: "$set", path: "$.key", value: key },
        ...Object.entries(fields).map(([name, value]) => ({
          op: "$set" as const,
          path: `$.value.${name}`,
          value,
        })),
      ];
      if (this.vectorIndex)
        operations.push({
          op: "$set",
          path: `$._embeddings.${this.vectorIndex}`,
          value: row.vectors[0] ?? null,
        });
      transforms.push({ key: this.key(key), upsert: true, operations });
      if (row.vectors.length > 1)
        transforms.push({
          key: `${markerPrefix(this.namespace, row.namespace)}!`,
          upsert: true,
          operations: [{ op: "$set", path: "$.multifield", value: true }],
        });
    }
    if (!transforms.length) return;
    let result: Awaited<ReturnType<typeof this.client.tables.batchWithOptions>>;
    try {
      result = await this.client.tables.batchWithOptions(this.table, {
        transforms,
        sync_level: "full_index",
      });
    } catch (error) {
      throw new WriteOutcomeError(
        "unknown",
        pairs.map(([key]) => key),
        [],
        error
      );
    }
    if (result.status !== "committed")
      throw new WriteOutcomeError(
        result.status ?? "unknown",
        pairs.map(([key]) => key),
        [],
        result
      );
  }
  async hasMultifield(ns: string[]): Promise<boolean> {
    return (
      queryHits(
        await this.client.tables.query(this.table, {
          filter_prefix: markerPrefix(this.namespace, ns),
          limit: 1,
        })
      ).length > 0
    );
  }
  async nativeSearch(
    ns: string[],
    vector: number[],
    limit: number
  ): Promise<(Item & { score: number })[]> {
    if (!this.vectorIndex) throw new Error("Semantic search requires index embeddings");
    const request: TableQueryRequest = {
      filter_prefix: this.key(prefix(ns)),
      indexes: [this.vectorIndex],
      embeddings: { [this.vectorIndex]: vector },
      limit,
      filter_query: {
        term: { path: "/value/indexed", value: true },
      } as NonNullable<TableQueryRequest["filter_query"]>,
    };
    return queryHits(await this.client.tables.query(this.table, request)).map((hit) => {
      if (
        typeof hit._id !== "string" ||
        !hit._id.startsWith(this.key(prefix(ns))) ||
        typeof hit._distance !== "number" ||
        !Number.isFinite(hit._distance)
      )
        throw new Error("Invalid scoped memory vector result");
      const source = hit._source as {
        codec?: { kind: string; version: number };
        value: StoredItem;
      };
      if (
        source.codec?.kind !== "json" ||
        source.codec.version !== 1 ||
        !ns.every((part, i) => source.value.namespace[i] === part)
      )
        throw new Error("Memory result escaped namespace or codec");
      return { ...memoryItem(source.value), score: 1 - hit._distance };
    });
  }
}
function memoryItem(row: StoredItem): Item {
  return {
    namespace: [...row.namespace],
    key: row.key,
    value: jsonCopy(row.value),
    createdAt: new Date(row.createdAt),
    updatedAt: new Date(row.updatedAtUs === undefined ? row.updatedAt : row.updatedAtUs / 1000),
  };
}
function integer(value: number, minimum = 0): number {
  if (!Number.isSafeInteger(value) || value < minimum)
    throw new TypeError("Expected a bounded nonnegative integer");
  return value;
}
function textsAtPath(value: unknown, path: string): string[] {
  if (path === "$" || path === "") return [JSON.stringify(value)];
  const tokens = path.replace(/\[(-?\d+|\*)\]/g, ".$1").split(".");
  if (tokens.some((token) => !token || /[[\]]/.test(token)))
    throw new TypeError("Unsupported memory field path");
  let values: unknown[] = [value];
  for (const token of tokens) {
    values = values.flatMap((item) => {
      if (token === "*") return Array.isArray(item) ? item : [];
      if (!item || typeof item !== "object") return [];
      const part =
        Array.isArray(item) && /^-\d+$/.test(token)
          ? item.at(Number(token))
          : (item as Record<string, unknown>)[token];
      return part === undefined ? [] : [part];
    });
  }
  return values.map((item) => (typeof item === "string" ? item : JSON.stringify(item)));
}
function matches(value: Record<string, unknown>, filter: Record<string, unknown> = {}): boolean {
  return Object.entries(filter).every(([path, condition]) => {
    const actual = path
      .split(".")
      .reduce<unknown>(
        (v, part) =>
          v && typeof v === "object" ? (v as Record<string, unknown>)[part] : undefined,
        value
      );
    if (
      !condition ||
      typeof condition !== "object" ||
      Array.isArray(condition) ||
      !Object.keys(condition).some((k) => k.startsWith("$"))
    )
      return isDeepStrictEqual(actual, condition);
    return Object.entries(condition).every(([op, expected]) => {
      if (op === "$eq") return isDeepStrictEqual(actual, expected);
      if (op === "$ne") return !isDeepStrictEqual(actual, expected);
      if (!["$gt", "$gte", "$lt", "$lte"].includes(op))
        throw new TypeError(`Unsupported filter operator ${op}`);
      if (
        typeof actual !== typeof expected ||
        (typeof actual !== "number" && typeof actual !== "string")
      )
        return false;
      const a = actual as number,
        b = expected as number;
      return op === "$gt" ? a > b : op === "$gte" ? a >= b : op === "$lt" ? a < b : a <= b;
    });
  });
}
/** Ordered, durable batches. Not a checkpointer or a multi-operation transaction. */
export class AntflyStore extends BaseStore {
  private readonly storage: MemoryRecords;
  private readonly options: AntflyMemoryOptions;
  constructor(options: AntflyMemoryOptions) {
    super();
    this.options = options;
    this.storage = new MemoryRecords(
      {
        ...options,
        namespace: `langgraph/${options.namespace ?? ""}`,
      },
      options.index ? (options.indexName ?? "memory_vectors") : undefined
    );
    if (!(options.indexName ?? "memory_vectors") || (options.indexName ?? "").includes("."))
      throw new TypeError("indexName must be nonempty and contain no dots");
    if (options.index) integer(options.index.dims, 1);
  }
  static async create(options: AntflyMemoryOptions): Promise<AntflyStore> {
    const store = new AntflyStore(options);
    await options.client.tables.create(
      options.table,
      options.index
        ? {
            indexes: {
              [options.indexName ?? "memory_vectors"]: {
                type: "embeddings",
                external: true,
                dimension: options.index.dims,
                distance_metric: "cosine",
              },
            },
          }
        : {}
    );
    return store;
  }
  private vector(v: number[]): number[] {
    if (
      v.length !== this.options.index?.dims ||
      v.some((x) => !Number.isFinite(x)) ||
      !v.some((x) => x !== 0)
    )
      throw new Error("Invalid memory embedding");
    return v;
  }
  private item(row: StoredItem): Item {
    return memoryItem(row);
  }
  private async *rows(ns: string[] = []): AsyncGenerator<StoredItem> {
    for await (const [, row] of this.storage.entries(prefix(ns))) {
      if (ns.every((part, i) => row.namespace[i] === part)) yield row;
    }
  }

  private async searchItems(op: SearchOperation): Promise<(Item & { score?: number })[]> {
    validateNamespace(op.namespacePrefix, { allowEmpty: true });
    const offset = integer(op.offset ?? 0),
      limit = integer(op.limit ?? 10);
    if (op.query !== undefined && !this.options.index)
      throw new Error("Semantic search requires index embeddings");
    const query =
      op.query === undefined
        ? undefined
        : this.vector(await this.options.index!.embeddings.embedQuery(op.query));
    if (!limit) return [];
    // Server term/range predicates use array-membership semantics. Evaluate
    // value filters against stored JSON so all search paths agree before pagination.
    const filtered = Object.keys(op.filter ?? {}).length > 0;
    if (query && !filtered && !(await this.storage.hasMultifield(op.namespacePrefix))) {
      const native = await this.storage.nativeSearch(op.namespacePrefix, query, offset + limit);
      // Match Python's second check for a concurrent first multifield write.
      if (!(await this.storage.hasMultifield(op.namespacePrefix)))
        return native.slice(offset, offset + limit);
    }
    const result: (Item & { score?: number })[] = [];
    for await (const row of this.rows(op.namespacePrefix)) {
      if (!matches(row.value, op.filter)) continue;
      if (query && !row.vectors.length) continue;
      const score = query
        ? Math.max(
            ...row.vectors.map((vector) => {
              this.vector(vector);
              const dot = vector.reduce((s, v, i) => s + v * query[i], 0);
              return (
                dot /
                Math.sqrt(
                  vector.reduce((s, v) => s + v * v, 0) * query.reduce((s, v) => s + v * v, 0)
                )
              );
            })
          )
        : undefined;
      result.push({ ...this.item(row), ...(score === undefined ? {} : { score }) });
      if (query) {
        result.sort(
          (a, b) =>
            (b.score ?? 0) - (a.score ?? 0) ||
            key(a.namespace, a.key).localeCompare(key(b.namespace, b.key))
        );
        result.splice(offset + limit);
      } else if (result.length >= offset + limit) break;
    }
    if (query)
      result.sort(
        (a, b) =>
          (b.score ?? 0) - (a.score ?? 0) ||
          key(a.namespace, a.key).localeCompare(key(b.namespace, b.key))
      );
    return result.slice(offset, offset + limit);
  }
  private async namespaces(op: ListNamespacesOperation): Promise<string[][]> {
    const offset = integer(op.offset ?? 0),
      limit = integer(op.limit ?? 100);
    if (op.maxDepth !== undefined) integer(op.maxDepth, 1);
    const found = new Map<string, string[]>();
    for await (const row of this.rows()) {
      const ns = row.namespace;
      if (
        !(op.matchConditions ?? []).every((c) => {
          if (c.matchType !== "prefix" && c.matchType !== "suffix")
            throw new TypeError("Unknown namespace match type");
          const start = c.matchType === "prefix" ? 0 : ns.length - c.path.length;
          return start >= 0 && c.path.every((p, i) => p === "*" || ns[start + i] === p);
        })
      )
        continue;
      const shortened = ns.slice(0, op.maxDepth);
      found.set(JSON.stringify(shortened), shortened);
    }
    return [...found.values()]
      .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))
      .slice(offset, offset + limit);
  }
  async batch<Op extends Operation[]>(operations: Op): Promise<OperationResults<Op>> {
    const results: unknown[] = [];
    for (const op of operations) {
      if ("ttl" in op) throw new Error("Per-item TTL is unsupported");
      if ("namespace" in op) {
        validateNamespace(op.namespace);
        if (typeof op.key !== "string") throw new TypeError("Memory key must be a string");
        const id = key(op.namespace, op.key);
        if ("value" in op) {
          if (op.value === null) await this.storage.mdelete([id]);
          else {
            const value = jsonCopy(op.value);
            if (!value || Array.isArray(value) || typeof value !== "object")
              throw new TypeError("Memory value must be an object");
            const texts =
              op.index === false || !this.options.index
                ? []
                : (op.index ?? this.options.index.fields ?? ["$"]).flatMap((path) =>
                    textsAtPath(value, path)
                  );
            const vectors =
              texts.length && this.options.index
                ? await this.options.index.embeddings.embedDocuments(texts)
                : [];
            if (vectors.length !== texts.length) throw new Error("Memory embedding count mismatch");
            for (const vector of vectors) this.vector(vector);
            const now = new Date().toISOString();
            await this.storage.mset([
              [
                id,
                {
                  namespace: [...op.namespace],
                  key: op.key,
                  value,
                  createdAt: now,
                  updatedAt: now,
                  vectors,
                },
              ],
            ]);
          }
          results.push(undefined);
        } else {
          const [row] = await this.storage.mget([id]);
          results.push(row ? this.item(row) : null);
        }
      } else if ("namespacePrefix" in op) results.push(await this.searchItems(op));
      else results.push(await this.namespaces(op));
    }
    return results as OperationResults<Op>;
  }
}
