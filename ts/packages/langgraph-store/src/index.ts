import { isDeepStrictEqual } from "node:util";
import { AntflyJSONStore, type AntflyStoreOptions, jsonCopy } from "@antfly/langchain";
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
  index?: { embeddings: EmbeddingsInterface; dims: number; fields?: string[] };
}
interface StoredItem {
  namespace: string[];
  key: string;
  value: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
  vectors: number[][];
}
const encoded = (value: string) => {
  if (Buffer.from(value).toString() !== value)
    throw new TypeError("Namespace and key must be valid Unicode");
  return Buffer.from(value).toString("base64url");
};
const prefix = (ns: string[]) => ns.map((p) => `${encoded(p)}/`).join("");
const key = (ns: string[], id: string) => `${prefix(ns)}!${encoded(id)}`;
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
  private readonly storage: AntflyJSONStore<StoredItem>;
  private readonly options: AntflyMemoryOptions;
  constructor(options: AntflyMemoryOptions) {
    super();
    this.options = options;
    this.storage = new AntflyJSONStore({
      ...options,
      namespace: `langgraph/${options.namespace ?? ""}`,
    });
    if (options.index) integer(options.index.dims, 1);
  }
  static async create(options: AntflyMemoryOptions): Promise<AntflyStore> {
    await options.client.tables.create(options.table, {});
    return new AntflyStore(options);
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
    return {
      namespace: [...row.namespace],
      key: row.key,
      value: jsonCopy(row.value),
      createdAt: new Date(row.createdAt),
      updatedAt: new Date(row.updatedAt),
    };
  }
  private async *rows(ns: string[] = []): AsyncGenerator<StoredItem> {
    for await (const id of this.storage.yieldKeys(prefix(ns))) {
      const [row] = await this.storage.mget([id]);
      if (row && ns.every((part, i) => row.namespace[i] === part)) yield row;
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
            const [old] = await this.storage.mget([id]);
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
                  createdAt: old?.createdAt ?? now,
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
