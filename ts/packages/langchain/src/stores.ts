import type { AntflyClient, TableQueryRequest } from "@antfly/sdk";
import { Document } from "@langchain/core/documents";
import { BaseStore } from "@langchain/core/stores";
import { count, hits, isStatus, type OperationOptions, record, write } from "./utils.js";

export type JSONValue =
  | null
  | boolean
  | number
  | string
  | JSONValue[]
  | { [key: string]: JSONValue };
export function jsonCopy<T>(value: T): T {
  const active = new Set<object>();
  function check(v: unknown): void {
    if (v === null || typeof v === "string" || typeof v === "boolean") return;
    if (typeof v === "number" && Number.isFinite(v)) return;
    if (
      !v ||
      typeof v !== "object" ||
      active.has(v) ||
      (!Array.isArray(v) && Object.getPrototypeOf(v) !== Object.prototype)
    )
      throw new TypeError("Values must be acyclic JSON");
    if (
      Object.getOwnPropertySymbols(v).length ||
      (Array.isArray(v) && Array.from({ length: v.length }, (_, i) => i).some((i) => !(i in v)))
    )
      throw new TypeError("Values must be lossless JSON");
    active.add(v);
    for (const child of Object.values(v)) check(child);
    active.delete(v);
  }
  check(value);
  return JSON.parse(JSON.stringify(value)) as T;
}
export interface AntflyStoreOptions {
  client: AntflyClient;
  table: string;
  namespace?: string;
  batchSize?: number;
  pageSize?: number;
}
/** Constructors attach; create explicitly provisions a new table. */
export class AntflyJSONStore<V = JSONValue> extends BaseStore<string, V> {
  lc_namespace = ["antfly", "stores"];
  protected kind = "json";
  readonly client: AntflyClient;
  readonly table: string;
  readonly namespace: string;
  private readonly prefix: string;
  private readonly batchSize: number;
  private readonly pageSize: number;
  constructor(options: AntflyStoreOptions) {
    super(options);
    this.client = options.client;
    this.table = options.table;
    this.namespace = options.namespace ?? "";
    if (
      !this.table ||
      typeof this.namespace !== "string" ||
      Buffer.from(this.namespace).toString() !== this.namespace
    )
      throw new TypeError("Table and string namespace required");
    this.prefix = `lc/${Buffer.from(this.namespace).toString("hex")}/`;
    this.batchSize = count(options.batchSize ?? 64, "batchSize", 1);
    this.pageSize = count(options.pageSize ?? 100, "pageSize", 1);
  }
  static async create<T extends AntflyJSONStore<unknown>>(
    this: new (
      options: AntflyStoreOptions
    ) => T,
    options: AntflyStoreOptions
  ): Promise<T> {
    await options.client.tables.create(options.table, {});
    return new this(options);
  }
  protected key(key: string): string {
    if (typeof key !== "string" || Buffer.from(key).toString() !== key)
      throw new TypeError("Key must be valid Unicode");
    return this.prefix + Buffer.from(key).toString("hex");
  }
  protected encode(value: V): unknown {
    return jsonCopy(value);
  }
  protected decode(value: unknown): V {
    return jsonCopy(value) as V;
  }
  async mget(keys: string[]): Promise<(V | undefined)[]> {
    const result: (V | undefined)[] = [];
    for (const key of keys) {
      let row: Record<string, unknown>;
      try {
        row = record(await this.client.tables.lookup(this.table, this.key(key)));
      } catch (error) {
        if (!isStatus(error, 404)) throw error;
        await this.client.tables.get(this.table);
        result.push(undefined);
        continue;
      }
      const codec = record(row.codec, "store codec");
      if (codec.version !== 1 || codec.kind !== this.kind || row.key !== key)
        throw new Error("Incompatible store codec or key");
      result.push(this.decode(row.value));
    }
    return result;
  }
  async mset(pairs: [string, V][], options?: OperationOptions): Promise<void> {
    const rows: [string, unknown][] = pairs.map(([key, value]) => [
      this.key(key),
      { codec: { version: 1, kind: this.kind }, key, value: this.encode(value) },
    ]);
    if (new Set(rows.map(([key]) => key)).size !== rows.length)
      throw new TypeError("Duplicate keys in mset");
    await write(this.client, this.table, rows, [], this.batchSize, true, options);
  }
  async mdelete(keys: string[], options?: OperationOptions): Promise<void> {
    await write(
      this.client,
      this.table,
      [],
      keys.map((key) => this.key(key)),
      this.batchSize,
      true,
      options
    );
  }
  protected async *scan(prefix = ""): AsyncGenerator<[string, Record<string, unknown>]> {
    let cursor: TableQueryRequest["search_after"];
    let snapshot: TableQueryRequest["remote_snapshot"];
    const encoded = this.key(prefix);
    while (true) {
      const response = await this.client.tables.query(this.table, {
        filter_prefix: encoded,
        limit: this.pageSize,
        order_by: [{ field: "_id", desc: false }],
        search_after: cursor,
        remote_snapshot: snapshot,
      });
      const page = hits(response);
      const body = record(response);
      const first = Array.isArray(body.responses) ? record(body.responses[0]) : body;
      snapshot = (first.remote_snapshot as typeof snapshot) ?? snapshot;
      for (const hit of page) {
        if (typeof hit._id !== "string" || !hit._id.startsWith(encoded))
          throw new Error("Store query escaped namespace");
        const hex = hit._id.slice(this.prefix.length);
        const decoded = Buffer.from(hex, "hex").toString();
        if (this.key(decoded) !== hit._id) throw new Error("Invalid store key encoding");
        yield [decoded, hit];
      }
      if (!page.length) return;
      const next = page.at(-1)?._sort as typeof cursor;
      if (!next || JSON.stringify(next) === JSON.stringify(cursor)) {
        if (page.length >= this.pageSize) throw new Error("Store cursor did not advance");
        return;
      }
      cursor = next;
    }
  }
  async *yieldKeys(prefix = ""): AsyncGenerator<string> {
    for await (const [key] of this.scan(prefix)) yield key;
  }
  /** Decode values directly from query pages; no per-key lookup requests. */
  async *entries(prefix = ""): AsyncGenerator<[string, V]> {
    for await (const [key, hit] of this.scan(prefix)) {
      const row = record(hit._source, "store row");
      const codec = record(row.codec, "store codec");
      if (codec.version !== 1 || codec.kind !== this.kind || row.key !== key)
        throw new Error("Incompatible store codec or key");
      yield [key, this.decode(row.value)];
    }
  }
}
export class AntflyDocumentStore extends AntflyJSONStore<Document> {
  protected override kind = "document";
  protected override encode(value: Document): unknown {
    if (typeof value.pageContent !== "string") throw new TypeError("Document required");
    return jsonCopy({
      id: value.id ?? null,
      page_content: value.pageContent,
      metadata: value.metadata,
    });
  }
  protected override decode(value: unknown): Document {
    const row = record(value);
    if (typeof row.page_content !== "string" || (row.id !== null && typeof row.id !== "string"))
      throw new Error("Invalid document codec");
    return new Document({
      id: row.id ?? undefined,
      pageContent: row.page_content,
      metadata: record(jsonCopy(row.metadata)),
    });
  }
}
export class AntflyByteStore extends AntflyJSONStore<Uint8Array> {
  protected override kind = "bytes";
  private readonly maxBytes: number;
  constructor(options: AntflyStoreOptions & { maxValueBytes?: number }) {
    super(options);
    this.maxBytes = count(options.maxValueBytes ?? 8 * 1024 * 1024, "maxValueBytes", 1);
  }
  protected override encode(value: Uint8Array): string {
    if (!(value instanceof Uint8Array) || value.byteLength > this.maxBytes)
      throw new TypeError("Expected bytes within maxValueBytes");
    return Buffer.from(value).toString("base64");
  }
  protected override decode(value: unknown): Uint8Array {
    if (typeof value !== "string" || value.length > 4 * Math.ceil(this.maxBytes / 3))
      throw new Error("Invalid byte codec");
    const bytes = Buffer.from(value, "base64");
    if (bytes.length > this.maxBytes || bytes.toString("base64") !== value)
      throw new Error("Invalid base64 or oversized value");
    return bytes;
  }
}
