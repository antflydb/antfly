import { createHash } from "node:crypto";
import { type AntflyClient, AntflyHTTPError, type TableQueryRequest } from "@antfly/sdk";
import { Document, type DocumentInterface } from "@langchain/core/documents";
import { jsonCopy } from "./stores.js";
import { count, hits, mapConcurrent, record, WriteOutcomeError } from "./utils.js";

export interface GraphNode {
  id: string | number;
  type: string;
  properties?: Record<string, unknown>;
}
export interface GraphRelationship {
  source: GraphNode;
  target: GraphNode;
  type: string;
  properties?: Record<string, unknown>;
}
export interface GraphDocument {
  nodes: GraphNode[];
  relationships: GraphRelationship[];
  source: DocumentInterface;
}
export interface AntflyGraphOptions {
  client: AntflyClient;
  table: string;
  index?: string;
  namespace?: string;
  maxResults?: number;
  maxDepth?: number;
  maxRetries?: number;
}
type ObjectValue = Record<string, any>;
const encode = (value: string) => {
  if (Buffer.from(value).toString() !== value)
    throw new TypeError("Graph identities must be valid Unicode");
  return Buffer.from(value).toString("base64url");
};
const hash = (value: string) => createHash("sha256").update(value).digest("hex");
/** Canonical Antfly graph JSON; deliberately does not accept Cypher. */
export class AntflyGraphStore {
  readonly client: AntflyClient;
  readonly table: string;
  readonly index: string;
  readonly namespace: string;
  readonly maxResults: number;
  readonly maxDepth: number;
  readonly prefix: string;
  private readonly maxRetries: number;
  structuredSchema: Record<string, unknown> = {
    dialect: "antfly-graph-json-v1",
    discovery_complete: false,
  };
  constructor(options: AntflyGraphOptions) {
    this.client = options.client;
    this.table = options.table;
    this.index = options.index ?? "graph";
    this.namespace = options.namespace ?? "";
    if (!this.table || !this.index) throw new TypeError("table and index required");
    this.maxResults = count(options.maxResults ?? 100, "maxResults", 1);
    this.maxDepth = count(options.maxDepth ?? 8, "maxDepth", 1);
    this.maxRetries = count(options.maxRetries ?? 4, "maxRetries", 1);
    if (this.maxResults > 10000 || this.maxDepth > 64 || this.maxRetries > 16)
      throw new TypeError("Graph bounds exceeded");
    this.prefix = `lcg1/${encode(this.namespace)}/`;
  }
  static async create(options: AntflyGraphOptions): Promise<AntflyGraphStore> {
    const store = new AntflyGraphStore(options);
    await store.client.tables.create(store.table, {
      indexes: {
        [store.index]: { type: "graph", edge_types: [] },
        [`${store.index}_predicates`]: { type: "algebraic", derive_from_schema: true },
      },
      schema: {
        default_type: "doc",
        document_schemas: {
          doc: {
            schema: {
              type: "object",
              additionalProperties: true,
              properties: {
                graph_namespace: { type: "string", "x-antfly-types": ["keyword"] },
                graph_kind: { type: "string", "x-antfly-types": ["keyword"] },
              },
            },
          },
        },
      },
    });
    return store;
  }
  getSchema(): string {
    return JSON.stringify(this.structuredSchema);
  }
  nodeKey(type: string, id: string | number): string {
    if (!type || (typeof id !== "string" && !(typeof id === "number" && Number.isFinite(id))))
      throw new TypeError("Invalid graph entity identity");
    return `${this.prefix}n/${encode(type)}/${encode(JSON.stringify(id))}`;
  }
  private async read(
    key: string,
    checkTable = true
  ): Promise<{ row: ObjectValue; version: string }> {
    const { data, error, response } = await this.client
      .getRawClient()
      .GET("/db/v1/tables/{tableName}/documents/{key}", {
        params: { path: { tableName: this.table, key }, query: { consistency: "read_index" } },
      });
    if (response.status === 404) {
      if (checkTable) await this.client.tables.get(this.table);
      return { row: {}, version: "0" };
    }
    if (error) throw new AntflyHTTPError("Graph read failed", response.status, error);
    const version = response.headers.get("x-antfly-version");
    if (!version) throw new Error("Graph lookup omitted OCC version");
    return { row: record(data), version };
  }
  private async updateSource(
    source: string,
    contributions?: Record<string, ObjectValue>
  ): Promise<void> {
    const manifest = `${this.prefix}s/${encode(source)}`;
    for (let attempt = 0; attempt < this.maxRetries; attempt++) {
      const old = await this.read(manifest);
      const keys = [
        ...new Set<string>([...(old.row.nodes ?? []), ...Object.keys(contributions ?? {})]),
      ].sort();
      if (keys.length > 4095) throw new Error("Source exceeds 4095 nodes");
      const read_set = [{ table: this.table, key: manifest, version: old.version }];
      const inserts: Record<string, any> = {};
      const deletes: string[] = [];
      const priorRows = await mapConcurrent(keys, 8, (key) => this.read(key, false));
      for (const [i, key] of keys.entries()) {
        const prior = priorRows[i];
        read_set.push({ table: this.table, key, version: prior.version });
        const owners: Record<string, ObjectValue> = { ...prior.row.contributions };
        if (contributions?.[key])
          owners[hash(source)] = { ...contributions[key], source_id: source };
        else delete owners[hash(source)];
        const ordered = Object.values(owners).sort((a, b) =>
          a.source_id < b.source_id ? -1 : a.source_id > b.source_id ? 1 : 0
        );
        if (!ordered.length) {
          deletes.push(key);
          continue;
        }
        const properties: ObjectValue = {},
          edges: Record<string, ObjectValue[]> = {};
        for (const owner of ordered) {
          Object.assign(properties, owner.properties);
          for (const edge of owner.edges) {
            const { type, ...body } = edge;
            edges[type] ??= [];
            edges[type].push(body);
          }
        }
        inserts[key] = {
          graph_kind: "node",
          graph_namespace: this.namespace,
          graph_type: ordered[0].type,
          graph_id: ordered[0].id,
          properties,
          contributions: owners,
          _edges: { [this.index]: edges },
        };
      }
      if (contributions)
        inserts[manifest] = {
          graph_kind: "source",
          graph_namespace: this.namespace,
          source_id: source,
          nodes: Object.keys(contributions).sort(),
        };
      else deletes.push(manifest);
      let result: { data?: unknown; error?: unknown };
      try {
        result = await this.client.getRawClient().POST("/db/v1/transactions/commit", {
          body: {
            read_set,
            tables: { [this.table]: { inserts, deletes } },
            sync_level: "full_index",
          },
        });
      } catch (error) {
        throw new WriteOutcomeError("unknown", [...Object.keys(inserts), ...deletes], [], error);
      }
      const raw = result.data ?? result.error;
      const outcome = raw && typeof raw === "object" ? (raw as ObjectValue) : {};
      if (outcome?.status === "aborted") continue;
      if (
        !outcome.tables ||
        Object.keys(outcome.tables).length !== 1 ||
        !(this.table in outcome.tables)
      )
        throw new WriteOutcomeError("unknown", [...Object.keys(inserts), ...deletes], [], outcome);
      if (outcome.tables[this.table]?.status !== "committed")
        throw new WriteOutcomeError(
          outcome.tables[this.table]?.status ?? "unknown",
          [...Object.keys(inserts), ...deletes],
          [],
          outcome
        );
      if (outcome?.status !== "committed")
        throw new WriteOutcomeError(
          outcome?.status ?? "unknown",
          [...Object.keys(inserts), ...deletes],
          [],
          outcome
        );
      this.structuredSchema.discovery_complete = false;
      return;
    }
    throw new Error(
      "Graph source update conflicted repeatedly; no mutation replayed after an ambiguous outcome"
    );
  }
  async addGraphDocuments(
    documents: GraphDocument[],
    options: { includeSource?: boolean } = {}
  ): Promise<void> {
    for (const graph of documents) {
      const source = String(
        graph.source.metadata.id ?? graph.source.id ?? hash(graph.source.pageContent)
      );
      const contributions: Record<string, ObjectValue> = {};
      for (const node of [
        ...graph.nodes,
        ...graph.relationships.flatMap((e) => [e.source, e.target]),
      ]) {
        const key = this.nodeKey(node.type, node.id);
        contributions[key] ??= { type: node.type, id: node.id, properties: {}, edges: [] };
        Object.assign(contributions[key].properties, jsonCopy(node.properties ?? {}));
      }
      for (const edge of graph.relationships) {
        if (!edge.type) throw new TypeError("Relationship type required");
        const start = this.nodeKey(edge.source.type, edge.source.id),
          target = this.nodeKey(edge.target.type, edge.target.id);
        const properties = jsonCopy(edge.properties ?? {});
        contributions[start].edges.push({
          type: edge.type,
          target,
          weight: 1,
          edge_id: hash(JSON.stringify([source, start, target, edge.type, properties.id ?? ""])),
          metadata: { ...properties, source_id: source },
        });
      }
      if (options.includeSource)
        contributions[this.nodeKey("__source__", source)] = {
          type: "__source__",
          id: source,
          properties: {
            page_content: graph.source.pageContent,
            metadata: jsonCopy(graph.source.metadata),
          },
          edges: Object.keys(contributions).map((target) => ({
            type: "MENTIONS",
            target,
            weight: 1,
            edge_id: hash(source + target),
            metadata: { source_id: source },
          })),
        };
      await this.updateSource(source, contributions);
    }
  }
  async deleteSource(source: string): Promise<void> {
    await this.updateSource(source);
  }
  private endpoint(value: ObjectValue): void {
    if (
      (value.table !== undefined && value.table !== this.table) ||
      typeof value.key !== "string" ||
      !value.key.startsWith(`${this.prefix}n/`)
    )
      throw new Error("Graph endpoint escaped table or namespace");
    if (
      value.document?.graph_namespace !== undefined &&
      value.document.graph_namespace !== this.namespace
    )
      throw new Error("Graph document escaped namespace");
  }
  private filter(existing?: unknown): ObjectValue {
    return {
      conjuncts: [
        { term: this.namespace, path: "/graph_namespace" },
        { term: "node", path: "/graph_kind" },
        ...(existing ? [existing] : []),
      ],
    };
  }
  private scoped(queries: ObjectValue): ObjectValue {
    const copy = jsonCopy(queries);
    if (!Object.keys(copy).length || Object.keys(copy).length > 8)
      throw new Error("Expected 1 to 8 graph operations");
    for (const op of Object.values(copy) as ObjectValue[]) {
      const kinds = ["match", "traverse", "shortest_path", "k_shortest_paths"].filter(
        (k) => k in op
      );
      if (
        kinds.length !== 1 ||
        Object.keys(op).some((k) => ![...kinds, "index", "return"].includes(k))
      )
        throw new Error("Invalid canonical graph operation");
      if (op.index !== undefined && op.index !== this.index)
        throw new Error("Graph index is fixed");
      op.index = this.index;
      const kind = kinds[0],
        body = op[kind];
      if (kind === "match") {
        if (
          !body.nodes ||
          !(body.anchor in body.nodes) ||
          "_antfly_graph" in body.nodes ||
          body.optional ||
          body.where
        )
          throw new Error(
            "MATCH requires explicit aliases; optional and WHERE expressions are unsupported"
          );
        for (const node of Object.values(body.nodes) as ObjectValue[]) {
          if (node.table !== undefined && node.table !== this.table)
            throw new Error("Cross-table MATCH forbidden");
          node.table = this.table;
          node.filter = this.filter(node.filter);
        }
        if ((body.edges ?? []).some((e: ObjectValue) => (e.max_hops ?? 1) !== 1))
          throw new Error("MATCH must explicitly bind every intermediate node");
        op.return ??= { bindings: Object.keys(body.nodes) };
        if (
          op.return.bindings?.includes("_antfly_graph") ||
          (op.return.aggregates && "_antfly_graph" in op.return.aggregates)
        )
          throw new Error("_antfly_graph is reserved");
        if (!op.return.aggregates)
          op.return.limit = Math.min(
            count(op.return.limit ?? this.maxResults, "limit", 1),
            this.maxResults
          );
      } else {
        body.filter = this.filter(body.filter);
        body.max_depth = Math.min(
          count(body.max_depth ?? this.maxDepth, "max_depth", 1),
          this.maxDepth
        );
        if (kind === "traverse") {
          const start = record(body.start);
          if (
            Object.keys(start).length !== 1 ||
            (!Array.isArray(start.keys) && !Array.isArray(start.identities))
          )
            throw new Error("Explicit traversal starts required");
          for (const endpoint of (start.identities as ObjectValue[] | undefined) ??
            (start.keys as string[]).map((key) => ({ key })))
            this.endpoint(record(endpoint));
          body.limit = Math.min(count(body.limit ?? this.maxResults, "limit", 1), this.maxResults);
          body.include_paths = true;
        } else {
          this.endpoint(record(body.from));
          this.endpoint(record(body.to));
          if (kind === "k_shortest_paths")
            body.k = Math.min(count(body.k ?? 1, "k", 1), this.maxResults);
        }
      }
    }
    return copy;
  }
  async queryGraph(queries: ObjectValue): Promise<ObjectValue[]> {
    const scoped = this.scoped(queries);
    const response = record(
      await this.client.tables.query(this.table, { graph_queries: scoped, limit: 0 })
    );
    const responses = response.responses ?? [response];
    if (!Array.isArray(responses) || responses.length !== 1)
      throw new Error("Expected one graph table response");
    const body = record(responses[0]);
    if (body.error || (body.status !== undefined && body.status !== 200))
      throw new Error("Graph query failed");
    const results = record(body.graph_results);
    if (Object.keys(results).sort().join() !== Object.keys(scoped).sort().join())
      throw new Error("Missing graph operation results");
    const rows: ObjectValue[] = [];
    for (const [name, raw] of Object.entries(results)) {
      const result = record(raw) as ObjectValue,
        op = scoped[name];
      if (
        result.complete === false ||
        ![undefined, "complete", "completed", "success", 200].includes(result.status)
      )
        throw new Error("Incomplete graph operation");
      const kind = ["match", "traverse", "shortest_path", "k_shortest_paths"].find((k) => k in op)!;
      const aggregate = kind === "match" && op.return?.aggregates;
      const field = aggregate
        ? "aggregates"
        : kind === "match"
          ? "rows"
          : kind === "traverse"
            ? "nodes"
            : "paths";
      if (["aggregates", "rows", "nodes", "paths"].filter((k) => k in result).join() !== field)
        throw new Error("Unexpected graph projection");
      if (aggregate) {
        if (
          result.stats?.truncated ||
          Object.keys(result.aggregates).sort().join() !== Object.keys(aggregate).sort().join() ||
          Object.values(result.aggregates).some(
            (v: any) => v.exact !== true || typeof v.value !== "string" || !/^\d+$/.test(v.value)
          )
        )
          throw new Error("Inexact graph aggregate");
      }
      const projected = aggregate ? [result.aggregates] : result[field];
      if (!Array.isArray(projected)) throw new Error("Missing graph rows");
      for (const row of projected) {
        if (!aggregate) {
          if (kind === "match") {
            if (Object.keys(row).sort().join() !== [...op.return.bindings].sort().join())
              throw new Error("Incomplete bindings");
            for (const value of Object.values(row)) this.endpoint(record(value));
          } else {
            const nodes = kind === "traverse" ? row.path : row.path?.nodes;
            const edges = kind === "traverse" ? row.path_edges : row.path?.edges;
            const length = kind === "traverse" ? row.depth : row.path?.length;
            if (
              !Array.isArray(nodes) ||
              !nodes.length ||
              nodes.length !== length + 1 ||
              !Array.isArray(edges) ||
              edges.length !== length
            )
              throw new Error("Missing complete graph path evidence");
            for (const node of nodes) this.endpoint(record(node));
            edges.forEach((e) => {
              this.endpoint(record(e.from));
              this.endpoint(record(e.to));
            });
            if (kind === "traverse") this.endpoint(row);
          }
        }
        rows.push({
          ...row,
          _antfly_graph: { operation: name, truncated: result.stats?.truncated ?? false },
        });
      }
    }
    return rows;
  }
  async query(query: string, params: Record<string, unknown> = {}): Promise<ObjectValue[]> {
    const used = new Set<string>();
    const bind = (value: any): any => {
      if (Array.isArray(value)) return value.map(bind);
      if (value && typeof value === "object") {
        if ("$param" in value) {
          if (Object.keys(value).length !== 1 || !(value.$param in params))
            throw new Error("Invalid graph parameter");
          used.add(value.$param);
          return jsonCopy(params[value.$param]);
        }
        return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, bind(v)]));
      }
      return value;
    };
    const parsed = bind(JSON.parse(query));
    if (Object.keys(params).some((k) => !used.has(k))) throw new Error("Unused graph parameter");
    return this.queryGraph(parsed);
  }
  async traverse(keys: string[], maxDepth = 1): Promise<ObjectValue[]> {
    return this.queryGraph({ neighbors: { traverse: { start: { keys }, max_depth: maxDepth } } });
  }
  async expandDocuments(
    documents: DocumentInterface[],
    options: { seedKeys?: string[]; maxDepth?: number; limit?: number } = {}
  ): Promise<DocumentInterface[]> {
    const keys = options.seedKeys ?? documents.map((doc) => doc.metadata.graph_node_key ?? doc.id);
    if (!keys.length) return [];
    if (keys.some((key) => typeof key !== "string"))
      throw new Error("Graph expansion requires explicit node identities");
    const rows = await this.queryGraph({
      neighbors: {
        traverse: {
          start: { keys },
          max_depth: options.maxDepth ?? 1,
          limit: options.limit ?? 20,
          include_documents: true,
        },
      },
    });
    const seen = new Set(keys);
    const result = [...documents];
    for (const row of rows) {
      if (seen.has(row.key)) continue;
      seen.add(row.key);
      const document = record(row.document, "graph expansion document");
      const properties = record(document.properties ?? document);
      const text = properties.page_content ?? properties.text ?? properties.body;
      result.push(
        new Document({
          id: row.key,
          pageContent: typeof text === "string" ? text : JSON.stringify(properties),
          metadata: { _antfly: { graph: row._antfly_graph, depth: row.depth, properties } },
        })
      );
    }
    return result;
  }
  async refreshSchema(): Promise<void> {
    const types = new Set<string>(),
      relationships = new Set<string>();
    const properties: Record<string, Set<string>> = {};
    let cursor: TableQueryRequest["search_after"];
    let snapshot: TableQueryRequest["remote_snapshot"];
    while (true) {
      const response = await this.client.tables.query(this.table, {
        filter_prefix: `${this.prefix}n/`,
        limit: this.maxResults,
        order_by: [{ field: "_id", desc: false }],
        search_after: cursor,
        remote_snapshot: snapshot,
      });
      const page = hits(response);
      const body = record(response);
      const first = Array.isArray(body.responses) ? record(body.responses[0]) : body;
      snapshot = (first.remote_snapshot as typeof snapshot) ?? snapshot;
      for (const hit of page) {
        const row = record(hit._source) as ObjectValue;
        types.add(row.graph_type);
        properties[row.graph_type] ??= new Set();
        Object.keys(row.properties).forEach((k) => {
          properties[row.graph_type].add(k);
        });
        for (const c of Object.values(row.contributions) as ObjectValue[])
          c.edges.forEach((e: ObjectValue) => {
            relationships.add(e.type);
          });
      }
      if (page.length < this.maxResults) break;
      const next = page.at(-1)?._sort as typeof cursor;
      if (!next || JSON.stringify(next) === JSON.stringify(cursor))
        throw new Error("Schema cursor failed to advance");
      cursor = next;
    }
    this.structuredSchema = {
      dialect: "antfly-graph-json-v1",
      namespace: this.namespace,
      node_types: [...types].sort(),
      node_properties: Object.fromEntries(
        Object.entries(properties).map(([k, v]) => [k, [...v].sort()])
      ),
      relationship_types: [...relationships].sort(),
      discovery_complete: true,
    };
  }
}
