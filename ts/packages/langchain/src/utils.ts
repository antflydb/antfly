import { type AntflyClient, AntflyHTTPError, type TableQueryRequest } from "@antfly/sdk";
import { Document, type DocumentInterface } from "@langchain/core/documents";

export type AntflyFilter = NonNullable<TableQueryRequest["filter_query"]>;
export type DistanceMetric = "cosine" | "l2_squared" | "inner_product";
export interface OperationOptions {
  signal?: AbortSignal;
}
export function count(value: number, name: string, minimum = 0): number {
  if (!Number.isSafeInteger(value) || value < minimum)
    throw new TypeError(`${name} must be an integer >= ${minimum}`);
  return value;
}
export function record(value: unknown, name = "response"): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new TypeError(`Invalid ${name}`);
  return value as Record<string, unknown>;
}
export function vectors(value: number[][], length: number, dimension?: number): number[][] {
  if (!Array.isArray(value) || value.length !== length)
    throw new TypeError("Embedding count mismatch");
  const size = dimension ?? value[0]?.length;
  for (const vector of value) {
    if (
      !Array.isArray(vector) ||
      !vector.length ||
      vector.length !== size ||
      vector.some((v) => typeof v !== "number" || !Number.isFinite(v))
    ) {
      throw new TypeError("Embeddings must be finite dense vectors of the expected dimension");
    }
  }
  return value;
}
export function intersect(a?: AntflyFilter, b?: AntflyFilter): AntflyFilter | undefined {
  return a && b ? { conjuncts: [a, b] } : (a ?? b);
}
export function isStatus(error: unknown, status: number): boolean {
  return error instanceof AntflyHTTPError && error.status === status;
}
export function hits(value: unknown): Record<string, unknown>[] {
  const body = record(value);
  const responses = body.responses ?? [body];
  if (!Array.isArray(responses) || responses.length !== 1)
    throw new Error("Expected one table query response");
  const response = record(responses[0]);
  if (response.error || (response.status !== undefined && response.status !== 200))
    throw new Error(`Antfly query failed: ${JSON.stringify(response)}`);
  const group = response.hits;
  const found = Array.isArray(group) ? group : record(group, "hits").hits;
  if (!Array.isArray(found)) throw new Error("Antfly response omitted hits");
  return found.map((hit) => record(hit, "hit"));
}
export function document(
  hit: Record<string, unknown>,
  contentField: string,
  metadataField: string,
  provenance = false
): Document {
  const source = record(hit._source, "document source");
  if (typeof source[contentField] !== "string" || typeof hit._id !== "string")
    throw new Error("Hit is missing document content or ID");
  const metadata = { ...record(source[metadataField] ?? {}, "document metadata") };
  if ("_antfly" in metadata)
    throw new Error("metadata._antfly is reserved for retrieval provenance");
  if (provenance)
    metadata._antfly = Object.fromEntries(
      ["_score", "_distance", "_table", "_index_scores"]
        .filter((key) => hit[key] !== undefined)
        .map((key) => [key.slice(1), hit[key]])
    );
  return new Document({ id: hit._id, pageContent: source[contentField] as string, metadata });
}
export function validateDocuments(documents: DocumentInterface[]): void {
  for (const doc of documents) {
    if (typeof doc.pageContent !== "string")
      throw new TypeError("Document content must be a string");
    const metadata = record(doc.metadata, "metadata");
    if ("_antfly" in metadata) throw new TypeError("metadata._antfly is reserved");
    // Reject lossy values rather than silently changing stored metadata.
    const seen = new Set<object>();
    const visit = (value: unknown): void => {
      if (value === null || typeof value === "string" || typeof value === "boolean") return;
      if (typeof value === "number" && Number.isFinite(value)) return;
      if (
        typeof value !== "object" ||
        !value ||
        seen.has(value) ||
        (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype)
      )
        throw new TypeError("Metadata must be acyclic JSON");
      seen.add(value);
      for (const item of Object.values(value)) visit(item);
      seen.delete(value);
    };
    visit(metadata);
  }
}
export class WriteOutcomeError extends Error {
  constructor(
    public readonly status: string,
    public readonly keys: string[],
    public readonly committedKeys: string[],
    public readonly details: unknown
  ) {
    super(`Antfly write outcome ${status}; inspect stored data and index status before retrying`);
    this.name = "WriteOutcomeError";
  }
  get committed(): boolean | undefined {
    return this.status.startsWith("committed") ? true : undefined;
  }
}
export async function write(
  client: AntflyClient,
  table: string,
  rows: [string, unknown][],
  deletes: string[],
  batchSize: number,
  wait: boolean,
  options?: OperationOptions
): Promise<void> {
  const operations = [
    ...rows.map(([key, value]) => ({ key, value, delete: false })),
    ...deletes.map((key) => ({ key, value: undefined, delete: true })),
  ];
  const committed: string[] = [];
  for (let i = 0; i < operations.length; i += batchSize) {
    const batch = operations.slice(i, i + batchSize);
    const keys = batch.map((op) => op.key);
    // Once earlier batches committed, cancellation must still report their identities.
    if (options?.signal?.aborted)
      throw new WriteOutcomeError("not_submitted", keys, committed, options.signal.reason);
    let result: Awaited<ReturnType<AntflyClient["tables"]["batch"]>>;
    try {
      result = await client.tables.batchWithOptions(
        table,
        {
          inserts: Object.fromEntries(
            batch.filter((op) => !op.delete).map((op) => [op.key, op.value])
          ),
          deletes: batch.filter((op) => op.delete).map((op) => op.key),
          sync_level: wait ? "full_index" : "write",
        },
        options
      );
    } catch (error) {
      throw new WriteOutcomeError("unknown", keys, committed, error);
    }
    if (result.status !== "committed" && !(result.status === "committed_pending" && !wait)) {
      throw new WriteOutcomeError(result.status ?? "unknown", keys, committed, result);
    }
    committed.push(...keys);
  }
}
