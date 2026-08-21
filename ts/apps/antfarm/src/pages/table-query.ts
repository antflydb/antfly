import type { IndexStatus, QueryRequest, TableStatus } from "@antfly/sdk";

export interface TableQueryBuilderState {
  query: string;
  queryIndexes: string[];
  selectedFields: string[];
  semanticQuery: string;
  filterQuery: string;
  includeProfile: boolean;
  artifactSearchField?: string;
  artifactProjectionFields?: string[];
  returnArtifactMatches?: boolean;
}

export const DEFAULT_TABLE_QUERY_LIMIT = 10;
export const DEFAULT_ARTIFACT_QUERY_LIMIT = 3;

export type TableQueryMetadataState = "loading" | "ready" | "error";

export function tableQueryMetadataBlocker(
  indexesState: TableQueryMetadataState,
  tableState: TableQueryMetadataState
): string | null {
  if (indexesState === "ready" && tableState === "ready") return null;
  if (indexesState === "error" || tableState === "error") {
    return "Query metadata could not be loaded safely. Retry before using the query builder, or use JSON mode with an explicit fields projection.";
  }
  return "Loading query metadata before enabling safe queries.";
}

export function tableQueryJsonSafetyBlocker(
  metadataBlocker: string | null,
  artifactProjectionRequired: boolean,
  request: QueryRequest | null
): string | null {
  if ((!metadataBlocker && !artifactProjectionRequired) || !request) return null;
  const fields = (request as { fields?: unknown }).fields;
  if (Array.isArray(fields) && fields.every((field) => typeof field === "string")) return null;
  if (!metadataBlocker) {
    return 'Artifact-backed JSON queries require an explicit "fields" array; use "fields": [] for identity-only results.';
  }
  return 'Query metadata is not ready. Add an explicit "fields" array before running this JSON query; use "fields": [] for identity-only results.';
}

type ArtifactAwareIndexConfig = IndexStatus["config"] & {
  artifact_name?: string;
  embedding_name?: string;
  field?: string;
  source_artifact_name?: string;
};

type ArtifactEnrichment = NonNullable<TableStatus["artifact_enrichments"]>[number];

export interface ArtifactRetrievalDefaults {
  field: string;
  fields: string[];
  returnMatches: boolean;
  selectionError?: string;
}

export function artifactRetrievalDefaults(
  indexes: IndexStatus[],
  selectedVectorIndexes: string[],
  tableStatus?: TableStatus | null
): ArtifactRetrievalDefaults | null {
  // The list endpoint owns membership. Table status supplies richer enrichment
  // metadata, but may briefly lag a create/drop operation.
  const configs = new Map(indexes.map((index) => [index.config.name, index.config]));
  const candidateNames =
    selectedVectorIndexes.length > 0
      ? selectedVectorIndexes.filter((name) => configs.get(name)?.type === "embeddings")
      : [...configs].filter(([, config]) => config.type === "full_text").map(([name]) => name);

  const tableEnrichments = tableStatus?.artifact_enrichments ?? [];
  const artifactFields = new Set<string>();
  let artifactIndexCount = 0;
  let ordinaryIndexCount = 0;

  for (const name of candidateNames) {
    const config = configs.get(name) as ArtifactAwareIndexConfig | undefined;
    if (!config) continue;

    const enrichments = structuredEnrichments(
      tableStatus?.indexes?.[name]?.enrichments ?? config.enrichments
    );
    if (config.type === "embeddings") {
      if (!config.source_artifact_name) {
        ordinaryIndexCount++;
        continue;
      }
      const sourceEnrichment = findEmbeddingSourceEnrichment(config, enrichments, tableEnrichments);
      artifactFields.add(sourceEnrichment?.field?.trim() || "text");
      artifactIndexCount++;
      continue;
    }

    // A table-level full-text artifact enrichment is relevant to full-text
    // candidates, but must never turn an unrelated vector index into an
    // artifact-backed index.
    const allEnrichments = [...enrichments, ...tableEnrichments];
    const artifactEnrichments = allEnrichments.filter(
      (enrichment) =>
        (enrichment.kind === "chunk" || enrichment.kind === "asset") &&
        (config.artifact_name
          ? enrichment.name === config.artifact_name
          : enrichment.full_text_index === true)
    );

    if (artifactEnrichments.length > 0) {
      for (const enrichment of artifactEnrichments) {
        artifactFields.add(enrichment.field?.trim() || config.field?.trim() || "text");
      }
      artifactIndexCount++;
    } else if (config.artifact_name) {
      artifactFields.add(config.field?.trim() || "text");
      artifactIndexCount++;
    } else {
      ordinaryIndexCount++;
    }
  }

  if (artifactIndexCount === 0) return null;
  const fields = [...artifactFields];
  const defaults: ArtifactRetrievalDefaults = {
    field: fields[0] ?? "text",
    fields,
    returnMatches: true,
  };
  if (selectedVectorIndexes.length > 0 && ordinaryIndexCount > 0) {
    defaults.selectionError =
      "Artifact-backed and document-backed vector indexes cannot be searched together. Select indexes that return the same result type.";
  }
  return defaults;
}

function structuredEnrichments(enrichments: unknown): ArtifactEnrichment[] {
  if (!Array.isArray(enrichments)) return [];
  return enrichments.filter(
    (enrichment): enrichment is ArtifactEnrichment =>
      typeof enrichment === "object" && enrichment !== null
  );
}

function findEmbeddingSourceEnrichment(
  config: ArtifactAwareIndexConfig,
  indexEnrichments: ArtifactEnrichment[],
  tableEnrichments: ArtifactEnrichment[]
): ArtifactEnrichment | undefined {
  const enrichments = [...indexEnrichments, ...tableEnrichments];
  return (
    enrichments.find(
      (enrichment) => enrichment.kind === "embedding" && enrichment.name === config.embedding_name
    ) ??
    enrichments.find(
      (enrichment) =>
        enrichment.kind === "embedding" &&
        enrichment.source_artifact_name === config.source_artifact_name
    )
  );
}

function parseJsonObject(source: string): Record<string, unknown> | null {
  try {
    const value: unknown = JSON.parse(source);
    return value !== null && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

export function parseTableQueryRequest(source: string): QueryRequest | null {
  return parseJsonObject(source) as QueryRequest | null;
}

export function tableQueryInput(
  request: QueryRequest,
  artifactSearchFields?: string | string[]
): string {
  if (typeof request.semantic_search === "string") {
    return request.semantic_search;
  }
  const artifactFields = [
    ...new Set(
      (Array.isArray(artifactSearchFields) ? artifactSearchFields : [artifactSearchFields])
        .filter((field): field is string => typeof field === "string")
        .map((field) => field.trim())
        .filter(Boolean)
    ),
  ];
  const artifactSearchField = artifactFields[0];
  const raw = request.full_text_search as
    | { disjuncts?: unknown; field?: unknown; match?: unknown; query?: unknown }
    | undefined;
  const disjunctionMatch = simpleArtifactDisjunctionMatch(raw, artifactFields);
  if (disjunctionMatch !== null) {
    return disjunctionMatch;
  }
  if (
    typeof raw?.match === "string" &&
    (artifactSearchField === undefined ||
      raw.field === undefined ||
      raw.field === artifactSearchField)
  ) {
    return raw.match;
  }
  if (typeof raw?.query !== "string") {
    return "";
  }
  if (!artifactSearchField) {
    return raw.query;
  }
  const prefix = `${artifactSearchField}:`;
  if (!raw.query.startsWith(prefix)) {
    return raw.query;
  }
  const value = raw.query.slice(prefix.length);
  if (value.startsWith('"') && value.endsWith('"')) {
    try {
      const parsed: unknown = JSON.parse(value);
      if (typeof parsed === "string") return parsed;
    } catch {
      // Keep the field-scoped expression editable when it is not JSON quoting.
    }
  }
  return value;
}

function simpleArtifactDisjunctionMatch(
  raw: { disjuncts?: unknown; field?: unknown; match?: unknown; query?: unknown } | undefined,
  artifactFields: string[]
): string | null {
  if (!raw || artifactFields.length < 2 || !Array.isArray(raw.disjuncts)) return null;
  if (Object.keys(raw).some((key) => key !== "disjuncts")) return null;

  const fields = new Set<string>();
  let match: string | undefined;
  for (const disjunct of raw.disjuncts) {
    if (!disjunct || typeof disjunct !== "object" || Array.isArray(disjunct)) return null;
    const clause = disjunct as Record<string, unknown>;
    if (
      Object.keys(clause).length !== 2 ||
      typeof clause.field !== "string" ||
      typeof clause.match !== "string"
    ) {
      return null;
    }
    if (match !== undefined && match !== clause.match) return null;
    match = clause.match;
    fields.add(clause.field);
  }

  const expectedFields = new Set(artifactFields);
  if (
    match === undefined ||
    fields.size !== raw.disjuncts.length ||
    fields.size !== expectedFields.size ||
    [...fields].some((field) => !expectedFields.has(field))
  ) {
    return null;
  }
  return match;
}

export function tableQueryErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message.trim()) return error.message.trim();
  if (typeof error === "string" && error.trim()) return error.trim();
  if (error && typeof error === "object") {
    const problem = error as Record<string, unknown>;
    for (const field of ["detail", "title", "error", "message"] as const) {
      const value = problem[field];
      if (typeof value === "string" && value.trim()) return value.trim();
    }
  }
  return fallback;
}

function artifactFullTextQuery(fields: string[], query: string): QueryRequest["full_text_search"] {
  if (fields.length > 1) {
    return {
      disjuncts: fields.map((field) => ({ match: query, field })),
    };
  }
  const field = fields[0] ?? "text";
  const reservedOperator = ["AND", "OR", "NOT"].includes(query.toUpperCase());
  if (!reservedOperator && /^[\p{L}\p{N}_]+$/u.test(query)) {
    return { query: `${field}:${query}` };
  }
  return { match: query, field };
}

function normalizedArtifactFields(primaryField: string, projectionFields?: string[]): string[] {
  const fields = Array.from(
    new Set(
      (projectionFields?.length ? projectionFields : [primaryField])
        .map((field) => field.trim())
        .filter(Boolean)
    )
  );
  return fields.length > 0 ? fields : [primaryField.trim() || "text"];
}

export function buildTableQueryRequest({
  query,
  queryIndexes,
  selectedFields,
  semanticQuery,
  filterQuery,
  includeProfile,
  artifactSearchField,
  artifactProjectionFields,
  returnArtifactMatches = false,
}: TableQueryBuilderState): QueryRequest {
  const request: QueryRequest = {};
  const trimmedQuery = query.trim();
  const hasSemanticQuery = trimmedQuery.length > 0 && queryIndexes.length > 0;
  const artifactFields = artifactSearchField
    ? normalizedArtifactFields(artifactSearchField, artifactProjectionFields)
    : [];

  if (hasSemanticQuery) {
    request.indexes = queryIndexes;
    request.semantic_search = trimmedQuery;
  } else if (trimmedQuery.length > 0) {
    if (artifactSearchField) {
      request.full_text_search = artifactFullTextQuery(artifactFields, trimmedQuery);
    } else {
      request.full_text_search = { query: trimmedQuery };
    }
  }
  if (selectedFields.length > 0) {
    request.fields = selectedFields;
  } else if (returnArtifactMatches && artifactSearchField) {
    request.fields = artifactFields;
  }
  if (returnArtifactMatches) {
    request.hierarchy = {};
  }

  const options = parseJsonObject(semanticQuery);
  if (options?.aggregations !== undefined) {
    request.aggregations = options.aggregations as QueryRequest["aggregations"];
  }
  request.limit =
    typeof options?.limit === "number"
      ? options.limit
      : returnArtifactMatches
        ? DEFAULT_ARTIFACT_QUERY_LIMIT
        : DEFAULT_TABLE_QUERY_LIMIT;
  if (!hasSemanticQuery && typeof options?.offset === "number") {
    request.offset = options.offset;
  }

  const filter = parseJsonObject(filterQuery);
  if (filter && Object.keys(filter).length > 0) {
    request.filter_query = filter as QueryRequest["filter_query"];
  }

  if (includeProfile) {
    request.profile = true;
  }
  return request;
}
