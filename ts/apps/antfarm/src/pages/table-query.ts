import type { IndexStatus, QueryRequest, TableStatus } from "@antfly/sdk";

export interface TableQueryBuilderState {
  query: string;
  queryIndexes: string[];
  selectedFields: string[];
  semanticQuery: string;
  filterQuery: string;
  includeProfile: boolean;
  artifactSearchFields?: string[];
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

export function tableQueryBuilderConversionBlocker(
  source: QueryRequest,
  rebuilt: QueryRequest
): string | null {
  if (JSON.stringify(canonicalJsonValue(source)) === JSON.stringify(canonicalJsonValue(rebuilt))) {
    return null;
  }
  return "The Builder cannot represent this JSON query without changing it. Keep using JSON, or remove unsupported controls before switching to Builder.";
}

function canonicalJsonValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalJsonValue);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .filter(([, item]) => item !== undefined)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => [key, canonicalJsonValue(item)])
  );
}

type ArtifactAwareIndexConfig = IndexStatus["config"] & {
  artifact_name?: string;
  embedding_name?: string;
  field?: string;
  source_artifact_name?: string;
};

type ArtifactEnrichment = NonNullable<TableStatus["artifact_enrichments"]>[number];

export interface ArtifactRetrievalDefaults {
  searchFields: string[];
  projectionFields: string[];
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
  const artifactSearchFields = new Set<string>();
  const artifactProjectionFields = new Set<string>();
  let hasSchemaUnknownAsset = false;
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
      const field = sourceEnrichment?.field?.trim() || "text";
      artifactSearchFields.add(field);
      artifactProjectionFields.add(field);
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
        if (enrichment.kind === "asset") {
          // Asset `field` identifies the source-document input, while the full-text
          // runtime indexes the produced artifact JSON directly. Its output fields
          // are not inferable without an output schema, so search `_all` and keep
          // the implicit projection identity-only.
          hasSchemaUnknownAsset = true;
          continue;
        }
        const field = enrichment.field?.trim() || config.field?.trim() || "text";
        artifactSearchFields.add(field);
        artifactProjectionFields.add(field);
      }
      artifactIndexCount++;
    } else if (config.artifact_name) {
      const field = config.field?.trim() || "text";
      artifactSearchFields.add(field);
      artifactProjectionFields.add(field);
      artifactIndexCount++;
    } else {
      ordinaryIndexCount++;
    }
  }

  if (artifactIndexCount === 0) return null;
  const searchFields = hasSchemaUnknownAsset ? ["_all"] : [...artifactSearchFields];
  const defaults: ArtifactRetrievalDefaults = {
    searchFields: searchFields.length > 0 ? searchFields : ["text"],
    projectionFields: [...artifactProjectionFields],
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

export function tableQueryInput(request: QueryRequest, artifactSearchFields?: string[]): string {
  if (typeof request.semantic_search === "string") {
    return request.semantic_search;
  }
  const artifactFields = [
    ...new Set(
      (artifactSearchFields ?? [])
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
  if (field !== "_all" && !reservedOperator && /^[\p{L}\p{N}_]+$/u.test(query)) {
    return { query: `${field}:${query}` };
  }
  return { match: query, field };
}

function normalizedArtifactFields(fields: string[], fallback: string[]): string[] {
  const normalized = Array.from(new Set(fields.map((field) => field.trim()).filter(Boolean)));
  return normalized.length > 0 ? normalized : fallback;
}

function normalizedProjectionFields(projectionFields?: string[]): string[] {
  if (projectionFields === undefined) return [];
  return Array.from(new Set(projectionFields.map((field) => field.trim()).filter(Boolean)));
}

export function buildTableQueryRequest({
  query,
  queryIndexes,
  selectedFields,
  semanticQuery,
  filterQuery,
  includeProfile,
  artifactSearchFields,
  artifactProjectionFields,
  returnArtifactMatches = false,
}: TableQueryBuilderState): QueryRequest {
  const request: QueryRequest = {};
  const trimmedQuery = query.trim();
  const hasSemanticQuery = trimmedQuery.length > 0 && queryIndexes.length > 0;
  const normalizedSearchFields = artifactSearchFields
    ? normalizedArtifactFields(artifactSearchFields, ["text"])
    : [];
  const normalizedProjection = normalizedProjectionFields(artifactProjectionFields);

  if (hasSemanticQuery) {
    request.indexes = queryIndexes;
    request.semantic_search = trimmedQuery;
  } else if (trimmedQuery.length > 0) {
    if (normalizedSearchFields.length > 0) {
      request.full_text_search = artifactFullTextQuery(normalizedSearchFields, trimmedQuery);
    } else {
      request.full_text_search = { query: trimmedQuery };
    }
  }
  if (selectedFields.length > 0) {
    request.fields = selectedFields;
  } else if (returnArtifactMatches) {
    request.fields = normalizedProjection;
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
