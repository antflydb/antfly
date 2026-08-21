import type { IndexStatus, QueryRequest, TableStatus } from "@antfly/sdk";

export interface TableQueryBuilderState {
  query: string;
  queryIndexes: string[];
  selectedFields: string[];
  semanticQuery: string;
  filterQuery: string;
  includeProfile: boolean;
  artifactSearchField?: string;
  returnArtifactMatches?: boolean;
}

export const DEFAULT_TABLE_QUERY_LIMIT = 10;
export const DEFAULT_ARTIFACT_QUERY_LIMIT = 3;

type ArtifactAwareIndexConfig = IndexStatus["config"] & {
  artifact_name?: string;
  embedding_name?: string;
  field?: string;
  source_artifact_name?: string;
};

type ArtifactEnrichment = NonNullable<TableStatus["artifact_enrichments"]>[number];

export interface ArtifactRetrievalDefaults {
  field: string;
  returnMatches: boolean;
}

export function artifactRetrievalDefaults(
  indexes: IndexStatus[],
  selectedVectorIndexes: string[],
  tableStatus?: TableStatus | null
): ArtifactRetrievalDefaults | null {
  const configs = new Map(indexes.map((index) => [index.config.name, index.config]));
  for (const [name, config] of Object.entries(tableStatus?.indexes ?? {})) {
    configs.set(name, config);
  }
  const candidateNames =
    selectedVectorIndexes.length > 0
      ? selectedVectorIndexes.filter((name) => configs.get(name)?.type === "embeddings")
      : [...configs].filter(([, config]) => config.type === "full_text").map(([name]) => name);

  const tableEnrichments = tableStatus?.artifact_enrichments ?? [];

  for (const name of candidateNames) {
    const config = configs.get(name) as ArtifactAwareIndexConfig | undefined;
    if (!config) continue;

    const enrichments = structuredEnrichments(config.enrichments);
    if (config.type === "embeddings" && config.source_artifact_name) {
      const sourceEnrichment = findEmbeddingSourceEnrichment(config, enrichments, tableEnrichments);
      return {
        field: sourceEnrichment?.field?.trim() || "text",
        returnMatches: true,
      };
    }

    const allEnrichments = [...enrichments, ...tableEnrichments];
    const artifactEnrichment =
      allEnrichments.find(
        (enrichment) =>
          (enrichment.kind === "chunk" || enrichment.kind === "asset") &&
          enrichment.name === config.artifact_name
      ) ??
      allEnrichments.find(
        (enrichment) =>
          (enrichment.kind === "chunk" || enrichment.kind === "asset") &&
          enrichment.full_text_index === true
      );

    if (artifactEnrichment || config.artifact_name) {
      return {
        field: artifactEnrichment?.field?.trim() || config.field?.trim() || "text",
        returnMatches: true,
      };
    }
  }
  return null;
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

export function tableQueryInput(request: QueryRequest, artifactSearchField?: string): string {
  if (typeof request.semantic_search === "string") {
    return request.semantic_search;
  }
  const raw = request.full_text_search as
    | { field?: unknown; match?: unknown; query?: unknown }
    | undefined;
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

function artifactFullTextQuery(field: string, query: string): QueryRequest["full_text_search"] {
  const reservedOperator = ["AND", "OR", "NOT"].includes(query.toUpperCase());
  if (!reservedOperator && /^[\p{L}\p{N}_]+$/u.test(query)) {
    return { query: `${field}:${query}` };
  }
  return { match: query, field };
}

export function buildTableQueryRequest({
  query,
  queryIndexes,
  selectedFields,
  semanticQuery,
  filterQuery,
  includeProfile,
  artifactSearchField,
  returnArtifactMatches = false,
}: TableQueryBuilderState): QueryRequest {
  const request: QueryRequest = {};
  const trimmedQuery = query.trim();
  const hasSemanticQuery = trimmedQuery.length > 0 && queryIndexes.length > 0;

  if (hasSemanticQuery) {
    request.indexes = queryIndexes;
    request.semantic_search = trimmedQuery;
  } else if (trimmedQuery.length > 0) {
    request.full_text_search = artifactSearchField
      ? artifactFullTextQuery(artifactSearchField, trimmedQuery)
      : { query: trimmedQuery };
  }
  if (selectedFields.length > 0) {
    request.fields = selectedFields;
  } else if (returnArtifactMatches && artifactSearchField) {
    request.fields = [artifactSearchField];
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
