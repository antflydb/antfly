import type { IndexStatus, QueryRequest } from "@antfly/sdk";

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

export const DEFAULT_TABLE_QUERY_LIMIT = 3;

type ArtifactAwareIndexConfig = IndexStatus["config"] & {
  artifact_name?: string;
  field?: string;
  source_artifact_name?: string;
};

export interface ArtifactRetrievalDefaults {
  field: string;
  returnMatches: boolean;
}

export function artifactRetrievalDefaults(
  indexes: IndexStatus[],
  selectedVectorIndexes: string[]
): ArtifactRetrievalDefaults | null {
  const candidates =
    selectedVectorIndexes.length > 0
      ? indexes.filter(
          (index) =>
            index.config.type === "embeddings" && selectedVectorIndexes.includes(index.config.name)
        )
      : indexes.filter((index) => index.config.type === "full_text");

  for (const index of candidates) {
    const config = index.config as ArtifactAwareIndexConfig;
    const artifactEnrichment = config.enrichments?.find(
      (enrichment) =>
        (enrichment.kind === "chunk" || enrichment.kind === "asset") &&
        enrichment.full_text_index === true
    );
    const consumesArtifact =
      Boolean(config.artifact_name || config.source_artifact_name) ||
      config.enrichments?.some((enrichment) => Boolean(enrichment.source_artifact_name));

    if (artifactEnrichment || consumesArtifact) {
      return {
        field: artifactEnrichment?.field?.trim() || config.field?.trim() || "text",
        returnMatches: true,
      };
    }
  }
  return null;
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
  const raw = request.full_text_search as { query?: unknown } | undefined;
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

function fieldScopedTextQuery(field: string, query: string): string {
  const reservedOperator = ["AND", "OR", "NOT"].includes(query.toUpperCase());
  if (!reservedOperator && /^[\p{L}\p{N}_]+$/u.test(query)) {
    return `${field}:${query}`;
  }
  const quoted = query.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  return `${field}:"${quoted}"`;
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
    request.full_text_search = {
      query: artifactSearchField
        ? fieldScopedTextQuery(artifactSearchField, trimmedQuery)
        : trimmedQuery,
    };
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
  request.limit = typeof options?.limit === "number" ? options.limit : DEFAULT_TABLE_QUERY_LIMIT;
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
