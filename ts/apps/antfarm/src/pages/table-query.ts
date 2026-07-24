import type { QueryRequest } from "@antfly/sdk";

export interface TableQueryBuilderState {
  query: string;
  queryIndexes: string[];
  selectedFields: string[];
  semanticQuery: string;
  filterQuery: string;
  includeProfile: boolean;
}

export function buildTableQueryRequest({
  query,
  queryIndexes,
  selectedFields,
  semanticQuery,
  filterQuery,
  includeProfile,
}: TableQueryBuilderState): QueryRequest {
  const request: QueryRequest = {};
  const hasSemanticQuery = query.trim().length > 0 && queryIndexes.length > 0;

  if (hasSemanticQuery) {
    request.indexes = queryIndexes;
    request.semantic_search = query;
  }
  if (selectedFields.length > 0) {
    request.fields = selectedFields;
  }

  try {
    const options = JSON.parse(semanticQuery);
    request.aggregations = options.aggregations;
    request.limit = options.limit ?? 10;
    if (!hasSemanticQuery && options.offset !== undefined) {
      request.offset = options.offset;
    }
  } catch (error) {
    request.limit = 10;
    console.error("Invalid semantic query JSON:", error);
  }

  try {
    const filter = JSON.parse(filterQuery);
    if (Object.keys(filter).length > 0) {
      request.filter_query = filter;
    }
  } catch (error) {
    console.error("Invalid filter query JSON:", error);
  }

  request.profile = includeProfile;
  return request;
}
