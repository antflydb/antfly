import type { AggregationBucket, QueryHit, QueryResult } from "@antfly/sdk";
import { type ReactNode, useEffect, useRef } from "react";
import { useSharedContext, type Widget } from "./SharedContext";
import { conjunctsFrom, defer, type MultiqueryRequest, multiquery, resolveTable } from "./utils";

interface ListenerProps {
  children: ReactNode;
  onChange?: (params: Map<string, unknown>) => void;
}

interface SearchWidgetConfig {
  itemsPerPage: number;
  page: number;
  sort?: string;
  fields?: string[];
}

interface FacetWidgetConfig {
  fields: string[];
  size: number;
  filterValue?: string;
  useCustomQuery?: boolean;
}

interface SemanticQueryConfig {
  indexes?: string[];
  limit?: number;
}

interface MSSearchItem {
  query: unknown;
  data: (result: QueryResult) => QueryHit[];
  facetData: (result: QueryResult) => AggregationBucket[];
  total: (result: QueryResult) => number;
  id: string;
}

interface ErrorResult {
  error: boolean;
  message: string;
}

interface QueryResponse {
  status: number;
  took: number;
  error?: string;
  hits?: {
    hits: QueryHit[];
    total: number;
  };
  aggregations?: Record<string, { buckets?: AggregationBucket[] }>;
}

interface MultiqueryResult {
  responses: QueryResponse[];
}

// Type guard function to check if configuration is a SearchWidgetConfig
function isSearchWidgetConfig(config: unknown): config is SearchWidgetConfig {
  return (
    config !== null &&
    typeof config === "object" &&
    typeof (config as SearchWidgetConfig).itemsPerPage === "number" &&
    typeof (config as SearchWidgetConfig).page === "number"
  );
}

export default function Listener({ children, onChange }: ListenerProps) {
  const [{ url, table, listenerEffect, widgets, headers }, dispatch] = useSharedContext();
  const debounceTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // We need to prepare some data in each render.
  // This needs to be done out of the effect function.
  function widgetThat(key: keyof Widget): Map<string, Widget> {
    return new Map([...widgets].filter(([, v]) => v[key]));
  }

  function mapFrom(key: keyof Widget): Map<string, unknown> {
    return new Map([...widgets].filter(([, v]) => v[key]).map(([k, v]) => [k, v[key]]));
  }

  const configurableWidgets = widgetThat("needsConfiguration");
  const facetWidgets = widgetThat("isFacet");
  const searchWidgets = widgetThat("needsQuery");
  const resultWidgets = widgetThat("wantResults");
  const queries = new Map([...widgets].filter(([, v]) => v.query).map(([k, v]) => [k, v.query]));
  const semanticQueries = new Map(
    [...widgets]
      .filter(([, v]) => v.semanticQuery && v.isSemantic)
      .map(([k, v]) => [
        k,
        {
          query: v.semanticQuery || "",
          indexes: (v.configuration as SemanticQueryConfig)?.indexes,
          limit: (v.configuration as SemanticQueryConfig)?.limit,
        },
      ])
  );
  const configurations = mapFrom("configuration");
  const values = mapFrom("value");

  const isAutosuggestWidget = (widgetId: string) => widgets.get(widgetId)?.isAutosuggest === true;

  function entriesForGroup<T>(entries: Iterable<[string, T]>, autosuggestOnly: boolean): Array<[string, T]> {
    return Array.from(entries)
      .filter(([widgetId]) => isAutosuggestWidget(widgetId) === autosuggestOnly)
      .sort();
  }

  // Track change groups using the actual widget inputs that drive requests.
  // This keeps autosuggest keystrokes isolated from submitted search widgets
  // while still invalidating correctly when configuration or backend context changes.
  const autosuggestGroupKey = JSON.stringify({
    context: { url, table, headers },
    queries: entriesForGroup(queries.entries(), true),
    semanticQueries: entriesForGroup(semanticQueries.entries(), true),
    configurations: entriesForGroup(configurations.entries(), true),
  });
  const nonAutosuggestGroupKey = JSON.stringify({
    context: { url, table, headers },
    queries: entriesForGroup(queries.entries(), false),
    semanticQueries: entriesForGroup(semanticQueries.entries(), false),
    configurations: entriesForGroup(configurations.entries(), false),
  });

  // Track previous group keys to determine which request families changed.
  const prevGroupKeysRef = useRef({
    autosuggest: "",
    nonAutosuggest: "",
  });

  useEffect(() => {
    // Apply custom callback effect on every change, useful for query params.
    if (onChange) {
      // Add pages to params.
      const pages = [...configurations]
        .filter(([, v]) => (v as SearchWidgetConfig)?.page && (v as SearchWidgetConfig).page > 1)
        .map(([k, v]) => [`${k}Page`, (v as SearchWidgetConfig).page]);
      // Run the change callback with all params.
      onChange(new Map([...pages, ...values] as Array<[string, unknown]>));
    }
    // Run the deferred (thx algolia) listener effect.
    if (listenerEffect) {
      listenerEffect();
    }
  });

  // Run effect on update for each change in queries or configuration.
  // We intentionally use stable JSON keys instead of Map objects/methods to prevent infinite re-renders.
  // The Maps are recreated on every render, so we use serialized keys to track actual content changes.
  // biome-ignore lint/correctness/useExhaustiveDependencies: Intentional - using stable JSON keys instead of Map objects to prevent infinite re-renders
  useEffect(() => {
    // Clear any existing timeout to debounce multiple rapid updates
    if (debounceTimeoutRef.current) {
      clearTimeout(debounceTimeoutRef.current);
    }

    // If you are debugging and your debug path leads you here, you might
    // check configurableWidgets and searchWidgets actually covers
    // the whole list of components that are configurables and queryable.
    const queriesReady = queries.size + semanticQueries.size === searchWidgets.size;
    const configurationsReady = configurations.size === configurableWidgets.size;
    const isAtLeastOneWidgetReady = searchWidgets.size + configurableWidgets.size > 0;

    if (queriesReady && configurationsReady && isAtLeastOneWidgetReady) {
      // Debounce to batch multiple widget updates into a single network call
      debounceTimeoutRef.current = setTimeout(() => {
        // The actual query to Antfly is deffered, to wait for all effects
        // and context operations before running.
        defer(() => {
          dispatch({
            type: "setListenerEffect",
            value: () => {
              // Determine which request groups changed to avoid re-sending stale queries.
              // Autosuggest updates should not invalidate submitted search results unless the
              // non-autosuggest request inputs actually changed.
              const prev = prevGroupKeysRef.current;
              const autosuggestChanged = autosuggestGroupKey !== prev.autosuggest;
              const nonAutosuggestChanged = nonAutosuggestGroupKey !== prev.nonAutosuggest;
              prevGroupKeysRef.current = {
                autosuggest: autosuggestGroupKey,
                nonAutosuggest: nonAutosuggestGroupKey,
              };

              const multiqueryData: MSSearchItem[] = [];
              resultWidgets.forEach((r, id) => {
                // Skip widgets whose query group hasn't changed
                if (r.isAutosuggest && !autosuggestChanged) return;
                if (!r.isAutosuggest && !nonAutosuggestChanged) return;
                const config = r.configuration;
                // Type guard to ensure configuration has required SearchWidgetConfig properties
                if (!isSearchWidgetConfig(config)) {
                  return; // Skip widgets without proper configuration
                }
                const { itemsPerPage, page, sort } = config;
                // For autosuggest widgets, only include their own semantic query (complete isolation)
                // For other widgets, exclude other autosuggest widgets
                const filteredSemanticQueries = r.isAutosuggest
                  ? [...semanticQueries.entries()].filter(([widgetId]) => widgetId === id)
                  : [...semanticQueries.entries()].filter(([widgetId]) => {
                      const widget = widgets.get(widgetId);
                      // Include this widget's own semantic query
                      if (widgetId === id) return true;
                      // Exclude other autosuggest widgets
                      return !widget?.isAutosuggest;
                    });

                const nonAutosuggestSemanticQueries = filteredSemanticQueries.map(([, v]) => v);
                const semanticQuery = nonAutosuggestSemanticQueries.map((v) => v.query).join(" ");
                const hasSemanticQuery = semanticQuery.length > 0;
                // Get the first indexes configured for the widget
                const indexes = nonAutosuggestSemanticQueries
                  .map((v) => v.indexes)
                  .filter((i) => i && Array.isArray(i) && i.length > 0)[0];

                // If this widget is an autosuggest, only include its own query (complete isolation)
                // Otherwise, if it's a root query, filter out other root queries
                const filteredQueries = r.isAutosuggest
                  ? new Map([...queries].filter(([queryId]) => queryId === id))
                  : r.rootQuery
                    ? new Map(
                        [...queries].filter(([queryId]) => {
                          const w = widgets.get(queryId);
                          // Always include this widget's own query
                          if (queryId === id) return true;
                          // Include non-root queries (facets), but exclude autosuggest widgets
                          return !w?.rootQuery && !w?.isAutosuggest;
                        })
                      )
                    : new Map(
                        [...queries].filter(([queryId]) => {
                          const w = widgets.get(queryId);
                          // For non-root query widgets, exclude only autosuggest widgets
                          return !w?.isAutosuggest;
                        })
                      );

                // Resolve table for this widget
                const tableName = resolveTable(r.table, table);

                // Build the query object
                const queryObj: Record<string, unknown> = {
                  table: tableName,
                  semantic_search: hasSemanticQuery ? semanticQuery : undefined,
                  indexes: hasSemanticQuery ? indexes : undefined,
                  limit: itemsPerPage,
                  offset: (page - 1) * itemsPerPage,
                  order_by: sort,
                };
                if (filteredQueries.size > 0 || !hasSemanticQuery) {
                  queryObj.full_text_search = conjunctsFrom(filteredQueries);
                }
                if (config.fields) {
                  queryObj.fields = config.fields;
                }
                if (r.filterQuery) {
                  queryObj.filter_query = r.filterQuery;
                }
                if (r.exclusionQuery) {
                  queryObj.exclusion_query = r.exclusionQuery;
                }
                // Add aggregation options if present (for autosuggest facets)
                if (r.facetOptions && r.facetOptions.length > 0) {
                  const aggregations: Record<
                    string,
                    { type: string; field: string; size: number }
                  > = {};
                  r.facetOptions.forEach((opt: { field: string; size?: number }) => {
                    aggregations[opt.field] = {
                      type: "terms",
                      field: opt.field,
                      size: opt.size || 5,
                    };
                  });
                  queryObj.aggregations = aggregations;
                }

                multiqueryData.push({
                  query: queryObj,
                  data: (result: QueryResult) => result.hits?.hits || [],
                  facetData: (result: QueryResult) => {
                    // Extract aggregation data if aggregations were requested
                    // For widgets with facetOptions (like autosuggest), return array of arrays
                    if (r.facetOptions && r.facetOptions.length > 0 && result.aggregations) {
                      return r.facetOptions.map((opt: { field: string }) => {
                        return result.aggregations?.[opt.field]?.buckets || [];
                      }) as unknown as AggregationBucket[];
                    }
                    return [];
                  },
                  total: (result: QueryResult) => result.hits?.total || 0,
                  id,
                });
              });

              // Fetch data for internal facet components (non-autosuggest only).
              facetWidgets.forEach((f, id) => {
                if (!nonAutosuggestChanged) return;
                // Resolve table for this widget
                const tableName = resolveTable(f.table, table);

                const config = f.configuration as FacetWidgetConfig;
                const fields = config.fields;
                const size = config.size;
                const filterValue = config.filterValue;
                const useCustomQuery = config.useCustomQuery;

                // Get the aggs (antfly queries) from fields
                // Dirtiest part, because we build a raw query from various params
                function aggsFromFields() {
                  // Remove current query from queries list (do not react to self)
                  function withoutOwnQueries() {
                    const q = new Map(queries);
                    q.delete(id);
                    return q;
                  }
                  // Transform a single field to agg query (using new aggregations API)
                  function aggFromField(field: string) {
                    const t = { type: "terms", field, size };
                    return { [field]: t };
                  }
                  // Actually build the query from fields
                  let result = {};
                  fields.forEach((f: string) => {
                    result = { ...result, ...aggFromField(f) };
                  });
                  // Join semanticQueries as a string, excluding autosuggest widgets except this one
                  const nonAutosuggestSemanticQueries = [...semanticQueries.entries()]
                    .filter(([widgetId]) => {
                      const widget = widgets.get(widgetId);
                      // Include this widget's own semantic query
                      if (widgetId === id) return true;
                      // Exclude other autosuggest widgets
                      return !widget?.isAutosuggest;
                    })
                    .map(([, v]) => v);
                  const semanticQuery = nonAutosuggestSemanticQueries.map((v) => v.query).join(" ");
                  // Get the first indexes configured for the widget
                  const indexes = nonAutosuggestSemanticQueries
                    .map((v) => v.indexes)
                    .filter((i) => i && Array.isArray(i) && i.length > 0)[0];
                  const limit = nonAutosuggestSemanticQueries.map((v) => v.limit)[0] || 10;

                  // Build query with custom query support
                  const baseQueries = withoutOwnQueries();

                  // For custom queries, only include queries from other facets (exclude searchbox)
                  const facetOnlyQueries = new Map(
                    [...baseQueries].filter(([queryId]) => {
                      const w = widgets.get(queryId);
                      return w?.isFacet === true; // Only include facet filters
                    })
                  );

                  const fullTextQuery =
                    useCustomQuery && f.query
                      ? conjunctsFrom(new Map([...facetOnlyQueries, [id, f.query]]))
                      : conjunctsFrom(baseQueries);

                  const facetQueryObj: Record<string, unknown> = {
                    table: tableName,
                    semantic_search: semanticQuery || undefined,
                    indexes: semanticQuery ? indexes : undefined,
                    limit: semanticQuery ? limit : 0,
                    full_text_search: fullTextQuery,
                    aggregations: result,
                  };
                  if (f.filterQuery) {
                    facetQueryObj.filter_query = f.filterQuery;
                  }
                  if (f.exclusionQuery) {
                    facetQueryObj.exclusion_query = f.exclusionQuery;
                  }

                  return facetQueryObj;
                }
                multiqueryData.push({
                  query: aggsFromFields(),
                  data: () => [],
                  facetData: (result: QueryResult) => {
                    // Merge aggs (if there is more than one for a facet),
                    // then remove duplicate and add count (sum),
                    // then sort and slice to get only 10 first.
                    const map = new Map();
                    // Safety check: ensure fields is an array before calling .map()
                    if (!fields || !Array.isArray(fields)) {
                      return [];
                    }
                    fields
                      .map((f: string) => {
                        if (
                          !result.aggregations ||
                          !result.aggregations[f] ||
                          !result.aggregations[f].buckets
                        ) {
                          return [];
                        }
                        // Only use filterValue for legacy mode (non-custom queries)
                        if (filterValue && !useCustomQuery) {
                          return result.aggregations[f].buckets?.filter((i: AggregationBucket) =>
                            i.key.toLowerCase().includes(filterValue.toLowerCase())
                          );
                        }
                        return result.aggregations[f].buckets;
                      })
                      .reduce((a: AggregationBucket[], b: AggregationBucket[]) => a.concat(b), [])
                      .forEach((i: AggregationBucket) => {
                        map.set(i.key, {
                          key: i.key,
                          doc_count: map.has(i.key)
                            ? i.doc_count + map.get(i.key).doc_count
                            : i.doc_count,
                        });
                      });
                    return [...map.values()]
                      .sort(
                        (x: AggregationBucket, y: AggregationBucket) => y.doc_count - x.doc_count
                      )
                      .slice(0, size);
                  },
                  total: (result: QueryResult) => result.hits?.total || 0,
                  id: id,
                });
              });

              // Fetch the data.
              async function fetchData() {
                // Only if there is a query to run.
                if (multiqueryData.length) {
                  try {
                    const msearchRequests: MultiqueryRequest[] = multiqueryData.map((item) => ({
                      query: item.query as Record<string, unknown>,
                    }));
                    const result = await multiquery(url || "", msearchRequests, headers || {});

                    // Handle connection error from multiquery
                    if (result && typeof result === "object" && "error" in result && result.error) {
                      console.error("Antfly connection error:", (result as ErrorResult).message);
                      // Set error state for all widgets
                      multiqueryData.forEach(({ id }) => {
                        dispatch({
                          type: "setWidgetResult",
                          key: id,
                          isLoading: false,
                          result: {
                            data: [],
                            facetData: [],
                            total: 0,
                            error: (result as ErrorResult).message,
                          },
                        });
                      });
                      return;
                    }

                    const responses = (result as MultiqueryResult)?.responses;
                    if (responses) {
                      responses.forEach((response: QueryResponse, key: number) => {
                        const id = multiqueryData[key].id;
                        if (response.status !== 200) {
                          console.error("Antfly response error:", response.error);
                          dispatch({
                            type: "setWidgetResult",
                            key: id,
                            isLoading: false,
                            result: {
                              data: [],
                              facetData: [],
                              total: 0,
                              error: response.error || "Query failed",
                            },
                          });
                        } else {
                          dispatch({
                            type: "setWidgetResult",
                            key: id,
                            isLoading: false,
                            result: {
                              data: multiqueryData[key].data(response),
                              facetData: multiqueryData[key].facetData(response),
                              total: multiqueryData[key].total(response),
                            },
                          });
                        }
                      });
                    }
                  } catch (error) {
                    console.error("Unexpected error during Antfly query:", error);
                    // Set error state for all widgets
                    multiqueryData.forEach(({ id }) => {
                      dispatch({
                        type: "setWidgetResult",
                        key: id,
                        isLoading: false,
                        result: {
                          data: [],
                          facetData: [],
                          total: 0,
                          error: "Unexpected error occurred",
                        },
                      });
                    });
                  }
                }
              }
              fetchData();
              // Destroy the effect listener to avoid infinite loop!
              dispatch({ type: "setListenerEffect", value: null });
            },
          });
        });
      }, 15); // 15ms debounce delay to batch rapid updates
    }

    // Cleanup timeout on unmount
    return () => {
      if (debounceTimeoutRef.current) {
        clearTimeout(debounceTimeoutRef.current);
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    dispatch,
    url,
    headers,
    searchWidgets.size,
    configurableWidgets.size,
    facetWidgets.size,
    resultWidgets.size,
    autosuggestGroupKey,
    nonAutosuggestGroupKey,
    semanticQueries.size,
    table,
    widgets.size,
    // listenerEffect removed to prevent infinite loop
    // Note: We use stable JSON keys instead of Map objects/methods
    // to avoid constant re-renders while still tracking changes
  ]);

  return <>{children}</>;
}
