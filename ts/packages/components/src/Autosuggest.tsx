import type { AggregationBucket, QueryHit } from "@antfly/sdk";
import React, {
  Children,
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
} from "react";
import { useSharedContext } from "./SharedContext";
import { conjunctsFrom, disjunctsFrom } from "./utils";

// Context for sharing state between Autosuggest parent and children
export interface AutosuggestContextValue {
  query: string;
  results: QueryHit[];
  facetData: Map<string, AggregationBucket[]>;
  selectedIndex: number;
  handleSelect: (value: string) => void;
  isLoading: boolean;
  registerItem: (id: string) => number;
  unregisterItem: (id: string) => void;
  fields?: string[];
}

const AutosuggestContext = createContext<AutosuggestContextValue | null>(null);

// eslint-disable-next-line react-refresh/only-export-components
export function useAutosuggestContext() {
  const context = useContext(AutosuggestContext);
  if (!context) {
    throw new Error("Component must be used within Autosuggest");
  }
  return context;
}

export interface AutosuggestProps {
  fields?: string[];
  returnFields?: string[];
  limit?: number;
  minChars?: number;
  debounceMs?: number; // Debounce delay for query triggering (default: 300ms, 0 to disable)
  renderSuggestion?: (hit: QueryHit) => ReactNode;
  customQuery?: (value?: string, fields?: string[]) => unknown;
  semanticIndexes?: string[];
  table?: string; // Optional table override (Phase 1: single table only)
  filterQuery?: Record<string, unknown>; // Filter query to constrain autocomplete suggestions
  exclusionQuery?: Record<string, unknown>; // Exclusion query to exclude matches
  // Internal props passed from SearchBox
  searchValue?: string;
  onSuggestionSelect?: (hit: QueryHit) => void;
  containerRef?: React.RefObject<HTMLDivElement | null>;
  isOpen?: boolean;
  // Composable children
  children?: ReactNode;
  layout?: "vertical" | "horizontal" | "grid" | "custom";
  className?: string;
  dropdownClassName?: string;
}

export interface AutosuggestResultsProps {
  limit?: number;
  renderItem?: (hit: QueryHit, index: number) => ReactNode;
  onSelect?: (hit: QueryHit) => void;
  filter?: (hit: QueryHit) => boolean;
  className?: string;
  itemClassName?: string;
  selectedItemClassName?: string;
  header?: ReactNode | ((count: number) => ReactNode);
  footer?: ReactNode;
  emptyMessage?: ReactNode;
}

export interface AutosuggestFacetsProps {
  field: string;
  size?: number;
  label?: string;
  order?: "count" | "term" | "reverse_count" | "reverse_term";
  renderItem?: (facet: AggregationBucket, index: number) => ReactNode;
  renderSection?: (field: string, label: string, terms: AggregationBucket[]) => ReactNode;
  onSelect?: (facet: AggregationBucket) => void;
  clickable?: boolean;
  filter?: (facet: AggregationBucket) => boolean;
  className?: string;
  itemClassName?: string;
  sectionClassName?: string;
  header?: ReactNode | ((field: string, label: string) => ReactNode);
  footer?: ReactNode;
  emptyMessage?: ReactNode;
}

export default function Autosuggest({
  fields,
  returnFields,
  limit = 10,
  minChars = 2,
  debounceMs = 300,
  renderSuggestion,
  customQuery,
  semanticIndexes,
  table,
  filterQuery,
  exclusionQuery,
  searchValue = "",
  onSuggestionSelect,
  containerRef,
  isOpen: isOpenProp,
  children,
  layout = "vertical",
  className,
  dropdownClassName,
}: AutosuggestProps) {
  const isSemanticEnabled = semanticIndexes && semanticIndexes.length > 0;
  // Default returnFields to fields if not specified
  const effectiveReturnFields = returnFields ?? fields;
  const [{ widgets }, dispatch] = useSharedContext();
  const [selectedIndex, setSelectedIndex] = useState(-1);
  const [isOpenOverride, setIsOpenOverride] = useState<boolean | null>(null);
  const suggestionsRef = useRef<HTMLUListElement | HTMLDivElement>(null);
  const justSelectedRef = useRef(false);
  const prevSearchValueRef = useRef(searchValue);
  const debounceTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const id = `autosuggest-${useId()}`;

  // Item registration for keyboard navigation
  const itemsRef = useRef<Map<string, number>>(new Map());

  // Scan children to collect facet configurations
  const facetConfigs = useMemo(() => {
    if (!children) return [];
    const configs: Array<{ field: string; size: number }> = [];
    Children.forEach(children, (child) => {
      if (React.isValidElement(child) && child.type === AutosuggestFacets) {
        const props = child.props as AutosuggestFacetsProps;
        configs.push({ field: props.field, size: props.size || 5 });
      }
    });
    return configs;
  }, [children]);

  // Get suggestions and facet data from widget result
  const widget = widgets.get(id);

  // Use hits directly as suggestions
  const suggestions: QueryHit[] = useMemo(() => {
    const rawData = (widget?.result?.data as QueryHit[]) || [];
    return rawData.slice(0, limit);
  }, [widget?.result?.data, limit]);

  // Extract facet data from widget result
  const facetData = useMemo(() => {
    const data = new Map<string, AggregationBucket[]>();
    if (widget?.result?.facetData) {
      // facetData is an array of arrays, one per facet configuration
      facetConfigs.forEach((config, index) => {
        const facetTerms = widget.result?.facetData?.[index] as AggregationBucket[] | undefined;
        if (facetTerms) {
          data.set(config.field, facetTerms);
        }
      });
    }
    return data;
  }, [widget, facetConfigs]);

  // Derive isOpen from searchValue, with ability to override
  // Priority: isOpenProp (from parent) > isOpenOverride (internal) > shouldShow (default)
  const shouldShow = searchValue.length >= minChars;
  const isOpen =
    isOpenProp !== undefined ? isOpenProp : isOpenOverride !== null ? isOpenOverride : shouldShow;

  // Update widget configuration when searchValue changes (with debouncing)
  useEffect(() => {
    // Reset override and selection when search value changes
    if (prevSearchValueRef.current !== searchValue) {
      prevSearchValueRef.current = searchValue;
      if (!justSelectedRef.current) {
        // Use a microtask to avoid setState during render
        Promise.resolve().then(() => {
          setIsOpenOverride(null);
          setSelectedIndex(-1);
        });
      }
      justSelectedRef.current = false;
    }

    // Clear any pending debounced query
    if (debounceTimeoutRef.current) {
      clearTimeout(debounceTimeoutRef.current);
      debounceTimeoutRef.current = null;
    }

    const shouldShowNow = searchValue.length >= minChars;

    // Function to dispatch the query
    const dispatchQuery = () => {
      if (shouldShowNow) {
        // Determine if this autosuggest can actually query
        // It needs either: semantic indexes, custom query, or non-empty fields
        const canQuery =
          isSemanticEnabled ||
          customQuery !== undefined ||
          (Array.isArray(fields) && fields.length > 0);

        // Build the base query
        let baseQuery: unknown = null;
        if (isSemanticEnabled) {
          baseQuery = customQuery ? customQuery() : null;
        } else if (customQuery) {
          baseQuery = customQuery(searchValue, fields);
        } else if (Array.isArray(fields) && fields.length > 0) {
          baseQuery = disjunctsFrom(
            fields.map((field) => {
              // TODO (ajr) Do we want match_phrase or make a match_phrase_prefix?
              // if (field.includes(" ")) return {};
              if (field.endsWith("__keyword")) return { prefix: searchValue, field };
              if (field.endsWith("__2gram")) return { match: searchValue, field };
              return { match: searchValue, field };
            })
          );
        }

        // Wrap with filterQuery if provided (for non-semantic queries)
        const finalQuery =
          !isSemanticEnabled && baseQuery && filterQuery
            ? conjunctsFrom(
                new Map([
                  ["filter", filterQuery],
                  ["query", baseQuery],
                ])
              )
            : baseQuery;

        // Build facet options if facet children are present
        const facetOptions =
          facetConfigs.length > 0
            ? facetConfigs.map((config) => ({
                field: config.field,
                size: config.size,
              }))
            : undefined;

        // Register widget to fetch its own query results
        dispatch({
          type: "setWidget",
          key: id,
          needsQuery: canQuery,
          needsConfiguration: canQuery,
          isFacet: false,
          rootQuery: true,
          isAutosuggest: true,
          isSemantic: isSemanticEnabled,
          wantResults: canQuery,
          wantFacets: facetConfigs.length > 0,
          query: finalQuery,
          semanticQuery: isSemanticEnabled ? searchValue : undefined,
          table: table,
          filterQuery: filterQuery,
          exclusionQuery: exclusionQuery,
          facetOptions: facetOptions,
          configuration: canQuery
            ? isSemanticEnabled
              ? {
                  indexes: Array.isArray(semanticIndexes) ? semanticIndexes : [],
                  limit,
                  itemsPerPage: limit,
                  page: 1,
                  fields: effectiveReturnFields,
                }
              : {
                  fields: effectiveReturnFields,
                  size: limit,
                  itemsPerPage: limit,
                  page: 1,
                }
            : undefined,
          // Don't clear result to prevent flashing - keep previous results visible while loading
          // result: undefined,
          isLoading: true, // Mark as loading to keep component mounted
        });
      } else {
        // Clear suggestions when search value is too short
        dispatch({
          type: "setWidget",
          key: id,
          needsQuery: false,
          needsConfiguration: false,
          isFacet: false,
          rootQuery: true,
          isAutosuggest: true,
          isSemantic: isSemanticEnabled,
          wantResults: false,
          table: table,
          result: { data: [], total: 0 },
        });
      }
    };

    // Apply debouncing if enabled, otherwise dispatch immediately
    if (debounceMs > 0 && shouldShowNow) {
      debounceTimeoutRef.current = setTimeout(dispatchQuery, debounceMs);
    } else {
      // Dispatch immediately when:
      // - debouncing is disabled (debounceMs === 0)
      // - search value is below minChars (need to clear suggestions immediately)
      dispatchQuery();
    }

    // Cleanup timeout on effect re-run
    return () => {
      if (debounceTimeoutRef.current) {
        clearTimeout(debounceTimeoutRef.current);
        debounceTimeoutRef.current = null;
      }
    };
  }, [
    searchValue,
    fields,
    limit,
    minChars,
    debounceMs,
    customQuery,
    dispatch,
    id,
    isSemanticEnabled,
    effectiveReturnFields,
    semanticIndexes,
    table,
    filterQuery,
    exclusionQuery,
    facetConfigs,
  ]);

  // Cleanup on unmount
  useEffect(() => () => dispatch({ type: "deleteWidget", key: id }), [dispatch, id]);

  // Item registration callbacks for child components
  const registerItem = useCallback((itemId: string): number => {
    if (!itemsRef.current.has(itemId)) {
      // Use current size as index to ensure contiguous indices starting from 0
      const index = itemsRef.current.size;
      itemsRef.current.set(itemId, index);
      return index;
    }
    return itemsRef.current.get(itemId) ?? -1;
  }, []);

  const unregisterItem = useCallback((itemId: string) => {
    itemsRef.current.delete(itemId);
    // Reindex remaining items to maintain contiguous indices
    let index = 0;
    itemsRef.current.forEach((_, key) => {
      itemsRef.current.set(key, index++);
    });
  }, []);

  // Handle selection (for child components)
  const handleSelect = useCallback(
    (value: string) => {
      justSelectedRef.current = true;
      // For backward compatibility, create a minimal QueryHit if using children
      if (onSuggestionSelect && children) {
        // Find the actual hit if it's from suggestions
        const hit = suggestions.find((s) => s._id === value);
        if (hit) {
          onSuggestionSelect(hit);
        } else {
          // It's a facet term, create a minimal hit
          onSuggestionSelect({ _id: value, _score: 0, _source: {} });
        }
      }
      setIsOpenOverride(false);
      setSelectedIndex(-1);
    },
    [onSuggestionSelect, suggestions, children]
  );

  // Compute total navigable items directly from data
  const navigableItemCount = useMemo(() => {
    if (!children) return suggestions.length;
    // Count suggestions plus all facet terms
    let count = suggestions.length;
    facetData.forEach((terms) => {
      count += terms.length;
    });
    return count;
  }, [children, suggestions.length, facetData]);

  // Handle keyboard navigation
  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (!isOpen) return;
      if (navigableItemCount === 0) return;

      switch (e.key) {
        case "ArrowDown":
          e.preventDefault();
          setSelectedIndex((prev) => (prev < navigableItemCount - 1 ? prev + 1 : prev));
          break;
        case "ArrowUp":
          e.preventDefault();
          setSelectedIndex((prev) => (prev > 0 ? prev - 1 : -1));
          break;
        case "Enter":
          e.preventDefault();
          if (selectedIndex >= 0 && selectedIndex < navigableItemCount) {
            if (!children) {
              // Legacy mode
              justSelectedRef.current = true;
              onSuggestionSelect?.(suggestions[selectedIndex]);
              setIsOpenOverride(false);
            }
            // In composable mode, child components handle Enter via their own logic
          }
          break;
        case "Escape":
          e.preventDefault();
          setIsOpenOverride(false);
          setSelectedIndex(-1);
          break;
      }
    },
    [isOpen, suggestions, selectedIndex, onSuggestionSelect, children, navigableItemCount]
  );

  // Attach keyboard event listener
  useEffect(() => {
    if (isOpen) {
      document.addEventListener("keydown", handleKeyDown);
      return () => document.removeEventListener("keydown", handleKeyDown);
    }
  }, [isOpen, handleKeyDown]);

  // Handle click outside to close autosuggest
  useEffect(() => {
    if (!isOpen) return;

    const handleClickOutside = (event: MouseEvent) => {
      if (containerRef?.current && !containerRef.current.contains(event.target as Node)) {
        setIsOpenOverride(false);
        setSelectedIndex(-1);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [isOpen, containerRef]);

  // Scroll selected item into view
  useEffect(() => {
    if (selectedIndex >= 0 && suggestionsRef.current) {
      const selectedElement = suggestionsRef.current.children[selectedIndex] as HTMLElement;
      selectedElement?.scrollIntoView({ block: "nearest" });
    }
  }, [selectedIndex]);

  // Handle suggestion click
  const handleSuggestionClick = useCallback(
    (hit: QueryHit) => {
      justSelectedRef.current = true;
      onSuggestionSelect?.(hit);
      setIsOpenOverride(false);
      setSelectedIndex(-1);
    },
    [onSuggestionSelect]
  );

  // Default suggestion renderer
  const defaultRenderSuggestion = (hit: QueryHit) => {
    // Display first available field value
    const firstField = fields?.[0]?.replace(/__(2gram|keyword)$/, "");
    const value = firstField ? hit._source?.[firstField] : undefined;
    return <span className="react-af-autosuggest-term">{value ? String(value) : hit._id}</span>;
  };

  // Context value for child components
  const contextValue: AutosuggestContextValue = useMemo(
    () => ({
      query: searchValue,
      results: suggestions,
      facetData,
      selectedIndex,
      handleSelect,
      isLoading: widget?.isLoading === true,
      registerItem,
      unregisterItem,
      fields,
    }),
    [
      searchValue,
      suggestions,
      facetData,
      selectedIndex,
      handleSelect,
      widget?.isLoading,
      registerItem,
      unregisterItem,
      fields,
    ]
  );

  // If using children (composable mode)
  if (children) {
    if (!isOpen) {
      return null;
    }

    // Hide if there are no results and no facets (but keep visible if loading to prevent flash)
    const hasResults = suggestions.length > 0;
    const hasFacets = Array.from(facetData.values()).some((terms) => terms.length > 0);
    // Extract isLoading from widget directly instead of from contextValue to avoid ref access warning
    const isCurrentlyLoading = widget?.isLoading === true;

    // Also check if search value is valid (meets minimum char requirement)
    const hasValidQuery = searchValue.length >= minChars;

    // Don't show dropdown if:
    // 1. No results AND no facets AND not loading
    // 2. OR query is too short (even if loading)
    if (!hasValidQuery || (!hasResults && !hasFacets && !isCurrentlyLoading)) {
      return null;
    }

    // Build the layout classes and container
    const layoutClass = layout !== "custom" ? `react-af-autosuggest-layout-${layout}` : "";
    const containerClass =
      `react-af-autosuggest-container ${layoutClass} ${className || ""}`.trim();
    const dropdownClass = `react-af-autosuggest-dropdown ${dropdownClassName || ""}`.trim();

    return (
      <AutosuggestContext.Provider value={contextValue}>
        <div
          className={containerClass}
          ref={suggestionsRef as React.RefObject<HTMLDivElement | null>}
          data-has-content={hasResults || hasFacets ? "true" : "false"}
          data-is-loading={isCurrentlyLoading ? "true" : "false"}
        >
          <div className={dropdownClass}>{children}</div>
        </div>
      </AutosuggestContext.Provider>
    );
  }

  // Legacy mode (no children)
  if (!isOpen || suggestions.length === 0) {
    return null;
  }

  return (
    <ul className="react-af-autosuggest" ref={suggestionsRef as React.RefObject<HTMLUListElement>}>
      {suggestions.map((hit, index) => (
        <li
          key={hit._id}
          className={`react-af-autosuggest-item ${
            index === selectedIndex ? "react-af-autosuggest-item-selected" : ""
          }`}
          onClick={() => handleSuggestionClick(hit)}
          onKeyDown={(e) => e.key === "Enter" && handleSuggestionClick(hit)}
          onMouseEnter={() => setSelectedIndex(index)}
        >
          {renderSuggestion ? renderSuggestion(hit) : defaultRenderSuggestion(hit)}
        </li>
      ))}
    </ul>
  );
}

// AutosuggestResults Component
export function AutosuggestResults({
  limit,
  renderItem,
  onSelect: onSelectProp,
  filter,
  className,
  itemClassName,
  selectedItemClassName,
  header,
  footer,
  emptyMessage,
}: AutosuggestResultsProps) {
  const { results, selectedIndex, handleSelect, registerItem, unregisterItem, fields } =
    useAutosuggestContext();

  // Track item indices for keyboard navigation
  const itemIndicesRef = useRef<Map<string, number>>(new Map());

  // Filter and limit results
  const displayResults = useMemo(() => {
    let filtered = filter ? results.filter(filter) : results;
    if (limit) {
      filtered = filtered.slice(0, limit);
    }
    return filtered;
  }, [results, filter, limit]);

  // Register items for keyboard navigation
  useEffect(() => {
    const itemIndices = itemIndicesRef.current;
    displayResults.forEach((hit) => {
      const itemId = `result-${hit._id}`;
      const index = registerItem(itemId);
      itemIndices.set(hit._id, index);
    });

    return () => {
      displayResults.forEach((hit) => {
        const itemId = `result-${hit._id}`;
        unregisterItem(itemId);
        itemIndices.delete(hit._id);
      });
    };
  }, [displayResults, registerItem, unregisterItem]);

  // Handle click
  const handleClick = useCallback(
    (hit: QueryHit) => {
      if (onSelectProp) {
        onSelectProp(hit);
      }
      handleSelect(hit._id);
    },
    [onSelectProp, handleSelect]
  );

  // Handle Enter key
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Enter" && selectedIndex >= 0) {
        const hit = displayResults.find((h) => itemIndicesRef.current.get(h._id) === selectedIndex);
        if (hit) {
          e.preventDefault();
          handleClick(hit);
        }
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [selectedIndex, displayResults, handleClick]);

  // Default renderer
  const defaultRenderItem = (hit: QueryHit) => {
    const firstField = fields?.[0]?.replace(/__(2gram|keyword)$/, "");
    const value = firstField ? hit._source?.[firstField] : undefined;
    return <span className="react-af-autosuggest-term">{value ? String(value) : hit._id}</span>;
  };

  if (displayResults.length === 0 && emptyMessage) {
    return <div className="react-af-autosuggest-results-empty">{emptyMessage}</div>;
  }

  if (displayResults.length === 0) {
    return null;
  }

  return (
    <div className={`react-af-autosuggest-results ${className || ""}`.trim()}>
      {header && (
        <div className="react-af-autosuggest-results-header">
          {typeof header === "function" ? header(displayResults.length) : header}
        </div>
      )}
      <ul className="react-af-autosuggest-results-list">
        {/* eslint-disable-next-line react-hooks/refs */}
        {displayResults.map((hit, idx) => {
          const itemIndex = itemIndicesRef.current.get(hit._id);
          const isSelected = itemIndex === selectedIndex;
          const itemClass = `react-af-autosuggest-result-item ${itemClassName || ""} ${
            isSelected ? selectedItemClassName || "react-af-autosuggest-item-selected" : ""
          }`.trim();

          return (
            <li
              key={hit._id}
              className={itemClass}
              onClick={() => handleClick(hit)}
              onKeyDown={(e) => e.key === "Enter" && handleClick(hit)}
            >
              {renderItem ? renderItem(hit, idx) : defaultRenderItem(hit)}
            </li>
          );
        })}
      </ul>
      {footer && <div className="react-af-autosuggest-results-footer">{footer}</div>}
    </div>
  );
}

// AutosuggestFacets Component
export function AutosuggestFacets({
  field,
  size = 5,
  label,
  order = "count",
  renderItem,
  renderSection,
  onSelect: onSelectProp,
  clickable = true,
  filter,
  className,
  itemClassName,
  sectionClassName,
  header,
  footer,
  emptyMessage,
}: AutosuggestFacetsProps) {
  const { facetData, selectedIndex, handleSelect, registerItem, unregisterItem } =
    useAutosuggestContext();

  // Track item indices for keyboard navigation
  const itemIndicesRef = useRef<Map<string, number>>(new Map());

  // Sort facet terms
  const sortedTerms = useMemo(() => {
    const rawTerms = facetData.get(field) || [];
    const terms = [...rawTerms];
    switch (order) {
      case "count":
        terms.sort((a, b) => b.doc_count - a.doc_count);
        break;
      case "reverse_count":
        terms.sort((a, b) => a.doc_count - b.doc_count);
        break;
      case "term":
        terms.sort((a, b) => a.key.localeCompare(b.key));
        break;
      case "reverse_term":
        terms.sort((a, b) => b.key.localeCompare(a.key));
        break;
    }
    return terms;
  }, [facetData, field, order]);

  // Filter and limit terms
  const displayTerms = useMemo(() => {
    let filtered = filter ? sortedTerms.filter(filter) : sortedTerms;
    filtered = filtered.slice(0, size);
    return filtered;
  }, [sortedTerms, filter, size]);

  // Register items for keyboard navigation
  useEffect(() => {
    if (!clickable) return;

    const itemIndices = itemIndicesRef.current;
    displayTerms.forEach((term) => {
      const itemId = `facet-${field}-${term.key}`;
      const index = registerItem(itemId);
      itemIndices.set(term.key, index);
    });

    return () => {
      displayTerms.forEach((term) => {
        const itemId = `facet-${field}-${term.key}`;
        unregisterItem(itemId);
        itemIndices.delete(term.key);
      });
    };
  }, [displayTerms, clickable, field, registerItem, unregisterItem]);

  // Handle click
  const handleClick = useCallback(
    (facet: AggregationBucket) => {
      if (!clickable) return;
      if (onSelectProp) {
        onSelectProp(facet);
      }
      handleSelect(facet.key);
    },
    [clickable, onSelectProp, handleSelect]
  );

  // Handle Enter key
  useEffect(() => {
    if (!clickable) return;

    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Enter" && selectedIndex >= 0) {
        const facet = displayTerms.find((t) => itemIndicesRef.current.get(t.key) === selectedIndex);
        if (facet) {
          e.preventDefault();
          handleClick(facet);
        }
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [clickable, selectedIndex, displayTerms, handleClick]);

  // Default renderer
  const defaultRenderItem = (facet: AggregationBucket) => (
    <span className="react-af-autosuggest-facet-term">
      {facet.key} <span className="react-af-autosuggest-facet-count">({facet.doc_count})</span>
    </span>
  );

  if (displayTerms.length === 0 && emptyMessage) {
    return <div className="react-af-autosuggest-facets-empty">{emptyMessage}</div>;
  }

  if (displayTerms.length === 0) {
    return null;
  }

  // Use custom section renderer if provided
  if (renderSection) {
    return <div className={className}>{renderSection(field, label || field, displayTerms)}</div>;
  }

  return (
    <div
      className={`react-af-autosuggest-facets ${sectionClassName || ""} ${className || ""}`.trim()}
    >
      {header && (
        <div className="react-af-autosuggest-facets-header">
          {typeof header === "function" ? header(field, label || field) : header}
        </div>
      )}
      {label && <div className="react-af-autosuggest-facet-section-label">{label}</div>}
      <ul className="react-af-autosuggest-facets-list">
        {/* eslint-disable-next-line react-hooks/refs */}
        {displayTerms.map((facet, index) => {
          const itemIndex = itemIndicesRef.current.get(facet.key);
          const isSelected = itemIndex === selectedIndex;
          const itemClass = `react-af-autosuggest-facet-item ${itemClassName || ""} ${
            isSelected ? "react-af-autosuggest-item-selected" : ""
          } ${clickable ? "react-af-autosuggest-facet-item-clickable" : ""}`.trim();

          return (
            <li
              key={facet.key}
              className={itemClass}
              onClick={() => handleClick(facet)}
              onKeyDown={(e) => e.key === "Enter" && handleClick(facet)}
              style={{ cursor: clickable ? "pointer" : "default" }}
            >
              {renderItem ? renderItem(facet, index) : defaultRenderItem(facet)}
            </li>
          );
        })}
      </ul>
      {footer && <div className="react-af-autosuggest-facets-footer">{footer}</div>}
    </div>
  );
}
