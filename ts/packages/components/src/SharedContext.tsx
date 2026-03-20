import type { AggregationBucket, QueryHit } from "@antfly/sdk";
import { createContext, type Dispatch, useContext } from "react";

export interface Widget {
  id: string;
  needsQuery?: boolean;
  needsConfiguration?: boolean;
  isFacet?: boolean;
  rootQuery?: boolean;
  isAutosuggest?: boolean;
  wantResults?: boolean;
  wantFacets?: boolean; // Whether widget wants facet data
  query?: unknown;
  semanticQuery?: string;
  isSemantic?: boolean;
  value?: unknown;
  submittedAt?: number; // Timestamp of when this widget was last submitted
  table?: string | string[]; // Table override (single or multi-table support)
  filterQuery?: unknown; // Filter query to constrain search results
  exclusionQuery?: unknown; // Exclusion query to exclude matches
  facetOptions?: Array<{ field: string; size?: number }>; // Facet configurations
  isLoading?: boolean; // Whether this widget is currently fetching results
  configuration?: {
    fields?: string[];
    size?: number;
    filterValue?: string;
    useCustomQuery?: boolean;
    [key: string]: unknown;
  };
  result?: {
    data?: QueryHit[];
    facetData?: AggregationBucket[] | AggregationBucket[][]; // Can be array of arrays for multiple facets
    total?: number | { value: number };
    error?: string;
  };
}

export interface SharedState {
  url?: string;
  table: string; // Required default table for all widgets
  listenerEffect?: (() => void) | null;
  widgets: Map<string, Widget>;
  headers?: Record<string, string>;
}

export type SharedAction =
  | {
      type: "setWidget";
      key: string;
      needsQuery?: boolean;
      needsConfiguration?: boolean;
      isFacet?: boolean;
      rootQuery?: boolean;
      isAutosuggest?: boolean;
      wantResults?: boolean;
      wantFacets?: boolean;
      query?: unknown;
      semanticQuery?: string;
      isSemantic?: boolean;
      value?: unknown;
      submittedAt?: number;
      table?: string | string[]; // Table override
      filterQuery?: unknown; // Filter query to constrain search results
      exclusionQuery?: unknown; // Exclusion query to exclude matches
      facetOptions?: Array<{ field: string; size?: number }>;
      isLoading?: boolean; // Whether this widget is currently fetching results
      configuration?: Widget["configuration"];
      result?: Widget["result"];
    }
  | {
      type: "setWidgetResult";
      key: string;
      isLoading: boolean;
      result?: Widget["result"];
    }
  | {
      type: "deleteWidget";
      key: string;
    }
  | {
      type: "setListenerEffect";
      value: (() => void) | null;
    }
  | {
      type: "setSharedConfig";
      url?: string;
      table: string;
      headers?: Record<string, string>;
    };

export type SharedContextType = [SharedState, Dispatch<SharedAction>] | null;

export const SharedContext = createContext<SharedContextType>(null);

export const useSharedContext = (): [SharedState, Dispatch<SharedAction>] => {
  const context = useContext(SharedContext);
  if (!context) {
    throw new Error("useSharedContext must be used within a SharedContextProvider");
  }
  return context;
};
