import { type ReactNode, useEffect } from "react";
import Listener from "./Listener";
import type { SharedAction, SharedState } from "./SharedContext";
import { SharedContextProvider } from "./SharedContextProvider";
import { initializeAntflyClient } from "./utils";

export interface AntflyProps {
  children: ReactNode;
  url: string; // Base URL only (e.g., http://localhost:8080/api/v1)
  table: string; // Required default table for all widgets
  onChange?: (params: Map<string, unknown>) => void;
  headers?: Record<string, string>;
}

export default function Antfly({ children, url, table, onChange, headers = {} }: AntflyProps) {
  const initialState: SharedState = {
    url,
    table,
    listenerEffect: null,
    widgets: new Map(),
    headers,
  };

  useEffect(() => {
    initializeAntflyClient(url, headers);
  }, [url, headers]);

  const reducer = (state: SharedState, action: SharedAction): SharedState => {
    switch (action.type) {
      case "setWidget": {
        // Get existing widget to preserve result when isLoading
        const existingWidget = state.widgets.get(action.key);

        const widget = {
          id: action.key,
          needsQuery: action.needsQuery,
          needsConfiguration: action.needsConfiguration,
          isFacet: action.isFacet,
          wantResults: action.wantResults,
          wantFacets: action.wantFacets,
          query: action.query,
          rootQuery: action.rootQuery,
          semanticQuery: action.semanticQuery,
          isSemantic: action.isSemantic,
          isAutosuggest: action.isAutosuggest,
          value: action.value,
          submittedAt: action.submittedAt,
          table: action.table,
          filterQuery: action.filterQuery,
          exclusionQuery: action.exclusionQuery,
          facetOptions: action.facetOptions,
          isLoading: action.isLoading,
          configuration: action.configuration,
          // Preserve previous result if no new result provided.
          // setWidgetResult is the dedicated action for updating results;
          // setWidget should never clear results as a side effect of
          // re-registering configuration (e.g. from unstable useEffect deps).
          result: action.result !== undefined ? action.result : existingWidget?.result,
        };
        // Create a new Map to maintain immutability
        const newWidgets = new Map(state.widgets);
        newWidgets.set(action.key, widget);
        return { ...state, widgets: newWidgets };
      }
      case "setWidgetResult": {
        const existingWidget = state.widgets.get(action.key);
        if (!existingWidget) return state;
        const newWidgets = new Map(state.widgets);
        newWidgets.set(action.key, {
          ...existingWidget,
          isLoading: action.isLoading,
          result: action.result !== undefined ? action.result : existingWidget.result,
        });
        return { ...state, widgets: newWidgets };
      }
      case "deleteWidget": {
        // Create a new Map to maintain immutability
        const newWidgets = new Map(state.widgets);
        newWidgets.delete(action.key);
        return { ...state, widgets: newWidgets };
      }
      case "setListenerEffect":
        return { ...state, listenerEffect: action.value };
      default:
        return state;
    }
  };

  return (
    <SharedContextProvider initialState={initialState} reducer={reducer}>
      <Listener onChange={onChange}>{children}</Listener>
    </SharedContextProvider>
  );
}
