import type { TableStatus } from "@antfly/sdk";
import type { ReactNode } from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";
import { isProductEnabled } from "@/config/products";
import { TableContext } from "@/contexts/table-context";
import { useApi } from "@/hooks/use-api-config";
import { normalizeTablesResponse } from "@/lib/table-utils";

export function TableProvider({ children }: { children: ReactNode }) {
  const apiClient = useApi();
  const location = useLocation();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  const searchParamsRef = useRef(searchParams);
  searchParamsRef.current = searchParams;
  const setSearchParamsRef = useRef(setSearchParams);
  setSearchParamsRef.current = setSearchParams;

  const [tables, setTables] = useState<TableStatus[]>([]);
  const [isLoadingTables, setIsLoadingTables] = useState(() => isProductEnabled("antfly"));
  const [embeddingIndexes, setEmbeddingIndexes] = useState<string[]>([]);
  const [graphIndexes, setGraphIndexes] = useState<string[]>([]);
  const [chatIndexes, setChatIndexes] = useState<string[]>([]);
  const [isLoadingIndexes, setIsLoadingIndexes] = useState(() => isProductEnabled("antfly"));
  const [selectedIndex, setSelectedIndexState] = useState(() => searchParams.get("index") || "");
  const selectedIndexRef = useRef(selectedIndex);
  selectedIndexRef.current = selectedIndex;
  const setSelectedIndex = useCallback((name: string) => {
    setSelectedIndexState(name);
    if ((searchParamsRef.current.get("index") || "") === name) return;
    setSearchParamsRef.current(
      (prev) => {
        const next = new URLSearchParams(prev);
        if (name) next.set("index", name);
        else next.delete("index");
        return next;
      },
      { replace: true }
    );
  }, []);
  useEffect(() => {
    setSelectedIndexState(searchParams.get("index") || "");
  }, [searchParams]);

  // Internal state for selected table. URL is synced to/from this.
  const [selectedTable, setSelectedTableState] = useState(() => {
    const pathMatch = location.pathname.match(/^\/tables\/([^/]+)/);
    if (pathMatch) return decodeURIComponent(pathMatch[1]);
    return searchParams.get("table") || "";
  });

  // Track previous pathname to detect navigation
  const prevPathnameRef = useRef(location.pathname);

  // Sync URL → state when URL changes (browser nav, link clicks)
  useEffect(() => {
    const pathMatch = location.pathname.match(/^\/tables\/([^/]+)/);
    const pathTable = pathMatch ? decodeURIComponent(pathMatch[1]) : undefined;
    const queryTable = searchParams.get("table");
    const urlTable = pathTable || queryTable || "";
    if (urlTable) {
      setSelectedTableState(urlTable);
    }
  }, [location.pathname, searchParams]);

  // Sync state → URL on non-table pages so the selection persists in the URL
  useEffect(() => {
    if (location.pathname.startsWith("/tables/")) return;
    const currentQueryTable = searchParams.get("table");
    if (selectedTable && currentQueryTable !== selectedTable) {
      setSearchParams(
        (prev) => {
          prev.set("table", selectedTable);
          return prev;
        },
        { replace: true }
      );
    }
    prevPathnameRef.current = location.pathname;
  }, [location.pathname, selectedTable, searchParams, setSearchParams]);

  // setSelectedTable: update state + URL
  const setSelectedTable = useCallback(
    (name: string) => {
      setSelectedTableState(name);
      const isOnTablePage = location.pathname.startsWith("/tables/");
      if (isOnTablePage) {
        navigate(name ? `/tables/${encodeURIComponent(name)}?section=overview` : "/");
      } else {
        setSearchParams(
          (prev) => {
            const next = new URLSearchParams(prev);
            if (name) next.set("table", name);
            else next.delete("table");
            return next;
          },
          { replace: true }
        );
      }
    },
    [location.pathname, navigate, setSearchParams]
  );

  const tableRequestId = useRef(0);

  // Fetch tables on mount
  const refreshTables = useCallback(async () => {
    if (!isProductEnabled("antfly")) return;
    const requestId = ++tableRequestId.current;
    setIsLoadingTables(true);
    try {
      const response = await apiClient.tables.list();
      if (requestId === tableRequestId.current) setTables(normalizeTablesResponse(response));
    } catch {
      if (requestId === tableRequestId.current) setTables([]);
    } finally {
      if (requestId === tableRequestId.current) setIsLoadingTables(false);
    }
  }, [apiClient]);

  useEffect(() => {
    void refreshTables();
    return () => {
      tableRequestId.current++;
    };
  }, [refreshTables]);

  // Keep the selection valid after endpoint changes or table deletion.
  useEffect(() => {
    if (isLoadingTables || tables.some((table) => table.name === selectedTable)) return;
    const fallback = tables[0]?.name ?? "";
    if (fallback !== selectedTable) setSelectedTable(fallback);
  }, [isLoadingTables, tables, selectedTable, setSelectedTable]);

  // Discard responses from the previous table or API endpoint.
  useEffect(() => {
    let active = true;
    const fetchIndexes = async () => {
      if (!isProductEnabled("antfly") || !selectedTable) {
        setIsLoadingIndexes(false);
        setEmbeddingIndexes([]);
        setGraphIndexes([]);
        setChatIndexes([]);
        setSelectedIndex("");
        return;
      }
      setEmbeddingIndexes([]);
      setChatIndexes([]);
      setGraphIndexes([]);
      setIsLoadingIndexes(true);
      try {
        const response = await apiClient.indexes.list(selectedTable);
        if (!active) return;
        const indexes = response || [];
        const embeddingIdxs = indexes
          .filter(
            (idx: { config?: { type?: string } }) =>
              idx.config?.type?.includes("aknn") || idx.config?.type?.includes("embedding")
          )
          .map((idx: { config?: { name?: string } }) => idx.config?.name || "")
          .filter(Boolean);
        // Chat indexes include both embedding and full-text indexes
        const chatIdxs = indexes
          .filter(
            (idx: { config?: { type?: string } }) =>
              idx.config?.type?.includes("aknn") ||
              idx.config?.type?.includes("embedding") ||
              idx.config?.type?.includes("full_text")
          )
          .map((idx: { config?: { name?: string } }) => idx.config?.name || "")
          .filter(Boolean);
        const graphIdxs = indexes
          .filter((idx: { config?: { type?: string } }) => idx.config?.type === "graph")
          .map((idx: { config?: { name?: string } }) => idx.config?.name || "")
          .filter(Boolean);
        setEmbeddingIndexes(embeddingIdxs);
        setGraphIndexes(graphIdxs);
        setChatIndexes(chatIdxs);
        const available = [...embeddingIdxs, ...chatIdxs];
        const requested = selectedIndexRef.current;
        if (!available.includes(requested)) setSelectedIndex(embeddingIdxs[0] ?? chatIdxs[0] ?? "");
      } catch {
        if (!active) return;
        setEmbeddingIndexes([]);
        setGraphIndexes([]);
        setChatIndexes([]);
        setSelectedIndex("");
      } finally {
        if (active) setIsLoadingIndexes(false);
      }
    };
    void fetchIndexes();
    return () => {
      active = false;
    };
  }, [selectedTable, apiClient, setSelectedIndex]);

  return (
    <TableContext.Provider
      value={{
        tables,
        isLoadingTables,
        selectedTable,
        setSelectedTable,
        embeddingIndexes,
        graphIndexes,
        chatIndexes,
        isLoadingIndexes,
        selectedIndex,
        setSelectedIndex,
        refreshTables,
      }}
    >
      {children}
    </TableContext.Provider>
  );
}
