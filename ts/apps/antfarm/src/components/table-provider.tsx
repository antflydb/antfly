import type { TableStatus } from "@antfly/sdk";
import type { ReactNode } from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";
import { isProductEnabled } from "@/config/products";
import { TableContext } from "@/contexts/table-context";
import { useApi } from "@/hooks/use-api-config";

export function TableProvider({ children }: { children: ReactNode }) {
  const apiClient = useApi();
  const location = useLocation();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  const [tables, setTables] = useState<TableStatus[]>([]);
  const [isLoadingTables, setIsLoadingTables] = useState(false);
  const [embeddingIndexes, setEmbeddingIndexes] = useState<string[]>([]);
  const [chatIndexes, setChatIndexes] = useState<string[]>([]);
  const [isLoadingIndexes, setIsLoadingIndexes] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState("");

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
        navigate(`/tables/${name}`);
      } else {
        setSearchParams(
          (prev) => {
            prev.set("table", name);
            return prev;
          },
          { replace: true }
        );
      }
    },
    [location.pathname, navigate, setSearchParams]
  );

  // Fetch tables on mount
  const refreshTables = useCallback(async () => {
    if (!isProductEnabled("antfly")) return;
    setIsLoadingTables(true);
    try {
      const response = await apiClient.tables.list();
      if (response && Array.isArray(response)) {
        setTables(response as TableStatus[]);
      }
    } catch {
      setTables([]);
    } finally {
      setIsLoadingTables(false);
    }
  }, [apiClient]);

  useEffect(() => {
    refreshTables();
  }, [refreshTables]);

  // Auto-select first table when tables load and none selected
  useEffect(() => {
    if (!selectedTable && tables.length > 0) {
      setSelectedTable(tables[0].name);
    }
  }, [tables, selectedTable, setSelectedTable]);

  // Fetch embedding indexes when table changes
  useEffect(() => {
    const fetchIndexes = async () => {
      if (!selectedTable) {
        setEmbeddingIndexes([]);
        setChatIndexes([]);
        setSelectedIndex("");
        return;
      }
      setIsLoadingIndexes(true);
      try {
        const response = await apiClient.indexes.list(selectedTable);
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
        setEmbeddingIndexes(embeddingIdxs);
        setChatIndexes(chatIdxs);
        if (embeddingIdxs.length > 0) {
          setSelectedIndex(embeddingIdxs[0]);
        } else {
          setSelectedIndex("");
        }
      } catch {
        setEmbeddingIndexes([]);
        setChatIndexes([]);
        setSelectedIndex("");
      } finally {
        setIsLoadingIndexes(false);
      }
    };
    fetchIndexes();
  }, [selectedTable, apiClient]);

  return (
    <TableContext.Provider
      value={{
        tables,
        isLoadingTables,
        selectedTable,
        setSelectedTable,
        embeddingIndexes,
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
