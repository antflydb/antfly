import type { TableStatus } from "@antfly/sdk";
import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { api } from "@/api";
import { useApi } from "@/hooks/use-api-config";

interface UseTableIndexSelectorOptions {
  syncToUrl?: boolean;
}

interface UseTableIndexSelectorReturn {
  tables: TableStatus[];
  selectedTable: string;
  setSelectedTable: (table: string) => void;
  embeddingIndexes: string[];
  selectedIndex: string;
  setSelectedIndex: (index: string) => void;
  isLoading: boolean;
}

export function useTableIndexSelector(
  options: UseTableIndexSelectorOptions = {}
): UseTableIndexSelectorReturn {
  const { syncToUrl = false } = options;
  const apiClient = useApi();
  const [searchParams, setSearchParams] = useSearchParams();

  const [tables, setTables] = useState<TableStatus[]>([]);
  const [selectedTable, setSelectedTableState] = useState("");
  const [embeddingIndexes, setEmbeddingIndexes] = useState<string[]>([]);
  const [selectedIndex, setSelectedIndexState] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const [tablesLoaded, setTablesLoaded] = useState(false);

  // Setters that optionally sync to URL
  const setSelectedTable = (table: string) => {
    setSelectedTableState(table);
    if (syncToUrl) {
      setSearchParams(
        (prev) => {
          if (table) {
            prev.set("table", table);
          } else {
            prev.delete("table");
          }
          prev.delete("index"); // Reset index when table changes
          return prev;
        },
        { replace: true }
      );
    }
  };

  const setSelectedIndex = (index: string) => {
    setSelectedIndexState(index);
    if (syncToUrl) {
      setSearchParams(
        (prev) => {
          if (index) {
            prev.set("index", index);
          } else {
            prev.delete("index");
          }
          return prev;
        },
        { replace: true }
      );
    }
  };

  // Fetch tables on mount
  useEffect(() => {
    const fetchTables = async () => {
      try {
        const response = await api.tables.list();
        setTables(response as TableStatus[]);

        // Read initial table from URL or default to first
        const urlTable = syncToUrl ? searchParams.get("table") : null;
        const validUrlTable =
          urlTable && (response as TableStatus[]).some((t) => t.name === urlTable);

        if (validUrlTable) {
          setSelectedTableState(urlTable as string);
        } else if (response.length > 0) {
          setSelectedTableState(response[0].name);
        }
      } catch {
        setTables([]);
      } finally {
        setTablesLoaded(true);
        setIsLoading(false);
      }
    };
    fetchTables();
  }, [searchParams.get, syncToUrl]); // eslint-disable-line react-hooks/exhaustive-deps

  // Fetch embedding indexes when table changes
  useEffect(() => {
    if (!selectedTable || !tablesLoaded) {
      setEmbeddingIndexes([]);
      setSelectedIndexState("");
      return;
    }

    const fetchIndexes = async () => {
      try {
        const response = await apiClient.indexes.list(selectedTable);
        const embeddingIdxs = (response || [])
          .filter(
            (idx: { config?: { type?: string } }) =>
              idx.config?.type?.includes("aknn") || idx.config?.type?.includes("embedding")
          )
          .map((idx: { config?: { name?: string } }) => idx.config?.name || "")
          .filter(Boolean);
        setEmbeddingIndexes(embeddingIdxs);

        // Read initial index from URL or default to first
        const urlIndex = syncToUrl ? searchParams.get("index") : null;
        const validUrlIndex = urlIndex && embeddingIdxs.includes(urlIndex);

        if (validUrlIndex) {
          setSelectedIndexState(urlIndex as string);
        } else if (embeddingIdxs.length > 0) {
          setSelectedIndexState(embeddingIdxs[0]);
        } else {
          setSelectedIndexState("");
        }
      } catch {
        setEmbeddingIndexes([]);
        setSelectedIndexState("");
      }
    };
    fetchIndexes();
  }, [selectedTable, tablesLoaded, apiClient, searchParams.get, syncToUrl]); // eslint-disable-line react-hooks/exhaustive-deps

  return {
    tables,
    selectedTable,
    setSelectedTable,
    embeddingIndexes,
    selectedIndex,
    setSelectedIndex,
    isLoading,
  };
}
