import type { TableStatus } from "@antfly/sdk";
import { createContext } from "react";

export interface TableContextType {
  tables: TableStatus[];
  isLoadingTables: boolean;
  selectedTable: string;
  setSelectedTable: (name: string) => void;
  embeddingIndexes: string[];
  /** All searchable indexes (embedding + full-text) for chat */
  chatIndexes: string[];
  isLoadingIndexes: boolean;
  selectedIndex: string;
  setSelectedIndex: (name: string) => void;
  refreshTables: () => Promise<void>;
}

export const TableContext = createContext<TableContextType | undefined>(undefined);
