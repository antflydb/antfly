import { useContext } from "react";
import { TableContext } from "@/contexts/table-context";

export function useTable() {
  const context = useContext(TableContext);
  if (context === undefined) {
    throw new Error("useTable must be used within a TableProvider");
  }
  return context;
}
