import type { TableStatus } from "@antfly/sdk";

export function normalizeTablesResponse(response: unknown): TableStatus[] {
  if (Array.isArray(response)) return response as TableStatus[];
  if (
    response &&
    typeof response === "object" &&
    "tables" in response &&
    Array.isArray((response as { tables?: unknown }).tables)
  ) {
    return (response as { tables: TableStatus[] }).tables;
  }
  return [];
}
