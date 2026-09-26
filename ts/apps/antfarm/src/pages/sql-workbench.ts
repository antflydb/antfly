import type { SQLResponse } from "@antfly/sdk";

export function sqlCell(result: SQLResponse, row: number, column: number): string {
  const value = result.rows[row][column];
  const isNull = result.sql_nulls?.[row]?.[column] ?? value === null;
  if (isNull) return "NULL";
  if (typeof value === "string") return value;
  return JSON.stringify(value) ?? "";
}

export function sqlParameters(text: string): unknown[] {
  if (text.length > 1 << 20) throw new Error("Parameters exceed 1 MiB.");
  const value: unknown = JSON.parse(text);
  if (!Array.isArray(value) || value.length > 1024) {
    throw new Error("Parameters must be a JSON array with at most 1024 values.");
  }
  const check = (item: unknown, depth: number): void => {
    if (depth > 64) throw new Error("Parameter nesting exceeds 64 levels.");
    if (
      typeof item === "number" &&
      (!Number.isFinite(item) || (Number.isInteger(item) && !Number.isSafeInteger(item)))
    ) {
      throw new Error("Pass large integers as quoted decimal strings to preserve precision.");
    }
    if (item && typeof item === "object") {
      for (const child of Object.values(item)) check(child, depth + 1);
    }
  };
  check(value, 0);
  return value;
}
