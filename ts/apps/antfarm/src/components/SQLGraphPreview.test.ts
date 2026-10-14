import { describe, expect, it } from "vitest";
import type { SQLResponse } from "@antfly/sdk";
import { sqlGraphValues } from "./SQLGraphPreview";

const result = (rows: unknown[][]): SQLResponse => ({
  columns: [{ name: "value", type: "json" }],
  rows,
  rows_affected: 0,
  command_tag: "SELECT",
});
describe("SQL returned graph values", () => {
  it("ignores scalar identifiers and malformed graph-shaped JSON", () => {
    expect(
      sqlGraphValues(
        result([
          ["a"],
          [{ type: "node", value: null }],
          [{ type: "map", value: null }],
          [{ type: "relationship", value: { identity: { table: "docs", key: "edge" } } }],
        ])
      )
    ).toEqual([]);
  });
  it("preserves qualified identities inside encoded JSON paths", () => {
    const node = {
      type: "node",
      value: {
        identity: { table: "docs", key: "a" },
        labels: ["Document"],
        properties: { type: "map", value: {} },
      },
    };
    const path = { type: "path", value: { nodes: [node], relationships: [] } };
    expect(sqlGraphValues(result([[JSON.stringify(path)]]))).toEqual([path]);
  });
});
