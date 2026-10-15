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
  it("accepts every managed relationship kind and preserves its producer", () => {
    for (const ownership of ["document", "field", "artifact"]) {
      const edge = {
        type: "relationship",
        value: {
          identity: { table: "docs", key: "opaque-fact" },
          index: "links",
          type: "LINK",
          source: { table: "docs", key: "a" },
          target: { table: "docs", key: "b" },
          ownership,
          producer: { table: "docs", key: "claim" },
          properties: { type: "map", value: {} },
        },
      };
      expect(sqlGraphValues(result([[JSON.stringify(edge)]]))).toEqual([edge]);
    }
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
