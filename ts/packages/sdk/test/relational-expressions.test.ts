import { describe, expect, it } from "vitest";
import type { RelationalColumnExpression, RelationalIndexPredicate } from "../src/index.js";

describe("generated relational expression and predicate contracts", () => {
  it("keeps exact literals and explicit null through recursive wire values", () => {
    const generated: RelationalColumnExpression = {
      column: "total",
      expression: {
        op: "coalesce",
        args: [
          { op: "literal", type: "integer", value: null },
          { op: "literal", type: "integer", value: "9007199254740993" },
        ],
      },
    };
    const predicate: RelationalIndexPredicate = {
      column: "total",
      op: "eq",
      value: "9007199254740993",
    };
    expect(JSON.parse(JSON.stringify({ generated, predicate }))).toEqual({ generated, predicate });
  });
});
