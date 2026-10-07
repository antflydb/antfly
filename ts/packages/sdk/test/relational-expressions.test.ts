import { describe, expect, it } from "vitest";
import type { RelationalColumnExpression, RelationalIndexPredicate } from "../src/index.js";

describe("generated relational expression and predicate contracts", () => {
  it("retains conditional branch order and a typed NULL fallback", () => {
    const generated: RelationalColumnExpression = {
      column: "n",
      expression: {
        op: "case_when",
        args: [
          { op: "literal", type: "boolean", value: true },
          { op: "column", column: "source" },
          { op: "literal", type: "integer", sql_type: "int32", value: null },
        ],
      },
    };
    expect(JSON.parse(JSON.stringify(generated))).toEqual(generated);
  });

  it("retains builtin identities on deferred numeric assignment casts", () => {
    const generated: RelationalColumnExpression = {
      column: "n",
      expression: {
        op: "cast",
        type: "integer",
        sql_type: "int16",
        args: [{ op: "literal", type: "integer", sql_type: "int32", value: 32768 }],
      },
    };
    expect(JSON.parse(JSON.stringify(generated))).toEqual(generated);
  });

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
