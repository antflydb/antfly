import { describe, expect, it } from "vitest";
import type { RelationalColumnExpression, RelationalIndexPredicate } from "../src/index.js";
import { validateRelationalExpression } from "../src/relational-expression.js";

describe("generated relational expression and predicate contracts", () => {
  it("accepts bounded membership lists and rejects malformed membership", () => {
    const operand = { op: "column", column: "title" };
    const literal = { op: "literal", type: "string", value: "needle" };
    const validate = (expression: unknown) =>
      validateRelationalExpression(expression, "expression", { nodes: 0, literalBytes: 0 });
    expect(() =>
      validate({ op: "in_list", collation: "ci", args: [operand, ...Array(100).fill(literal)] })
    ).not.toThrow();
    expect(() => validate({ op: "in_list", args: [operand] })).toThrow();
    expect(() =>
      validate({ op: "in_list", args: [operand, ...Array(127).fill(literal)] })
    ).toThrow();
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
