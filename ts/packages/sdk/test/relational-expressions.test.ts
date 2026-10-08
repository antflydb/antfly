import { describe, expect, it } from "vitest";
import type { RelationalColumnExpression, RelationalIndexPredicate } from "../src/index.js";
import { validateRelationalExpression } from "../src/relational-expression.js";

describe("relational expression structural admission", () => {
  const integer = { op: "literal", type: "integer", sql_type: "int32", value: 1 };
  const boolean = { op: "literal", type: "boolean", value: true };
  const validate = (expression: unknown) =>
    validateRelationalExpression(expression, "expression", { nodes: 0, literalBytes: 0 });

  it.each([
    { op: "cast", type: "integer", sql_type: "int16", args: [integer] },
    { op: "case_when", args: [boolean, integer, integer] },
    { op: "modulo", sql_type: "int32", args: [integer, integer] },
    { op: "in_list", args: [integer, integer] },
    { op: "not_in_list", args: [integer, integer] },
  ])("accepts server-supported $op expressions", (expression) => {
    expect(() => validate(expression)).not.toThrow();
  });

  it.each([
    { op: "cast", type: "integer", args: [integer] },
    { op: "cast", type: "integer", sql_type: "float64", args: [integer] },
    { op: "cast", type: "integer", sql_type: "int32", args: [] },
    { op: "case_when", args: [boolean, integer, boolean, integer] },
    { op: "modulo", args: [integer] },
    { op: "in_list", args: [integer] },
    { op: "not_in_list", args: Array(33).fill(integer) },
    { ...integer, sql_type: "uuid" },
    { op: "add", sql_type: "numeric", args: [integer, integer] },
    { op: "coalesce", sql_type: "int32", args: [integer, integer] },
  ])("rejects malformed $op contracts before transport", (expression) => {
    expect(() => validate(expression)).toThrow(TypeError);
  });
});

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
