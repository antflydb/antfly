import { describe, expect, it } from "vitest";
import type { RelationalColumnExpression, RelationalIndexPredicate } from "../src/index.js";
import { validateRelationalExpression } from "../src/relational-expression.js";

describe("relational expression structural admission", () => {
  const integer = { op: "literal", type: "integer", sql_type: "int32", value: 1 };
  const boolean = { op: "literal", type: "boolean", value: true };
  const validate = (expression: unknown) =>
    validateRelationalExpression(expression, "expression", { nodes: 0, literalBytes: 0 });

  const envelope = {
    dimensions: [{ length: 2, lower_bound: -4 }],
    values: ["9007199254740993", null],
    sql_nulls: [false, true],
  };
  const array = (value: unknown = envelope, sql_type = "int64") => ({
    op: "literal",
    type: "sql_array",
    sql_type,
    value,
  });

  it("admits bounded typed constructors and array identity/modifier casts", () => {
    expect(() => validate({ op: "array", sql_type: "int32", args: [] })).not.toThrow();
    expect(() => validate({ op: "array", sql_type: "int32", args: [integer] })).not.toThrow();
    expect(() =>
      validate({ op: "array", sql_type: "int32", args: Array(32).fill(integer) })
    ).not.toThrow();
    expect(() =>
      validate({ op: "array", sql_type: "int64", args: [array(), array()] })
    ).not.toThrow();
    expect(() =>
      validate({ op: "cast", type: "sql_array", sql_type: "int64", args: [array()] })
    ).not.toThrow();
    expect(() =>
      validate({
        op: "cast",
        type: "sql_array",
        sql_type: "numeric",
        numeric_modifier: { precision: 4, scale: 2 },
        args: [array(null, "numeric")],
      })
    ).not.toThrow();
    for (const value of [
      { op: "array", args: [] },
      { op: "array", sql_type: "unknown", args: [] },
      { op: "array", sql_type: "int32", args: Array(128).fill(integer) },
      { op: "array", sql_type: "int32", args: [], value: null },
    ])
      expect(() => validate(value)).toThrow(TypeError);
  });

  it.each([
    "text",
    "int16",
    "int32",
    "int64",
    "float32",
    "float64",
    "boolean",
    "uuid",
    "jsonb",
    "numeric",
  ])("admits generated array identity %s without client-side width inference", (identity) => {
    expect(() => validate(array(null, identity))).not.toThrow();
    expect(() =>
      validate(array({ dimensions: [], values: [], sql_nulls: [] }, identity))
    ).not.toThrow();
  });

  it("preserves exact integer strings, non-default bounds and SQL NULL versus JSON null", () => {
    expect(() => validate(array())).not.toThrow();
    expect(() => validate(array({ ...envelope, values: [null, null] }, "jsonb"))).not.toThrow();
    const budget = { nodes: 0, literalBytes: 0 };
    validateRelationalExpression(array(), "expression", budget);
    expect(budget.literalBytes).toBe(new TextEncoder().encode(JSON.stringify(envelope)).length);
  });

  it.each([
    {},
    [],
    { ...envelope, extra: true },
    { ...envelope, dimensions: [{ length: 3, lower_bound: 1 }] },
    { ...envelope, dimensions: [{ length: 2, lower_bound: 0.5 }] },
    { ...envelope, dimensions: Array(7).fill({ length: 1, lower_bound: 1 }) },
    { ...envelope, sql_nulls: [false] },
    { ...envelope, sql_nulls: [0, true] },
    { ...envelope, values: ["1", "not-null"] },
    { ...envelope, values: ["x".repeat(1024 * 1024), null] },
    { ...envelope, values: [Number.POSITIVE_INFINITY, null] },
    { ...envelope, values: ["\ud800", null] },
  ])("rejects malformed or unbounded array envelope %#", (value) => {
    expect(() => validate(array(value))).toThrow(TypeError);
  });

  it("bounds cyclic JSONB and shares the aggregate literal budget", () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => validate(array({ ...envelope, values: [cyclic, null] }, "jsonb"))).toThrow(
      TypeError
    );
    expect(() => validate(array(envelope, "unknown"))).toThrow(TypeError);
    expect(() => validate({ op: "literal", type: "sql_array", value: null })).toThrow(TypeError);
    expect(() =>
      validate({ op: "cast", type: "sql_array", sql_type: "numeric", args: [integer] })
    ).toThrow(TypeError);
    expect(() =>
      validateRelationalExpression(array(), "expression", {
        nodes: 0,
        literalBytes: 4 * 1024 * 1024 - 1,
      })
    ).toThrow(/literal budget/);
  });

  it("counts escaped Unicode and nested JSONB without a serialized envelope copy", () => {
    const value = {
      ...envelope,
      values: [{ label: '\u00e9\ud83d\ude00\n\t"\\', nested: [null, true, 1.25] }, null],
    };
    const budget = { nodes: 0, literalBytes: 0 };
    validateRelationalExpression(array(value, "jsonb"), "expression", budget);
    expect(budget.literalBytes).toBe(new TextEncoder().encode(JSON.stringify(value)).length);
  });

  it.each([
    { op: "cast", type: "integer", sql_type: "int16", args: [integer] },
    { op: "case_when", args: [boolean, integer, integer] },
    { op: "modulo", sql_type: "int32", args: [integer, integer] },
    { op: "in_list", args: [integer, integer] },
    { op: "not_in_list", args: [integer, integer] },
    { op: "literal", type: "numeric", value: "9007199254740993.125" },
    { op: "literal", type: "numeric", sql_type: "numeric", value: "NaN" },
    { op: "cast", type: "numeric", sql_type: "numeric", args: [integer] },
    { op: "add", sql_type: "numeric", args: [integer, integer] },
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
    { op: "not_in_list", args: Array(128).fill(integer) },
    { ...integer, sql_type: "uuid" },
    { op: "literal", type: "number", sql_type: "numeric", value: 1 },
    { op: "literal", type: "numeric", sql_type: "float64", value: "1" },
    { op: "coalesce", sql_type: "int32", args: [integer, integer] },
  ])("rejects malformed $op contracts before transport", (expression) => {
    expect(() => validate(expression)).toThrow(TypeError);
  });

  it("charges exact numeric literal bytes across the full expression budget", () => {
    const budget = { nodes: 0, literalBytes: 4 * 1024 * 1024 - 2 };
    expect(() =>
      validateRelationalExpression(
        { op: "literal", type: "numeric", value: "1.25" },
        "expression",
        budget
      )
    ).toThrow(/literal budget/);
  });

  it.each([
    { precision: 2, scale: -3 },
    { precision: 2, scale: 4 },
    { precision: 1000, scale: 1000 },
  ])("admits PostgreSQL NUMERIC modifier %j", (numeric_modifier) => {
    expect(() =>
      validate({
        op: "cast",
        type: "numeric",
        sql_type: "numeric",
        numeric_modifier,
        args: [integer],
      })
    ).not.toThrow();
  });

  it.each([
    null,
    undefined,
    {},
    { precision: 2 },
    { precision: 0, scale: 0 },
    { precision: 1001, scale: 0 },
    { precision: 2, scale: -1001 },
    { precision: 2, scale: 1001 },
    { precision: 2.5, scale: 1 },
    { precision: 2, scale: 1.5 },
    { precision: "2", scale: 1 },
    { precision: 2, scale: 1, extra: true },
  ])("rejects malformed modifier %j", (numeric_modifier) => {
    expect(() =>
      validate({
        op: "cast",
        type: "numeric",
        sql_type: "numeric",
        numeric_modifier,
        args: [integer],
      })
    ).toThrow(TypeError);
  });

  it("rejects modifiers on non-NUMERIC casts and non-cast nodes", () => {
    const numeric_modifier = { precision: 4, scale: 2 };
    expect(() =>
      validate({
        op: "cast",
        type: "integer",
        sql_type: "int32",
        numeric_modifier,
        args: [integer],
      })
    ).toThrow(TypeError);
    expect(() => validate({ ...integer, numeric_modifier })).toThrow(TypeError);
  });
});

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
  it("retains signed NUMERIC modifiers in generated recursive contracts", () => {
    const generated: RelationalColumnExpression = {
      column: "n",
      expression: {
        op: "cast",
        type: "numeric",
        sql_type: "numeric",
        numeric_modifier: { precision: 2, scale: -3 },
        args: [{ op: "literal", type: "numeric", value: "99499" }],
      },
    };
    validateRelationalExpression(generated.expression, "expression", { nodes: 0, literalBytes: 0 });
    expect(JSON.parse(JSON.stringify(generated))).toEqual(generated);
  });

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
