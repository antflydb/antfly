import type { RelationalExpressionOp, RelationalExpressionType } from "./types.js";

const arities: Record<RelationalExpressionOp, readonly [number, number]> = {
  literal: [0, 0],
  column: [0, 0],
  add: [2, 2],
  subtract: [2, 2],
  multiply: [2, 2],
  divide: [2, 2],
  negate: [1, 1],
  concat: [2, 32],
  coalesce: [2, 32],
  lower_ascii: [1, 1],
  upper_ascii: [1, 1],
  eq: [2, 2],
  ne: [2, 2],
  gt: [2, 2],
  gte: [2, 2],
  lt: [2, 2],
  lte: [2, 2],
  is_null: [1, 1],
  is_not_null: [1, 1],
  is_distinct: [2, 2],
  is_not_distinct: [2, 2],
  and: [2, 32],
  or: [2, 32],
  not: [1, 1],
};
const operations = new Set(Object.keys(arities));
const comparisons = new Set([
  "eq",
  "ne",
  "gt",
  "gte",
  "lt",
  "lte",
  "is_distinct",
  "is_not_distinct",
]);
const types: Record<RelationalExpressionType, true> = {
  string: true,
  blob: true,
  boolean: true,
  datetime: true,
  integer: true,
  number: true,
};
const typeNames = new Set(Object.keys(types));

export function isRelationalExpressionType(value: unknown): value is RelationalExpressionType {
  return typeof value === "string" && typeNames.has(value);
}

export interface ExpressionBudget {
  nodes: number;
  literalBytes: number;
}

/** Structural checks only: the server binds column references and exact types. */
export function validateRelationalExpression(
  input: unknown,
  path: string,
  aggregate: ExpressionBudget
): void {
  let visited = 0;
  const visit = (value: unknown, location: string, depth: number): void => {
    if (depth >= 16 || ++visited > 128 || ++aggregate.nodes > 4096)
      throw new TypeError(`${path} exceeds the expression depth/node budget`);
    if (value === null || typeof value !== "object" || Array.isArray(value))
      throw new TypeError(`${location} must be an expression object`);
    const node = value as Record<string, unknown>;
    if (typeof node.op !== "string" || !operations.has(node.op))
      throw new TypeError(`${location}.op must be a supported relational expression operation`);
    const op = node.op as RelationalExpressionOp;
    const allowed = new Set(
      op === "literal"
        ? ["op", "type", "value"]
        : op === "column"
          ? ["op", "column"]
          : comparisons.has(op)
            ? ["op", "args", "collation"]
            : ["op", "args"]
    );
    for (const key of Object.keys(node))
      if (!allowed.has(key)) throw new TypeError(`${location}.${key} is not valid for ${op}`);
    if (op === "literal") {
      if (!isRelationalExpressionType(node.type))
        throw new TypeError(`${location}.type is required for a literal`);
      if (node.value != null && !["string", "number", "boolean"].includes(typeof node.value))
        throw new TypeError(`${location}.value must be a scalar or null`);
      if (typeof node.value === "number" && !Number.isFinite(node.value))
        throw new TypeError(`${location}.value must be finite`);
      if (
        (node.type === "integer" || node.type === "datetime") &&
        typeof node.value === "number" &&
        !Number.isSafeInteger(node.value)
      )
        throw new TypeError(`${location}.value must be a safe integer or an exact decimal string`);
      let literalBytes = 8;
      if (typeof node.value === "string") {
        const maxBytes = 1024 * 1024;
        if (node.type === "blob") {
          // Match the native compiler's decoded-byte budget without allocating
          // a second buffer or requiring browser/Node-specific base64 APIs.
          if (
            node.value.length > 4 * Math.ceil(maxBytes / 3) ||
            node.value.length % 4 !== 0 ||
            !/^[A-Za-z0-9+/]*={0,2}$/.test(node.value)
          )
            throw new TypeError(`${location}.value must be bounded standard base64`);
          const padding = node.value.endsWith("==") ? 2 : node.value.endsWith("=") ? 1 : 0;
          literalBytes = (node.value.length / 4) * 3 - padding;
        } else {
          if (node.value.length > maxBytes)
            throw new TypeError(`${location}.value exceeds the literal budget`);
          if (node.type === "string") literalBytes = new TextEncoder().encode(node.value).length;
        }
        if (literalBytes > maxBytes)
          throw new TypeError(`${location}.value exceeds the literal budget`);
      }
      aggregate.literalBytes += literalBytes;
      if (aggregate.literalBytes > 4 * 1024 * 1024)
        throw new TypeError(`${path} exceeds the literal budget`);
      return;
    }
    if (op === "column") {
      if (typeof node.column !== "string" || node.column.length === 0)
        throw new TypeError(`${location}.column must be a non-empty string`);
      return;
    }
    if (
      node.collation !== undefined &&
      (typeof node.collation !== "string" || node.collation.length === 0)
    )
      throw new TypeError(`${location}.collation must be a non-empty string`);
    const [min, max] = arities[op];
    if (!Array.isArray(node.args) || node.args.length < min || node.args.length > max)
      throw new TypeError(
        `${location}.args must contain ${min === max ? min : `${min}–${max}`} expressions`
      );
    node.args.forEach((child, index) => visit(child, `${location}.args[${index}]`, depth + 1));
  };
  visit(input, path, 0);
}
