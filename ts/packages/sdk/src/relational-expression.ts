import type { RelationalExpressionOp, RelationalExpressionType, SQLBuiltinType } from "./types.js";

const arities: Record<RelationalExpressionOp, readonly [number, number]> = {
  literal: [0, 0],
  column: [0, 0],
  add: [2, 2],
  subtract: [2, 2],
  multiply: [2, 2],
  divide: [2, 2],
  modulo: [2, 2],
  cast: [1, 1],
  case_when: [3, 31],
  in_list: [2, 32],
  not_in_list: [2, 32],
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
const numericOperations = new Set(["add", "subtract", "multiply", "divide", "modulo", "negate"]);
const integerIdentities = new Set(["int16", "int32", "int64"]);
const floatIdentities = new Set(["float32", "float64"]);
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
  numeric: true,
  sql_array: true,
};
const typeNames = new Set(Object.keys(types));
const arrayTypes: Record<SQLBuiltinType, true> = {
  text: true,
  int16: true,
  int32: true,
  int64: true,
  float32: true,
  float64: true,
  boolean: true,
  uuid: true,
  jsonb: true,
  numeric: true,
};
const arrayIdentities = new Set(Object.keys(arrayTypes));

// Count bounded JSON without allocating a second serialized envelope. The
// depth cap includes the envelope and values array around a 64-level JSONB cell.
function arrayLiteralBytes(input: unknown, location: string): number {
  const fail = (): never => {
    throw new TypeError(`${location} requires a bounded ordinal SQL array envelope`);
  };
  if (input === null || typeof input !== "object" || Array.isArray(input)) return fail();
  const envelope = input as Record<string, unknown>;
  if (Object.keys(envelope).length !== 3) return fail();
  const { dimensions, values, sql_nulls: nulls } = envelope;
  if (
    !Array.isArray(dimensions) ||
    dimensions.length > 6 ||
    !Array.isArray(values) ||
    !Array.isArray(nulls) ||
    values.length !== nulls.length
  )
    return fail();
  let count = dimensions.length === 0 ? 0 : 1;
  for (const axis of dimensions) {
    if (
      axis === null ||
      typeof axis !== "object" ||
      Array.isArray(axis) ||
      Object.keys(axis).length !== 2
    )
      return fail();
    const { length, lower_bound: lower } = axis as Record<string, unknown>;
    if (
      typeof length !== "number" ||
      !Number.isInteger(length) ||
      length <= 0 ||
      length > 2147483647 ||
      typeof lower !== "number" ||
      !Number.isInteger(lower) ||
      lower < -2147483648 ||
      lower + length > 2147483647
    )
      return fail();
    count *= length;
    if (count > 1024 * 1024) return fail();
  }
  if (count !== values.length) return fail();
  for (let i = 0; i < count; i++)
    if (typeof nulls[i] !== "boolean" || (nulls[i] && values[i] !== null)) return fail();
  let bytes = 0;
  const charge = (amount: number): void => {
    bytes += amount;
    if (bytes > 1024 * 1024) fail();
  };
  const text = (value: string): void => {
    charge(2);
    for (let i = 0; i < value.length; i++) {
      const unit = value.charCodeAt(i);
      if (
        unit === 34 ||
        unit === 92 ||
        unit === 8 ||
        unit === 9 ||
        unit === 10 ||
        unit === 12 ||
        unit === 13
      )
        charge(2);
      else if (unit < 32) charge(6);
      else if (unit < 128) charge(1);
      else if (unit < 2048) charge(2);
      else if (unit >= 0xd800 && unit <= 0xdbff) {
        const low = value.charCodeAt(++i);
        if (!(low >= 0xdc00 && low <= 0xdfff)) fail();
        charge(4);
      } else {
        if (unit >= 0xdc00 && unit <= 0xdfff) fail();
        charge(3);
      }
    }
  };
  const visit = (value: unknown, depth: number): void => {
    if (depth > 66) fail();
    if (value === null) {
      charge(4);
      return;
    }
    if (typeof value === "string") {
      text(value);
      return;
    }
    if (typeof value === "boolean") {
      charge(value ? 4 : 5);
      return;
    }
    if (typeof value === "number") {
      if (!Number.isFinite(value)) fail();
      charge(String(value).length);
      return;
    }
    if (Array.isArray(value)) {
      charge(2);
      for (let i = 0; i < value.length; i++) {
        if (i !== 0) charge(1);
        visit(value[i], depth + 1);
      }
      return;
    }
    if (typeof value !== "object" || value === null) return fail();
    const proto = Object.getPrototypeOf(value);
    if (proto !== Object.prototype && proto !== null) fail();
    charge(2);
    let first = true;
    for (const key in value)
      if (Object.prototype.hasOwnProperty.call(value, key)) {
        if (!first) charge(1);
        first = false;
        text(key);
        charge(1);
        visit((value as Record<string, unknown>)[key], depth + 1);
      }
  };
  visit(input, 0);
  return bytes;
}

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
        ? ["op", "type", "value", "sql_type"]
        : op === "column"
          ? ["op", "column"]
          : op === "cast"
            ? ["op", "type", "sql_type", "numeric_modifier", "args"]
            : numericOperations.has(op)
              ? ["op", "args", "sql_type"]
              : comparisons.has(op)
                ? ["op", "args", "collation"]
                : ["op", "args"]
    );
    for (const key of Object.keys(node))
      if (!allowed.has(key)) throw new TypeError(`${location}.${key} is not valid for ${op}`);
    const arrayLiteral = op === "literal" && node.type === "sql_array";
    if (arrayLiteral && (typeof node.sql_type !== "string" || !arrayIdentities.has(node.sql_type)))
      throw new TypeError(`${location}.sql_type requires a supported SQL array element identity`);
    if (node.sql_type !== undefined && !arrayLiteral) {
      const integer = typeof node.sql_type === "string" && integerIdentities.has(node.sql_type);
      const floating = typeof node.sql_type === "string" && floatIdentities.has(node.sql_type);
      const exact = node.sql_type === "numeric";
      if (
        (!integer && !floating && !exact) ||
        ((op === "literal" || op === "cast") &&
          !(
            (node.type === "integer" && integer) ||
            (node.type === "number" && floating) ||
            (node.type === "numeric" && exact)
          ))
      )
        throw new TypeError(`${location}.sql_type must match a supported numeric builtin identity`);
    }
    if (op === "cast" && node.sql_type === undefined)
      throw new TypeError(`${location}.sql_type is required for a numeric cast`);
    if (Object.prototype.hasOwnProperty.call(node, "numeric_modifier")) {
      const modifier = node.numeric_modifier;
      if (
        node.type !== "numeric" ||
        node.sql_type !== "numeric" ||
        modifier === null ||
        typeof modifier !== "object" ||
        Array.isArray(modifier)
      )
        throw new TypeError(
          `${location}.numeric_modifier requires a NUMERIC cast and precision/scale object`
        );
      const fields = modifier as Record<string, unknown>;
      if (
        Object.keys(fields).length !== 2 ||
        !Object.prototype.hasOwnProperty.call(fields, "precision") ||
        !Object.prototype.hasOwnProperty.call(fields, "scale") ||
        typeof fields.precision !== "number" ||
        !Number.isInteger(fields.precision) ||
        fields.precision < 1 ||
        fields.precision > 1000 ||
        typeof fields.scale !== "number" ||
        !Number.isInteger(fields.scale) ||
        fields.scale < -1000 ||
        fields.scale > 1000
      )
        throw new TypeError(
          `${location}.numeric_modifier requires integer precision 1..1000 and scale -1000..1000`
        );
    }
    if (op === "literal") {
      if (!isRelationalExpressionType(node.type))
        throw new TypeError(`${location}.type is required for a literal`);
      if (
        !arrayLiteral &&
        node.value != null &&
        !["string", "number", "boolean"].includes(typeof node.value)
      )
        throw new TypeError(`${location}.value must be a scalar or null`);
      if (typeof node.value === "number" && !Number.isFinite(node.value))
        throw new TypeError(`${location}.value must be finite`);
      if (
        (node.type === "integer" || node.type === "datetime") &&
        typeof node.value === "number" &&
        !Number.isSafeInteger(node.value)
      )
        throw new TypeError(`${location}.value must be a safe integer or an exact decimal string`);
      let literalBytes =
        arrayLiteral && node.value != null ? arrayLiteralBytes(node.value, `${location}.value`) : 8;
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
          literalBytes = new TextEncoder().encode(node.value).length;
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
    if (op === "case_when" && node.args.length % 2 !== 1)
      throw new TypeError(`${location}.args requires condition/result pairs and a fallback`);
    node.args.forEach((child, index) => {
      visit(child, `${location}.args[${index}]`, depth + 1);
    });
  };
  visit(input, path, 0);
}
