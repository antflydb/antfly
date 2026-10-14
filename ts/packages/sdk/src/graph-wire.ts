/** Lossless Cypher transport values, wire version 1. */
export type GraphIdentity = { table: string; key: string };
export type GraphData =
  | null
  | boolean
  | number
  | bigint
  | string
  | GraphData[]
  | { [key: string]: GraphData };
export type GraphValue =
  | { type: "null"; value: null }
  | { type: "boolean"; value: boolean }
  | { type: "integer"; value: string }
  | { type: "number"; value: number }
  | { type: "string"; value: string }
  | { type: "list"; value: GraphValue[] }
  | { type: "map"; value: Record<string, GraphValue> }
  | { type: "node"; value: { identity: GraphIdentity; labels: string[]; properties: GraphValue } }
  | {
      type: "relationship";
      value: {
        identity: GraphIdentity;
        index: string;
        type: string;
        source: GraphIdentity;
        target: GraphIdentity;
        ownership: "explicit" | "document" | "artifact";
        properties: GraphValue;
      };
    }
  | { type: "path"; value: { nodes: GraphValue[]; relationships: GraphValue[] } };

/** Encode data parameters without rounding INTEGER or reserving map keys. */
export function encodeGraphParameter(value: GraphData, depth = 0): GraphValue {
  if (depth > 64) throw new Error("Graph parameter nesting exceeds 64 levels");
  if (value === null) return { type: "null", value: null };
  if (typeof value === "boolean") return { type: "boolean", value };
  if (typeof value === "string") return { type: "string", value };
  if (typeof value === "bigint") {
    if (value < -(1n << 63n) || value > (1n << 63n) - 1n)
      throw new Error("Graph INTEGER exceeds signed 64-bit range");
    return { type: "integer", value: value.toString() };
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("Graph numbers must be finite");
    if (!Number.isInteger(value)) return { type: "number", value };
    if (!Number.isSafeInteger(value))
      throw new Error("Use bigint for a graph INTEGER outside JavaScript's safe integer range");
    return { type: "integer", value: value.toString() };
  }
  if (Array.isArray(value))
    return {
      type: "list",
      value: Array.from(value, (item) => encodeGraphParameter(item, depth + 1)),
    };
  if (
    typeof value !== "object" ||
    (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)
  )
    throw new Error("Graph parameters require plain data objects");
  return {
    type: "map",
    value: Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, encodeGraphParameter(item, depth + 1)])
    ),
  };
}

export type GraphDecoded =
  | null
  | boolean
  | number
  | bigint
  | string
  | GraphDecoded[]
  | { [key: string]: GraphDecoded }
  | Extract<GraphValue, { type: "node" | "relationship" | "path" }>;

/** Decode data values. Entity values stay typed, preserving their identity. */
export function decodeGraphValue(value: GraphValue): GraphDecoded {
  switch (value.type) {
    case "null":
    case "boolean":
    case "number":
    case "string":
      return value.value;
    case "integer":
      return BigInt(value.value);
    case "list":
      return value.value.map(decodeGraphValue);
    case "map":
      return Object.fromEntries(
        Object.entries(value.value).map(([key, item]) => [key, decodeGraphValue(item)])
      );
    default:
      return value;
  }
}
