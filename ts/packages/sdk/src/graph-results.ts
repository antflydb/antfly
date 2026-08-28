import { isValidGraphIdentifier } from "./graph-identifier-policy.generated.js";
import type { QueryRequest, QueryResponses } from "./types.js";

const MAX_ALIASES = 64;
const MAX_EDGES = 64;
const MAX_ITEMS = 10_000;
const MAX_EDGE_TYPE_BYTES = 64 * 1024;
const utf8 = new TextEncoder();

type JsonObject = Record<string, unknown>;
type GraphDialect = "canonical" | "legacy" | "none";
type CanonicalResultContract = {
  kind: "bindings" | "aggregates" | "nodes";
  names?: Set<string>;
  maxItems?: number;
  nodeMode?: "traversal" | "shortest_path" | "k_shortest_paths";
  includePaths?: boolean;
};
type RequestGraphContract = {
  dialect: GraphDialect;
  operations: Map<string, CanonicalResultContract | undefined>;
};

function invalid(path: string, message: string): never {
  throw new TypeError(`query returned invalid graph response at ${path}: ${message}`);
}

function object(value: unknown, path: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return invalid(path, "must be an object");
  }
  return value as JsonObject;
}

function array(value: unknown, path: string): unknown[] {
  if (!Array.isArray(value)) return invalid(path, "must be an array");
  return value;
}

function exactKeys(
  value: JsonObject,
  path: string,
  required: readonly string[],
  optional: readonly string[] = []
): void {
  const allowed = new Set([...required, ...optional]);
  for (const key of required) {
    if (Object.getOwnPropertyDescriptor(value, key) === undefined)
      invalid(path, `is missing required member ${JSON.stringify(key)}`);
  }
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) invalid(path, `contains unknown member ${JSON.stringify(key)}`);
  }
}

function item<T>(values: readonly T[], index: number, path: string): T {
  const value = values[index];
  if (value === undefined) return invalid(path, "is missing an expected item");
  return value;
}

function nonemptyString(value: unknown, path: string, maxBytes?: number): string {
  if (typeof value !== "string" || value.length === 0)
    return invalid(path, "must be a non-empty string");
  if (maxBytes !== undefined && utf8.encode(value).byteLength > maxBytes) {
    invalid(path, `must encode to at most ${maxBytes} UTF-8 bytes`);
  }
  return value;
}

function boundedInteger(value: unknown, path: string, minimum: number, maximum: number): number {
  if (!Number.isSafeInteger(value) || (value as number) < minimum || (value as number) > maximum) {
    return invalid(path, `must be an integer between ${minimum} and ${maximum}`);
  }
  return value as number;
}

function finiteNonnegative(value: unknown, path: string, atMostOne = false): number {
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    value < 0 ||
    (atMostOne && value > 1)
  ) {
    return invalid(
      path,
      atMostOne ? "must be finite and between 0 and 1" : "must be finite and non-negative"
    );
  }
  return value;
}

function endpoint(value: unknown, path: string): JsonObject {
  const result = object(value, path);
  exactKeys(result, path, ["key"], ["table"]);
  nonemptyString(result.key, `${path}.key`);
  if (result.table !== undefined) nonemptyString(result.table, `${path}.table`);
  return result;
}

function sameEndpoint(left: JsonObject, right: JsonObject): boolean {
  return left.key === right.key && left.table === right.table;
}

function pathEdge(
  value: unknown,
  path: string,
  expectedFrom: JsonObject,
  expectedTo: JsonObject,
  maxWeightMode: boolean
): number {
  const edge = object(value, path);
  exactKeys(edge, path, ["from", "to", "direction", "type", "weight"], ["metadata"]);
  const from = endpoint(edge.from, `${path}.from`);
  const to = endpoint(edge.to, `${path}.to`);
  if (!sameEndpoint(from, expectedFrom) || !sameEndpoint(to, expectedTo)) {
    invalid(path, "endpoints do not match adjacent path nodes");
  }
  if (edge.direction !== "out" && edge.direction !== "in") {
    invalid(`${path}.direction`, "must be out or in");
  }
  nonemptyString(edge.type, `${path}.type`, MAX_EDGE_TYPE_BYTES);
  if (edge.metadata !== undefined) object(edge.metadata, `${path}.metadata`);
  return finiteNonnegative(edge.weight, `${path}.weight`, maxWeightMode);
}

function floatEqual(left: number, right: number): boolean {
  return Math.abs(left - right) <= 1e-12 * Math.max(1, Math.abs(left), Math.abs(right));
}

function graphPath(value: unknown, path: string): JsonObject {
  const result = object(value, path);
  exactKeys(result, path, [
    "nodes",
    "edges",
    "length",
    "weight_mode",
    "weight_sum",
    "objective_value",
  ]);
  const rawNodes = array(result.nodes, `${path}.nodes`);
  const rawEdges = array(result.edges, `${path}.edges`);
  if (rawNodes.length < 1 || rawNodes.length > MAX_EDGES + 1)
    invalid(`${path}.nodes`, "has an invalid length");
  if (rawEdges.length > MAX_EDGES) invalid(`${path}.edges`, "has an invalid length");
  const length = boundedInteger(result.length, `${path}.length`, 0, MAX_EDGES);
  if (length !== rawEdges.length || rawNodes.length !== rawEdges.length + 1) {
    invalid(path, "length, nodes, and edges do not align");
  }
  const nodes = rawNodes.map((node, index) => endpoint(node, `${path}.nodes[${index}]`));
  const mode = result.weight_mode;
  if (mode !== "min_hops" && mode !== "min_weight" && mode !== "max_weight") {
    invalid(`${path}.weight_mode`, "has an unknown value");
  }
  let sum = 0;
  let product = 1;
  rawEdges.forEach((edge, index) => {
    const weight = pathEdge(
      edge,
      `${path}.edges[${index}]`,
      item(nodes, index, `${path}.nodes[${index}]`),
      item(nodes, index + 1, `${path}.nodes[${index + 1}]`),
      mode === "max_weight"
    );
    sum += weight;
    product *= weight;
  });
  const weightSum = finiteNonnegative(result.weight_sum, `${path}.weight_sum`);
  const objective = finiteNonnegative(result.objective_value, `${path}.objective_value`);
  if (!floatEqual(weightSum, sum))
    invalid(`${path}.weight_sum`, "does not equal the edge-weight sum");
  const expectedObjective = mode === "min_hops" ? length : mode === "min_weight" ? sum : product;
  if (!floatEqual(objective, expectedObjective))
    invalid(`${path}.objective_value`, "does not match weight_mode");
  return result;
}

function resultNode(value: unknown, path: string): JsonObject {
  const node = object(value, path);
  exactKeys(
    node,
    path,
    ["key", "depth"],
    ["table", "document", "path", "path_edges", "provenance", "evidence"]
  );
  nonemptyString(node.key, `${path}.key`);
  boundedInteger(node.depth, `${path}.depth`, 0, MAX_EDGES);
  if (node.table !== undefined) nonemptyString(node.table, `${path}.table`);
  if (node.document !== undefined) object(node.document, `${path}.document`);
  if (node.evidence !== undefined) object(node.evidence, `${path}.evidence`);
  if (node.provenance !== undefined) {
    array(node.provenance, `${path}.provenance`).forEach((label, index) => {
      if (typeof label !== "string") invalid(`${path}.provenance[${index}]`, "must be a string");
    });
  }
  let pathNodes: JsonObject[] | undefined;
  if (node.path !== undefined) {
    const rawPath = array(node.path, `${path}.path`);
    if (rawPath.length < 1 || rawPath.length > MAX_EDGES + 1)
      invalid(`${path}.path`, "has an invalid length");
    pathNodes = rawPath.map((item, index) => endpoint(item, `${path}.path[${index}]`));
    if (node.depth !== pathNodes.length - 1)
      invalid(`${path}.depth`, "must equal path length minus one");
    if (!sameEndpoint(item(pathNodes, pathNodes.length - 1, `${path}.path`), node))
      invalid(`${path}.path`, "must terminate at the result node");
  }
  if (node.path_edges !== undefined) {
    const edges = array(node.path_edges, `${path}.path_edges`);
    if (!pathNodes) invalid(`${path}.path_edges`, "requires path");
    if (edges.length + 1 !== pathNodes.length)
      invalid(`${path}.path_edges`, "must align with path");
    for (const [index, edge] of edges.entries()) {
      pathEdge(
        edge,
        `${path}.path_edges[${index}]`,
        item(pathNodes, index, `${path}.path[${index}]`),
        item(pathNodes, index + 1, `${path}.path[${index + 1}]`),
        false
      );
    }
  }
  return node;
}

function stats(value: unknown, path: string, expectedItems: number, allowTruncated: boolean): void {
  const result = object(value, path);
  exactKeys(result, path, ["returned_items", "truncated"]);
  if (
    boundedInteger(result.returned_items, `${path}.returned_items`, 0, MAX_ITEMS) !== expectedItems
  ) {
    invalid(`${path}.returned_items`, "does not match the result payload");
  }
  if (typeof result.truncated !== "boolean") invalid(`${path}.truncated`, "must be a boolean");
  if (!allowTruncated && result.truncated)
    invalid(`${path}.truncated`, "must be false for an exact result");
}

function sameNameSet(actual: readonly string[], expected: Set<string>): boolean {
  return actual.length === expected.size && actual.every((name) => expected.has(name));
}

function canonicalResult(value: unknown, path: string, contract: CanonicalResultContract): void {
  const result = object(value, path);
  if (result.kind !== contract.kind) {
    invalid(`${path}.kind`, `must be ${JSON.stringify(contract.kind)} for the requested operation`);
  }
  if (result.kind === "bindings") {
    exactKeys(result, path, ["kind", "rows", "stats"]);
    const rows = array(result.rows, `${path}.rows`);
    if (rows.length > (contract.maxItems ?? MAX_ITEMS))
      invalid(`${path}.rows`, "exceeds the requested limit");
    rows.forEach((rawRow, rowIndex) => {
      const rowPath = `${path}.rows[${rowIndex}]`;
      const row = object(rawRow, rowPath);
      const entries = Object.entries(row);
      if (entries.length < 1 || entries.length > MAX_ALIASES)
        invalid(rowPath, "has an invalid binding count");
      for (const [alias, rawBinding] of entries) {
        if (!isValidGraphIdentifier(alias))
          invalid(rowPath, `contains invalid alias ${JSON.stringify(alias)}`);
        if (rawBinding === null) continue;
        const binding = object(rawBinding, `${rowPath}.${alias}`);
        exactKeys(binding, `${rowPath}.${alias}`, ["key"], ["table", "document"]);
        nonemptyString(binding.key, `${rowPath}.${alias}.key`);
        if (binding.table !== undefined) nonemptyString(binding.table, `${rowPath}.${alias}.table`);
        if (binding.document !== undefined)
          object(binding.document, `${rowPath}.${alias}.document`);
      }
      if (contract.names && !sameNameSet(Object.keys(row), contract.names)) {
        invalid(rowPath, "binding aliases do not match the requested projection");
      }
    });
    stats(result.stats, `${path}.stats`, rows.length, true);
    return;
  }
  if (result.kind === "aggregates") {
    exactKeys(result, path, ["kind", "aggregates", "stats"]);
    const aggregates = object(result.aggregates, `${path}.aggregates`);
    const entries = Object.entries(aggregates);
    if (entries.length < 1 || entries.length > MAX_ALIASES)
      invalid(`${path}.aggregates`, "has an invalid aggregate count");
    for (const [name, rawAggregate] of entries) {
      if (!isValidGraphIdentifier(name))
        invalid(`${path}.aggregates`, `contains invalid name ${JSON.stringify(name)}`);
      const aggregate = object(rawAggregate, `${path}.aggregates.${name}`);
      exactKeys(aggregate, `${path}.aggregates.${name}`, ["value", "exact"]);
      if (
        typeof aggregate.value !== "string" ||
        !/^[0-9]+$/.test(aggregate.value) ||
        aggregate.exact !== true
      ) {
        invalid(`${path}.aggregates.${name}`, "must contain an exact unsigned decimal value");
      }
    }
    if (contract.names && !sameNameSet(Object.keys(aggregates), contract.names)) {
      invalid(`${path}.aggregates`, "names do not match the requested aggregates");
    }
    stats(result.stats, `${path}.stats`, entries.length, false);
    return;
  }
  if (result.kind === "nodes") {
    exactKeys(result, path, ["kind", "nodes", "paths", "stats"]);
    const rawNodes = array(result.nodes, `${path}.nodes`);
    const rawPaths = array(result.paths, `${path}.paths`);
    const maxItems = contract.maxItems ?? MAX_ITEMS;
    if (rawNodes.length > maxItems || rawPaths.length > maxItems)
      invalid(path, "exceeds the requested result limit");
    const nodes = rawNodes.map((node, index) => resultNode(node, `${path}.nodes[${index}]`));
    const paths = rawPaths.map((item, index) => graphPath(item, `${path}.paths[${index}]`));
    if (contract.nodeMode === "traversal") {
      if (paths.length !== 0) invalid(`${path}.paths`, "traversal paths belong on result nodes");
      nodes.forEach((node, index) => {
        if (contract.includePaths) {
          if (node.path === undefined)
            invalid(`${path}.nodes[${index}]`, "is missing its requested path");
        } else {
          if (node.path !== undefined || node.path_edges !== undefined)
            invalid(`${path}.nodes[${index}]`, "contains a path that was not requested");
        }
      });
      stats(result.stats, `${path}.stats`, nodes.length, true);
      return;
    }
    if (contract.nodeMode === "shortest_path" || contract.nodeMode === "k_shortest_paths") {
      if (nodes.length !== paths.length) invalid(path, "requires one terminal node per path");
      paths.forEach((graphPathValue, index) => {
        const node = item(nodes, index, `${path}.nodes[${index}]`);
        if (node.path !== undefined || node.path_edges !== undefined)
          invalid(`${path}.nodes[${index}]`, "duplicates its authoritative top-level path");
        if (node.depth !== graphPathValue.length)
          invalid(`${path}.nodes[${index}].depth`, "does not match its path length");
        const endpoints = graphPathValue.nodes as unknown[];
        if (
          !sameEndpoint(
            object(endpoints[endpoints.length - 1], `${path}.paths[${index}].nodes`),
            node
          )
        ) {
          invalid(`${path}.nodes[${index}]`, "does not match its path terminal");
        }
      });
      stats(result.stats, `${path}.stats`, paths.length, false);
      return;
    }
    invalid(path, "has no node operation contract");
  }
  invalid(`${path}.kind`, "canonical graph results require bindings, aggregates, or nodes");
}

function canonicalOperationContract(value: unknown, path: string): CanonicalResultContract {
  const operation = object(value, path);
  if (operation.match !== undefined) {
    const returned = object(operation.return, `${path}.return`);
    if (returned.bindings !== undefined) {
      const bindings = array(returned.bindings, `${path}.return.bindings`);
      const names = new Set<string>();
      bindings.forEach((name, index) => {
        if (typeof name !== "string")
          invalid(`${path}.return.bindings[${index}]`, "must be a string");
        names.add(name);
      });
      const rawLimit = returned.limit;
      const maxItems =
        rawLimit === undefined
          ? 100
          : boundedInteger(rawLimit, `${path}.return.limit`, 1, MAX_ITEMS);
      return { kind: "bindings", names, maxItems };
    }
    if (returned.aggregates !== undefined) {
      return {
        kind: "aggregates",
        names: new Set(Object.keys(object(returned.aggregates, `${path}.return.aggregates`))),
      };
    }
    return invalid(`${path}.return`, "must select bindings or aggregates");
  }
  if (operation.traverse !== undefined) {
    const traversal = object(operation.traverse, `${path}.traverse`);
    const rawLimit = traversal.limit;
    const maxItems =
      rawLimit === undefined
        ? 100
        : boundedInteger(rawLimit, `${path}.traverse.limit`, 1, MAX_ITEMS);
    return {
      kind: "nodes",
      maxItems,
      nodeMode: "traversal",
      includePaths: traversal.include_paths === true,
    };
  }
  if (operation.shortest_path !== undefined) {
    return { kind: "nodes", maxItems: 1, nodeMode: "shortest_path" };
  }
  if (operation.k_shortest_paths !== undefined) {
    const kShortestPaths = object(operation.k_shortest_paths, `${path}.k_shortest_paths`);
    return {
      kind: "nodes",
      maxItems: boundedInteger(kShortestPaths.k, `${path}.k_shortest_paths.k`, 1, 100),
      nodeMode: "k_shortest_paths",
    };
  }
  return invalid(path, "does not contain a supported graph operation");
}

function legacySafeInteger(value: unknown, path: string): number {
  if (!Number.isSafeInteger(value)) invalid(path, "must be a safely representable integer");
  return value as number;
}

function legacyPathEdge(value: unknown, path: string): void {
  const edge = object(value, path);
  for (const name of ["source", "target", "type"] as const) {
    if (edge[name] !== undefined && typeof edge[name] !== "string")
      invalid(`${path}.${name}`, "must be a string");
  }
  if (
    edge.weight !== undefined &&
    (typeof edge.weight !== "number" || !Number.isFinite(edge.weight))
  )
    invalid(`${path}.weight`, "must be a finite number");
  if (edge.metadata !== undefined) object(edge.metadata, `${path}.metadata`);
}

function legacyPath(value: unknown, path: string): void {
  const graphPath = object(value, path);
  if (graphPath.nodes !== undefined) {
    array(graphPath.nodes, `${path}.nodes`).forEach((node, index) => {
      if (typeof node !== "string") invalid(`${path}.nodes[${index}]`, "must be a string");
    });
  }
  if (graphPath.edges !== undefined)
    array(graphPath.edges, `${path}.edges`).forEach((edge, index) => {
      legacyPathEdge(edge, `${path}.edges[${index}]`);
    });
  if (graphPath.total_weight !== undefined)
    finiteNonnegative(graphPath.total_weight, `${path}.total_weight`);
  if (graphPath.length !== undefined) legacySafeInteger(graphPath.length, `${path}.length`);
}

function legacyNode(value: unknown, path: string): void {
  const node = object(value, path);
  if (typeof node.key !== "string") invalid(`${path}.key`, "must be a string");
  if (node.table !== undefined && typeof node.table !== "string")
    invalid(`${path}.table`, "must be a string");
  if (node.depth !== undefined) legacySafeInteger(node.depth, `${path}.depth`);
  if (
    node.distance !== undefined &&
    (typeof node.distance !== "number" || !Number.isFinite(node.distance))
  )
    invalid(`${path}.distance`, "must be a finite number");
  if (node.document !== undefined) object(node.document, `${path}.document`);
  if (node.evidence !== undefined) object(node.evidence, `${path}.evidence`);
  for (const name of ["path", "provenance"] as const) {
    if (node[name] !== undefined)
      array(node[name], `${path}.${name}`).forEach((item, index) => {
        if (typeof item !== "string") invalid(`${path}.${name}[${index}]`, "must be a string");
      });
  }
  if (node.path_edges !== undefined)
    array(node.path_edges, `${path}.path_edges`).forEach((edge, index) => {
      legacyPathEdge(edge, `${path}.path_edges[${index}]`);
    });
  if (node.edges !== undefined) array(node.edges, `${path}.edges`);
}

function legacyResult(value: unknown, path: string): void {
  const result = object(value, path);
  exactKeys(result, path, ["type", "total"], ["kind", "nodes", "paths", "matches", "took"]);
  if (result.kind !== undefined && result.kind !== "legacy")
    invalid(`${path}.kind`, "must be legacy when present");
  if (
    result.type !== "neighbors" &&
    result.type !== "traverse" &&
    result.type !== "shortest_path" &&
    result.type !== "k_shortest_paths" &&
    result.type !== "pattern"
  ) {
    invalid(`${path}.type`, "has an unknown legacy graph query type");
  }
  legacySafeInteger(result.total, `${path}.total`);
  if (result.took !== undefined) legacySafeInteger(result.took, `${path}.took`);
  if (result.nodes !== undefined)
    array(result.nodes, `${path}.nodes`).forEach((node, index) => {
      legacyNode(node, `${path}.nodes[${index}]`);
    });
  if (result.paths !== undefined)
    array(result.paths, `${path}.paths`).forEach((graphPath, index) => {
      legacyPath(graphPath, `${path}.paths[${index}]`);
    });
  if (result.matches !== undefined) {
    array(result.matches, `${path}.matches`).forEach((rawMatch, index) => {
      const matchPath = `${path}.matches[${index}]`;
      const match = object(rawMatch, matchPath);
      if (match.bindings !== undefined) {
        for (const [name, binding] of Object.entries(
          object(match.bindings, `${matchPath}.bindings`)
        ))
          legacyNode(binding, `${matchPath}.bindings.${name}`);
      }
      if (match.path !== undefined)
        array(match.path, `${matchPath}.path`).forEach((edge, edgeIndex) => {
          legacyPathEdge(edge, `${matchPath}.path[${edgeIndex}]`);
        });
    });
  }
}

function requestDialect(request: QueryRequest): RequestGraphContract {
  const canonical = request.graph_queries;
  const legacy = request.graph_searches;
  if (canonical !== undefined && canonical !== null && legacy !== undefined && legacy !== null) {
    throw new TypeError("query accepts either graph_queries or graph_searches, not both");
  }
  if (canonical !== undefined && canonical !== null) {
    const operations = new Map<string, CanonicalResultContract>();
    for (const [name, operation] of Object.entries(canonical)) {
      operations.set(name, canonicalOperationContract(operation, `request.graph_queries.${name}`));
    }
    return { dialect: "canonical", operations };
  }
  if (legacy !== undefined && legacy !== null) {
    return {
      dialect: "legacy",
      operations: new Map(Object.keys(legacy).map((name) => [name, undefined])),
    };
  }
  return { dialect: "none", operations: new Map() };
}

export function validateGraphQueryResponses(
  value: QueryResponses | undefined,
  requests: readonly QueryRequest[]
): void {
  const requestContracts = requests.map(requestDialect);
  const requiresGraphResults = requestContracts.some(({ dialect }) => dialect !== "none");
  if (!value) {
    if (requiresGraphResults) invalid("response", "is missing a graph query response");
    return;
  }
  const responses = value.responses;
  if (!Array.isArray(responses)) {
    if (requiresGraphResults) invalid("response.responses", "must be an array");
    return;
  }
  if (requiresGraphResults && responses.length !== requests.length) {
    invalid("response.responses", "must contain exactly one response per request");
  }
  responses.forEach((rawResponse, index) => {
    const request = requests[index];
    if (!request) invalid(`response.responses[${index}]`, "has no corresponding request");
    const { dialect, operations } = item(requestContracts, index, `response.responses[${index}]`);
    const response = object(rawResponse, `response.responses[${index}]`);
    const rawResults = response.graph_results;
    if (rawResults === undefined) {
      if (operations.size > 0) invalid(`response.responses[${index}]`, "is missing graph_results");
      return;
    }
    const results = object(rawResults, `response.responses[${index}].graph_results`);
    const names = Object.keys(results);
    if (dialect === "none") {
      if (names.length > 0)
        invalid(
          `response.responses[${index}].graph_results`,
          "was returned without graph operations"
        );
      return;
    }
    if (!sameNameSet(names, new Set(operations.keys())))
      invalid(
        `response.responses[${index}].graph_results`,
        "operation names do not match the request"
      );
    for (const name of names) {
      const path = `response.responses[${index}].graph_results[${JSON.stringify(name)}]`;
      const result = object(results[name], path);
      if (dialect === "canonical") {
        if (!isValidGraphIdentifier(name)) invalid(path, "has an invalid canonical operation name");
        const contract = operations.get(name);
        if (!contract) invalid(path, "has no canonical request contract");
        canonicalResult(result, path, contract);
      } else legacyResult(result, path);
    }
  });
}
