import { isValidGraphIdentifier } from "./graph-identifier-policy.generated.js";
import type { QueryRequest, QueryResponses } from "./types.js";

const MAX_ALIASES = 64;
const MAX_EDGES = 64;
const MAX_ITEMS = 10_000;
const MAX_PATHS = 100;
const MAX_EDGE_TYPE_BYTES = 64 * 1024;
const DEFAULT_TRAVERSAL_DEPTH = 1;
const DEFAULT_PATH_DEPTH = 10;
const utf8 = new TextEncoder();

type JsonObject = Record<string, unknown>;
type CanonicalEndpoint = { key: string; table?: string };
type GraphDialect = "canonical" | "none";
type CanonicalResultContract = {
  kind: "bindings" | "aggregates" | "nodes" | "paths";
  names?: Set<string>;
  maxItems?: number;
  nodeMode?: "traversal" | "shortest_path" | "k_shortest_paths";
  includePaths?: boolean;
  includeDocuments?: boolean;
  maxDepth?: number;
  queryTable?: string;
  from?: CanonicalEndpoint;
  to?: CanonicalEndpoint;
  objective?: "min_hops" | "min_weight_sum" | "max_weight_product";
  direction?: "out" | "in" | "both";
  edgeTypes?: Set<string>;
  edgeWeightMin?: number;
  edgeWeightMax?: number;
  starts?: CanonicalEndpoint[];
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

function contractEndpoint(value: unknown, path: string, queryTable?: string): CanonicalEndpoint {
  const result = endpoint(value, path);
  const key = result.key as string;
  const table = result.table as string | undefined;
  return table === undefined || table === queryTable ? { key } : { key, table };
}

function endpointMatchesContract(
  actual: JsonObject,
  expected: CanonicalEndpoint,
  queryTable?: string
): boolean {
  if (actual.key !== expected.key) return false;
  if (actual.table === expected.table) return true;
  // Standalone validation cannot distinguish an explicit table from the
  // implicit query table. High-level clients always provide the table.
  return queryTable === undefined && actual.table === undefined && expected.table !== undefined;
}

function edgeContract(
  operation: JsonObject,
  path: string
): Pick<CanonicalResultContract, "direction" | "edgeTypes" | "edgeWeightMin" | "edgeWeightMax"> {
  const direction = operation.direction ?? "out";
  if (direction !== "out" && direction !== "in" && direction !== "both") {
    invalid(`${path}.direction`, "must be out, in, or both");
  }
  let edgeTypes: Set<string> | undefined;
  if (operation.edge_types !== undefined) {
    edgeTypes = new Set(
      array(operation.edge_types, `${path}.edge_types`).map((value, index) =>
        nonemptyString(value, `${path}.edge_types[${index}]`, MAX_EDGE_TYPE_BYTES)
      )
    );
  }
  let edgeWeightMin: number | undefined;
  let edgeWeightMax: number | undefined;
  if (operation.edge_weight !== undefined) {
    const weight = object(operation.edge_weight, `${path}.edge_weight`);
    if (weight.min !== undefined)
      edgeWeightMin = finiteNonnegative(weight.min, `${path}.edge_weight.min`);
    if (weight.max !== undefined)
      edgeWeightMax = finiteNonnegative(weight.max, `${path}.edge_weight.max`);
    if (
      edgeWeightMin !== undefined &&
      edgeWeightMax !== undefined &&
      edgeWeightMin > edgeWeightMax
    ) {
      invalid(`${path}.edge_weight`, "minimum must not exceed maximum");
    }
  }
  return { direction, edgeTypes, edgeWeightMin, edgeWeightMax };
}

function validateEdgeContract(
  edge: JsonObject,
  path: string,
  contract: CanonicalResultContract
): void {
  if (contract.direction !== "both" && edge.direction !== contract.direction)
    invalid(`${path}.direction`, "does not match the requested direction");
  if (
    contract.edgeTypes &&
    contract.edgeTypes.size > 0 &&
    !contract.edgeTypes.has(edge.type as string)
  )
    invalid(`${path}.type`, "was not requested");
  const weight = edge.weight as number;
  if (contract.edgeWeightMin !== undefined && weight < contract.edgeWeightMin)
    invalid(`${path}.weight`, "is below the requested minimum");
  if (contract.edgeWeightMax !== undefined && weight > contract.edgeWeightMax)
    invalid(`${path}.weight`, "exceeds the requested maximum");
}

function pathEdge(
  value: unknown,
  path: string,
  expectedFrom: JsonObject,
  expectedTo: JsonObject,
  maxWeightProduct: boolean
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
  return finiteNonnegative(edge.weight, `${path}.weight`, maxWeightProduct);
}

function floatEqual(left: number, right: number): boolean {
  if (!Number.isFinite(left) || !Number.isFinite(right)) return false;
  return Math.abs(left - right) <= 1e-12 * Math.max(1, Math.abs(left), Math.abs(right));
}

function graphPath(value: unknown, path: string): JsonObject {
  const result = object(value, path);
  exactKeys(result, path, [
    "nodes",
    "edges",
    "length",
    "objective",
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
  const objectiveMode = result.objective;
  if (
    objectiveMode !== "min_hops" &&
    objectiveMode !== "min_weight_sum" &&
    objectiveMode !== "max_weight_product"
  ) {
    invalid(`${path}.objective`, "has an unknown value");
  }
  let sum = 0;
  let product = 1;
  rawEdges.forEach((edge, index) => {
    const weight = pathEdge(
      edge,
      `${path}.edges[${index}]`,
      item(nodes, index, `${path}.nodes[${index}]`),
      item(nodes, index + 1, `${path}.nodes[${index + 1}]`),
      objectiveMode === "max_weight_product"
    );
    sum += weight;
    if (!Number.isFinite(sum)) invalid(path, "path score overflowed");
    if (objectiveMode === "max_weight_product") {
      product *= weight;
      if (!Number.isFinite(product)) invalid(path, "path score overflowed");
    }
  });
  const weightSum = finiteNonnegative(result.weight_sum, `${path}.weight_sum`);
  const objective = finiteNonnegative(result.objective_value, `${path}.objective_value`);
  if (!floatEqual(weightSum, sum))
    invalid(`${path}.weight_sum`, "does not equal the edge-weight sum");
  const expectedObjective =
    objectiveMode === "min_hops" ? length : objectiveMode === "min_weight_sum" ? sum : product;
  if (!floatEqual(objective, expectedObjective))
    invalid(`${path}.objective_value`, "does not match objective");
  return result;
}

function validateGraphPathContract(
  value: JsonObject,
  path: string,
  contract: CanonicalResultContract
): void {
  const length = value.length as number;
  if (contract.maxDepth !== undefined && length > contract.maxDepth)
    invalid(`${path}.length`, "exceeds the requested max_depth");
  if (value.objective !== contract.objective)
    invalid(`${path}.objective`, "does not match the requested objective");
  const nodes = array(value.nodes, `${path}.nodes`).map((node, index) =>
    object(node, `${path}.nodes[${index}]`)
  );
  const first = item(nodes, 0, `${path}.nodes[0]`);
  const last = item(nodes, nodes.length - 1, `${path}.nodes[${nodes.length - 1}]`);
  if (contract.from && !endpointMatchesContract(first, contract.from, contract.queryTable))
    invalid(`${path}.nodes[0]`, "does not match the requested start endpoint");
  if (contract.to && !endpointMatchesContract(last, contract.to, contract.queryTable))
    invalid(`${path}.nodes[${nodes.length - 1}]`, "does not match the requested terminal endpoint");
  array(value.edges, `${path}.edges`).forEach((rawEdge, index) => {
    validateEdgeContract(
      object(rawEdge, `${path}.edges[${index}]`),
      `${path}.edges[${index}]`,
      contract
    );
  });
}

function endpointSignature(value: JsonObject): string {
  return JSON.stringify([value.table ?? null, value.key]);
}

function pathSignature(value: JsonObject): string {
  const nodes = array(value.nodes, "path.nodes").map((node) =>
    endpointSignature(object(node, "path.nodes"))
  );
  const edges = array(value.edges, "path.edges").map((edge) => {
    const result = object(edge, "path.edges");
    return [result.direction, result.type];
  });
  return JSON.stringify([nodes, edges]);
}

function validatePathCollection(
  paths: readonly JsonObject[],
  path: string,
  contract: CanonicalResultContract
): void {
  if (contract.nodeMode !== "k_shortest_paths") return;
  const seenPaths = new Set<string>();
  let previous: number | undefined;
  paths.forEach((graphPath, index) => {
    const nodeSignatures = array(graphPath.nodes, `${path}[${index}].path.nodes`).map((node) =>
      endpointSignature(object(node, `${path}[${index}].path.nodes`))
    );
    if (new Set(nodeSignatures).size !== nodeSignatures.length)
      invalid(`${path}[${index}].path`, "must be loopless for k_shortest_paths");
    const signature = pathSignature(graphPath);
    if (seenPaths.has(signature)) invalid(`${path}[${index}].path`, "duplicates an earlier path");
    seenPaths.add(signature);
    const score = graphPath.objective_value as number;
    if (previous !== undefined) {
      const descending = contract.objective === "max_weight_product";
      if (
        (descending && score > previous && !floatEqual(score, previous)) ||
        (!descending && score < previous && !floatEqual(score, previous))
      ) {
        invalid(`${path}[${index}].path.objective_value`, "is out of objective order");
      }
    }
    previous = score;
  });
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

function validateResultNodeContract(
  node: JsonObject,
  path: string,
  contract: CanonicalResultContract
): void {
  if (contract.maxDepth !== undefined && (node.depth as number) > contract.maxDepth)
    invalid(`${path}.depth`, "exceeds the requested max_depth");
  if (contract.starts) {
    let root: JsonObject | undefined;
    if (node.path !== undefined) {
      const rawPath = array(node.path, `${path}.path`);
      root = object(item(rawPath, 0, `${path}.path[0]`), `${path}.path[0]`);
    } else if (node.depth === 0) {
      root = node;
    }
    if (
      root &&
      !contract.starts.some((start) => endpointMatchesContract(root, start, contract.queryTable))
    ) {
      invalid(path, "does not originate at a requested traversal identity");
    }
  }
  if (node.path_edges !== undefined) {
    array(node.path_edges, `${path}.path_edges`).forEach((rawEdge, index) => {
      validateEdgeContract(
        object(rawEdge, `${path}.path_edges[${index}]`),
        `${path}.path_edges[${index}]`,
        contract
      );
    });
  }
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
        if (!contract.includeDocuments && binding.document !== undefined)
          invalid(`${rowPath}.${alias}.document`, "was returned without being requested");
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
    exactKeys(result, path, ["kind", "nodes", "stats"]);
    const rawNodes = array(result.nodes, `${path}.nodes`);
    const maxItems = contract.maxItems ?? MAX_ITEMS;
    if (rawNodes.length > maxItems) invalid(path, "exceeds the requested result limit");
    const nodes = rawNodes.map((node, index) => resultNode(node, `${path}.nodes[${index}]`));
    if (!contract.includeDocuments) {
      nodes.forEach((node, index) => {
        if (node.document !== undefined)
          invalid(`${path}.nodes[${index}].document`, "was returned without being requested");
      });
    }
    if (contract.nodeMode === "traversal") {
      nodes.forEach((node, index) => {
        if (contract.includePaths) {
          if (node.path === undefined)
            invalid(`${path}.nodes[${index}]`, "is missing its requested path");
        } else {
          if (node.path !== undefined || node.path_edges !== undefined)
            invalid(`${path}.nodes[${index}]`, "contains a path that was not requested");
        }
        validateResultNodeContract(node, `${path}.nodes[${index}]`, contract);
      });
      stats(result.stats, `${path}.stats`, nodes.length, true);
      return;
    }
    invalid(path, "has no node operation contract");
  }
  if (result.kind === "paths") {
    exactKeys(result, path, ["kind", "paths", "stats"]);
    if (contract.nodeMode !== "shortest_path" && contract.nodeMode !== "k_shortest_paths")
      invalid(path, "requires a path operation contract");
    const rawPaths = array(result.paths, `${path}.paths`);
    if (rawPaths.length > Math.min(contract.maxItems ?? MAX_PATHS, MAX_PATHS))
      invalid(path, "exceeds the requested result limit");
    const validatedPaths: JsonObject[] = [];
    rawPaths.forEach((rawItem, index) => {
      const pathItem = object(rawItem, `${path}.paths[${index}]`);
      exactKeys(pathItem, `${path}.paths[${index}]`, ["path"], ["document"]);
      const validatedPath = graphPath(pathItem.path, `${path}.paths[${index}].path`);
      validateGraphPathContract(validatedPath, `${path}.paths[${index}].path`, contract);
      validatedPaths.push(validatedPath);
      if (!contract.includeDocuments && pathItem.document !== undefined)
        invalid(`${path}.paths[${index}].document`, "was returned without being requested");
      if (pathItem.document !== undefined)
        object(pathItem.document, `${path}.paths[${index}].document`);
    });
    validatePathCollection(validatedPaths, `${path}.paths`, contract);
    stats(result.stats, `${path}.stats`, rawPaths.length, false);
    return;
  }
  invalid(`${path}.kind`, "canonical graph results require bindings, aggregates, nodes, or paths");
}

function canonicalOperationContract(
  value: unknown,
  path: string,
  queryTable?: string
): CanonicalResultContract {
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
      return {
        kind: "bindings",
        names,
        maxItems,
        includeDocuments: returned.include_documents === true,
      };
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
    const selector = object(traversal.start, `${path}.traverse.start`);
    let starts: CanonicalEndpoint[] | undefined;
    if (selector.keys !== undefined) {
      starts = array(selector.keys, `${path}.traverse.start.keys`).map((key, index) => ({
        key: nonemptyString(key, `${path}.traverse.start.keys[${index}]`),
      }));
    } else if (selector.identities !== undefined) {
      starts = array(selector.identities, `${path}.traverse.start.identities`).map(
        (identity, index) =>
          contractEndpoint(identity, `${path}.traverse.start.identities[${index}]`, queryTable)
      );
    }
    return {
      kind: "nodes",
      maxItems,
      nodeMode: "traversal",
      includePaths: traversal.include_paths === true,
      includeDocuments: traversal.include_documents === true,
      maxDepth:
        traversal.max_depth === undefined
          ? DEFAULT_TRAVERSAL_DEPTH
          : boundedInteger(traversal.max_depth, `${path}.traverse.max_depth`, 0, MAX_EDGES),
      queryTable,
      starts,
      ...edgeContract(traversal, `${path}.traverse`),
    };
  }
  if (operation.shortest_path !== undefined) {
    const shortestPath = object(operation.shortest_path, `${path}.shortest_path`);
    const objective = shortestPath.objective ?? "min_hops";
    if (
      objective !== "min_hops" &&
      objective !== "min_weight_sum" &&
      objective !== "max_weight_product"
    ) {
      return invalid(`${path}.shortest_path.objective`, "has an unknown value");
    }
    return {
      kind: "paths",
      maxItems: 1,
      nodeMode: "shortest_path",
      includeDocuments: shortestPath.include_documents === true,
      maxDepth:
        shortestPath.max_depth === undefined
          ? DEFAULT_PATH_DEPTH
          : boundedInteger(shortestPath.max_depth, `${path}.shortest_path.max_depth`, 1, MAX_EDGES),
      queryTable,
      from: contractEndpoint(shortestPath.from, `${path}.shortest_path.from`, queryTable),
      to: contractEndpoint(shortestPath.to, `${path}.shortest_path.to`, queryTable),
      objective,
      ...edgeContract(shortestPath, `${path}.shortest_path`),
    };
  }
  if (operation.k_shortest_paths !== undefined) {
    const kShortestPaths = object(operation.k_shortest_paths, `${path}.k_shortest_paths`);
    const objective = kShortestPaths.objective ?? "min_hops";
    if (
      objective !== "min_hops" &&
      objective !== "min_weight_sum" &&
      objective !== "max_weight_product"
    ) {
      return invalid(`${path}.k_shortest_paths.objective`, "has an unknown value");
    }
    return {
      kind: "paths",
      maxItems: boundedInteger(kShortestPaths.k, `${path}.k_shortest_paths.k`, 1, 100),
      nodeMode: "k_shortest_paths",
      includeDocuments: kShortestPaths.include_documents === true,
      maxDepth:
        kShortestPaths.max_depth === undefined
          ? DEFAULT_PATH_DEPTH
          : boundedInteger(
              kShortestPaths.max_depth,
              `${path}.k_shortest_paths.max_depth`,
              1,
              MAX_EDGES
            ),
      queryTable,
      from: contractEndpoint(kShortestPaths.from, `${path}.k_shortest_paths.from`, queryTable),
      to: contractEndpoint(kShortestPaths.to, `${path}.k_shortest_paths.to`, queryTable),
      objective,
      ...edgeContract(kShortestPaths, `${path}.k_shortest_paths`),
    };
  }
  return invalid(path, "does not contain a supported graph operation");
}

function requestDialect(request: QueryRequest, fallbackQueryTable?: string): RequestGraphContract {
  const canonical = request.graph_queries;
  if (canonical !== undefined && canonical !== null) {
    const queryTable = request.table ?? fallbackQueryTable;
    const operations = new Map<string, CanonicalResultContract>();
    for (const [name, operation] of Object.entries(canonical)) {
      operations.set(
        name,
        canonicalOperationContract(operation, `request.graph_queries.${name}`, queryTable)
      );
    }
    return { dialect: "canonical", operations };
  }
  return { dialect: "none", operations: new Map() };
}

export function validateGraphQueryResponses(
  value: QueryResponses | undefined,
  requests: readonly QueryRequest[],
  queryTable?: string
): void {
  const requestContracts = requests.map((request) => requestDialect(request, queryTable));
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
      if (!isValidGraphIdentifier(name)) invalid(path, "has an invalid canonical operation name");
      const contract = operations.get(name);
      if (!contract) invalid(path, "has no canonical request contract");
      canonicalResult(result, path, contract);
    }
  });
}
