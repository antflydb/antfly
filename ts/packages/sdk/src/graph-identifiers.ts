// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { isValidGraphIdentifier } from "./graph-identifier-policy.generated.js";
import type { GraphCountAggregate } from "./types.js";

type JSONObject = Record<string, unknown>;

function object(value: unknown): JSONObject | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as JSONObject)
    : undefined;
}

function requireIdentifier(value: unknown, path: string): asserts value is string {
  if (typeof value !== "string" || !isValidGraphIdentifier(value)) {
    throw new TypeError(
      `${path} must satisfy the versioned GraphIdentifier policy ` +
        "(1-128 Unicode code points; no reserved, boundary-space, non-ASCII whitespace, control, or format characters)"
    );
  }
}

function validateEdges(value: unknown, path: string): void {
  if (!Array.isArray(value)) return;
  value.forEach((candidate, index) => {
    const edge = object(candidate);
    if (!edge) return;
    requireIdentifier(edge.from, `${path}[${index}].from`);
    requireIdentifier(edge.to, `${path}[${index}].to`);
  });
}

function validateWhere(root: unknown, path: string): void {
  const stack: Array<{ value: unknown; path: string; depth: number }> = [
    { value: root, path, depth: 0 },
  ];
  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) break;
    const where = object(current.value);
    if (!where) continue;
    if (current.depth >= 16) {
      throw new TypeError(`${current.path} exceeds the maximum graph predicate depth`);
    }

    if (Array.isArray(where.and)) {
      where.and.forEach((child, index) => {
        stack.push({
          value: child,
          path: `${current.path}.and[${index}]`,
          depth: current.depth + 1,
        });
      });
    }
    const notEqual = object(where.not_equal);
    if (notEqual) {
      for (const side of ["left", "right"] as const) {
        const operand = object(notEqual[side]);
        if (operand) requireIdentifier(operand.alias, `${current.path}.not_equal.${side}.alias`);
      }
    }
    const notExists = object(where.not_exists);
    if (notExists) validateEdges(notExists.edges, `${current.path}.not_exists.edges`);
  }
}

function validateMatch(query: JSONObject, path: string): void {
  const match = object(query.match);
  if (!match) return;
  requireIdentifier(match.anchor, `${path}.match.anchor`);

  const nodes = object(match.nodes);
  if (nodes) {
    for (const alias of Object.keys(nodes)) requireIdentifier(alias, `${path}.match.nodes key`);
  }
  validateEdges(match.edges, `${path}.match.edges`);
  validateWhere(match.where, `${path}.match.where`);

  if (Array.isArray(match.optional)) {
    match.optional.forEach((candidate, index) => {
      const optional = object(candidate);
      if (!optional) return;
      const optionalPath = `${path}.match.optional[${index}]`;
      const optionalNodes = object(optional.nodes);
      if (optionalNodes) {
        for (const alias of Object.keys(optionalNodes)) {
          requireIdentifier(alias, `${optionalPath}.nodes key`);
        }
      }
      validateEdges(optional.edges, `${optionalPath}.edges`);
      validateWhere(optional.where, `${optionalPath}.where`);
    });
  }

  const result = object(query.return);
  if (!result) return;
  if (Array.isArray(result.bindings)) {
    result.bindings.forEach((alias, index) => {
      requireIdentifier(alias, `${path}.return.bindings[${index}]`);
    });
  }
  const aggregates = object(result.aggregates);
  if (aggregates) {
    for (const [name, candidate] of Object.entries(aggregates)) {
      requireIdentifier(name, `${path}.return.aggregates key`);
      const aggregate = object(candidate);
      if (aggregate?.count === "*") {
        if ("distinct" in aggregate) {
          throw new TypeError(
            `${path}.return.aggregates[${JSON.stringify(name)}].distinct is only valid for alias counts`
          );
        }
      } else {
        requireIdentifier(
          aggregate?.count,
          `${path}.return.aggregates[${JSON.stringify(name)}].count`
        );
      }
    }
  }
}

/** Construct an exact count over complete graph bindings. */
export function countGraphRows(): GraphCountAggregate {
  return { count: "*" };
}

/** Construct an exact count over the non-null bindings of one alias. */
export function countGraphAlias(alias: string, distinct = false): GraphCountAggregate {
  requireIdentifier(alias, "graph count alias");
  return distinct ? { count: alias, distinct: true } : { count: alias };
}

function validateTraverse(query: JSONObject, path: string): void {
  const traverse = object(query.traverse);
  const start = object(traverse?.start);
  if (!start || !("result_ref" in start)) return;

  const resultRef = start.result_ref;
  const selectorPath = `${path}.traverse.start`;
  if (resultRef !== "$query_results") {
    const prefix = "$graph_results.";
    if (typeof resultRef !== "string" || !resultRef.startsWith(prefix)) {
      throw new TypeError(
        `${selectorPath}.result_ref must be $query_results or $graph_results.<query-name>`
      );
    }
    requireIdentifier(resultRef.slice(prefix.length), `${selectorPath}.result_ref query name`);
  }
  if (start.binding !== undefined && start.binding !== null) {
    if (resultRef === "$query_results") {
      throw new TypeError(
        `${selectorPath}.binding requires a $graph_results.<query-name> reference`
      );
    }
    requireIdentifier(start.binding, `${selectorPath}.binding`);
  }
}

/** Validate every identifier carried by canonical named graph operations. */
export function validateGraphQueryIdentifiers(graphQueries: unknown): void {
  const operations = object(graphQueries);
  if (!operations) return;
  const entries = Object.entries(operations);
  if (entries.length > 64) {
    throw new TypeError("graph_queries accepts at most 64 named operations");
  }
  for (const [name, candidate] of entries) {
    requireIdentifier(name, "graph_queries key");
    const query = object(candidate);
    if (query) {
      const path = `graph_queries[${JSON.stringify(name)}]`;
      validateMatch(query, path);
      validateTraverse(query, path);
    }
  }
}
