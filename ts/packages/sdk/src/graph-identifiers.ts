// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { isValidGraphIdentifier } from "./graph-identifier-policy.generated.js";
import type { GraphCountAggregate, GraphDocumentFilter } from "./types.js";

type JSONObject = Record<string, unknown>;
const MAX_ANTFLY_UNIX_NS = (1n << 64n) - 1n;
const NS_PER_SECOND = 1_000_000_000n;
const MAX_GRAPH_EDGE_TYPES = 64;
const MAX_GRAPH_EDGE_TYPE_UTF8_BYTES = 64 * 1024;
const MAX_GRAPH_MATCH_QUERIES = 8;

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

function validUtf8ByteLength(value: string): number | undefined {
  let bytes = 0;
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit <= 0x7f) bytes += 1;
    else if (unit <= 0x7ff) bytes += 2;
    else if (unit >= 0xd800 && unit <= 0xdbff) {
      const low = value.charCodeAt(index + 1);
      if (!(low >= 0xdc00 && low <= 0xdfff)) return undefined;
      bytes += 4;
      index += 1;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) return undefined;
    else bytes += 3;
  }
  return bytes;
}

function validateEdgeTypes(value: unknown, path: string): void {
  if (value === undefined) return;
  if (!Array.isArray(value) || value.length > MAX_GRAPH_EDGE_TYPES) {
    throw new TypeError(`${path} must contain at most ${MAX_GRAPH_EDGE_TYPES} edge types`);
  }
  const seen = new Set<string>();
  let totalBytes = 0;
  value.forEach((candidate, index) => {
    if (typeof candidate !== "string" || candidate.length === 0) {
      throw new TypeError(`${path}[${index}] must be a non-empty valid UTF-8 string`);
    }
    const bytes = validUtf8ByteLength(candidate);
    if (bytes === undefined) {
      throw new TypeError(`${path}[${index}] must be a non-empty valid UTF-8 string`);
    }
    if (seen.has(candidate)) throw new TypeError(`${path} must not contain duplicate edge types`);
    seen.add(candidate);
    totalBytes += bytes;
    if (totalBytes > MAX_GRAPH_EDGE_TYPE_UTF8_BYTES) {
      throw new TypeError(
        `${path} must encode to at most ${MAX_GRAPH_EDGE_TYPE_UTF8_BYTES} UTF-8 bytes`
      );
    }
  });
}

function validateWeightBounds(value: JSONObject, path: string): void {
  const minWeight = value.min_weight;
  const maxWeight = value.max_weight;
  for (const [field, bound] of [
    ["min_weight", minWeight],
    ["max_weight", maxWeight],
  ] as const) {
    if (
      bound !== undefined &&
      (typeof bound !== "number" || !Number.isFinite(bound) || bound < 0)
    ) {
      throw new TypeError(`${path}.${field} must be a finite non-negative number`);
    }
  }
  if (typeof minWeight === "number" && typeof maxWeight === "number" && minWeight > maxWeight) {
    throw new TypeError(`${path}.min_weight must not exceed max_weight`);
  }
}

function requireGraphDocumentPath(path: unknown): asserts path is string {
  if (typeof path !== "string" || !path.startsWith("/") || /~(?:[^01]|$)/.test(path)) {
    throw new TypeError("graph document filter path must be a valid RFC 6901 JSON Pointer");
  }
}

function requireRfc3339DateTime(value: unknown, path: string): bigint {
  if (typeof value !== "string") {
    throw new TypeError(`${path} must be an RFC 3339 date-time with a UTC offset`);
  }
  const match =
    /^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:[Zz]|([+-])(\d{2}):(\d{2}))$/.exec(
      value
    );
  if (!match) {
    throw new TypeError(`${path} must be an RFC 3339 date-time with a UTC offset`);
  }

  const [
    ,
    yearText,
    monthText,
    dayText,
    hourText,
    minuteText,
    secondText,
    fractionText,
    offsetSign,
    offsetHourText,
    offsetMinuteText,
  ] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  const valid =
    year >= 1 &&
    month >= 1 &&
    month <= 12 &&
    day >= 1 &&
    day <= (daysInMonth[month - 1] ?? 0) &&
    Number(hourText) <= 23 &&
    Number(minuteText) <= 59 &&
    Number(secondText) <= 59 &&
    (offsetHourText === undefined || Number(offsetHourText) <= 23) &&
    (offsetMinuteText === undefined || Number(offsetMinuteText) <= 59);
  if (!valid) {
    throw new TypeError(`${path} must be a valid RFC 3339 date-time`);
  }

  const localSeconds =
    daysFromCivil(year, month, day) * 86_400n +
    BigInt(Number(hourText) * 3_600 + Number(minuteText) * 60 + Number(secondText));
  const offsetMagnitude = BigInt(
    Number(offsetHourText ?? 0) * 3_600 + Number(offsetMinuteText ?? 0) * 60
  );
  const offsetSeconds = offsetSign === "-" ? -offsetMagnitude : offsetMagnitude;
  const fractionNs = BigInt((fractionText ?? "").padEnd(9, "0") || "0");
  const unixNs = (localSeconds - offsetSeconds) * NS_PER_SECOND + fractionNs;
  if (unixNs < 0n || unixNs > MAX_ANTFLY_UNIX_NS) {
    throw new TypeError(
      `${path} must fall within Antfly's supported Unix-nanosecond range ` +
        "(1970-01-01T00:00:00Z through 2554-07-21T23:34:33.709551615Z)"
    );
  }
  return unixNs;
}

function daysFromCivil(year: number, month: number, day: number): bigint {
  const adjustedYear = BigInt(year - (month <= 2 ? 1 : 0));
  const era = adjustedYear / 400n;
  const yearOfEra = adjustedYear - era * 400n;
  const adjustedMonth = BigInt(month + (month > 2 ? -3 : 9));
  const dayOfYear = (153n * adjustedMonth + 2n) / 5n + BigInt(day - 1);
  const dayOfEra = yearOfEra * 365n + yearOfEra / 4n - yearOfEra / 100n + dayOfYear;
  return era * 146_097n + dayOfEra - 719_468n;
}

function requireRangeOptions(value: unknown, name: string, allowedKeys: readonly string[]): void {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError(`${name} options must be an object`);
  }
  for (const key of Object.keys(value)) {
    if (!allowedKeys.includes(key)) {
      throw new TypeError(`${name} options contain unsupported property ${JSON.stringify(key)}`);
    }
  }
}

function optionalBoolean(value: unknown, path: string): boolean | undefined {
  if (value !== undefined && typeof value !== "boolean") {
    throw new TypeError(`${path} must be a boolean`);
  }
  return value;
}

export interface GraphNumericRangeOptions {
  min?: number;
  max?: number;
  inclusiveMin?: boolean;
  inclusiveMax?: boolean;
}

export interface GraphTermRangeOptions {
  min?: string;
  max?: string;
  inclusiveMin?: boolean;
  inclusiveMax?: boolean;
}

export interface GraphDateRangeOptions {
  start?: string;
  end?: string;
  inclusiveStart?: boolean;
  inclusiveEnd?: boolean;
}

/** Construct a validated non-scoring numeric range predicate for a graph node. */
export function graphNumericRangeFilter(
  path: string,
  options: GraphNumericRangeOptions
): GraphDocumentFilter {
  requireGraphDocumentPath(path);
  requireRangeOptions(options, "graph numeric range", [
    "min",
    "max",
    "inclusiveMin",
    "inclusiveMax",
  ]);
  if (options.min === undefined && options.max === undefined) {
    throw new TypeError("graph numeric range requires min or max");
  }
  if (
    (options.min !== undefined && !Number.isFinite(options.min)) ||
    (options.max !== undefined && !Number.isFinite(options.max))
  ) {
    throw new TypeError("graph numeric range bounds must be finite numbers");
  }
  if (options.min !== undefined && options.max !== undefined && options.min > options.max) {
    throw new TypeError("graph numeric range min must not exceed max");
  }
  const inclusiveMin = optionalBoolean(options.inclusiveMin, "graph numeric range inclusiveMin");
  const inclusiveMax = optionalBoolean(options.inclusiveMax, "graph numeric range inclusiveMax");
  return {
    numeric_range: {
      path,
      ...(options.min === undefined ? {} : { min: options.min }),
      ...(options.max === undefined ? {} : { max: options.max }),
      ...(inclusiveMin === undefined ? {} : { inclusive_min: inclusiveMin }),
      ...(inclusiveMax === undefined ? {} : { inclusive_max: inclusiveMax }),
    },
  };
}

/** Construct a validated non-scoring lexical range predicate for a graph node. */
export function graphTermRangeFilter(
  path: string,
  options: GraphTermRangeOptions
): GraphDocumentFilter {
  requireGraphDocumentPath(path);
  requireRangeOptions(options, "graph term range", ["min", "max", "inclusiveMin", "inclusiveMax"]);
  if (options.min === undefined && options.max === undefined) {
    throw new TypeError("graph term range requires min or max");
  }
  if (
    (options.min !== undefined && typeof options.min !== "string") ||
    (options.max !== undefined && typeof options.max !== "string")
  ) {
    throw new TypeError("graph term range bounds must be strings");
  }
  const inclusiveMin = optionalBoolean(options.inclusiveMin, "graph term range inclusiveMin");
  const inclusiveMax = optionalBoolean(options.inclusiveMax, "graph term range inclusiveMax");
  return {
    term_range: {
      path,
      ...(options.min === undefined ? {} : { min: options.min }),
      ...(options.max === undefined ? {} : { max: options.max }),
      ...(inclusiveMin === undefined ? {} : { inclusive_min: inclusiveMin }),
      ...(inclusiveMax === undefined ? {} : { inclusive_max: inclusiveMax }),
    },
  };
}

/** Construct a validated non-scoring RFC 3339 date range predicate for a graph node. */
export function graphDateRangeFilter(
  path: string,
  options: GraphDateRangeOptions
): GraphDocumentFilter {
  requireGraphDocumentPath(path);
  requireRangeOptions(options, "graph date range", [
    "start",
    "end",
    "inclusiveStart",
    "inclusiveEnd",
  ]);
  if (options.start === undefined && options.end === undefined) {
    throw new TypeError("graph date range requires start or end");
  }
  const startNs =
    options.start === undefined
      ? undefined
      : requireRfc3339DateTime(options.start, "graph date range start");
  const endNs =
    options.end === undefined
      ? undefined
      : requireRfc3339DateTime(options.end, "graph date range end");
  if (startNs !== undefined && endNs !== undefined && startNs > endNs) {
    throw new TypeError("graph date range start must not exceed end");
  }
  const inclusiveStart = optionalBoolean(options.inclusiveStart, "graph date range inclusiveStart");
  const inclusiveEnd = optionalBoolean(options.inclusiveEnd, "graph date range inclusiveEnd");
  return {
    date_range: {
      path,
      ...(options.start === undefined ? {} : { start: options.start }),
      ...(options.end === undefined ? {} : { end: options.end }),
      ...(inclusiveStart === undefined ? {} : { inclusive_start: inclusiveStart }),
      ...(inclusiveEnd === undefined ? {} : { inclusive_end: inclusiveEnd }),
    },
  };
}

function validateEdges(value: unknown, path: string): void {
  if (!Array.isArray(value)) return;
  value.forEach((candidate, index) => {
    const edge = object(candidate);
    if (!edge) return;
    requireIdentifier(edge.from, `${path}[${index}].from`);
    requireIdentifier(edge.to, `${path}[${index}].to`);
    validateDirection(edge.direction, `${path}[${index}].direction`);
    validateEdgeTypes(edge.types, `${path}[${index}].types`);
    validateWeightBounds(edge, `${path}[${index}]`);
  });
}

function validateDirection(value: unknown, path: string): void {
  if (value !== undefined && value !== "out" && value !== "in" && value !== "both") {
    throw new TypeError(`${path} must be out, in, or both`);
  }
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
  validateDirection(traverse?.direction, `${path}.traverse.direction`);
  validateEdgeTypes(traverse?.edge_types, `${path}.traverse.edge_types`);
  if (traverse) validateWeightBounds(traverse, `${path}.traverse`);
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

function validatePathEdgeTypes(query: JSONObject, path: string): void {
  for (const operation of ["shortest_path", "k_shortest_paths"] as const) {
    const body = object(query[operation]);
    validateDirection(body?.direction, `${path}.${operation}.direction`);
    validateEdgeTypes(body?.edge_types, `${path}.${operation}.edge_types`);
    if (body) validateWeightBounds(body, `${path}.${operation}`);
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
  let matchQueries = 0;
  for (const [name, candidate] of entries) {
    requireIdentifier(name, "graph_queries key");
    const query = object(candidate);
    if (query) {
      const path = `graph_queries[${JSON.stringify(name)}]`;
      if (query.match !== undefined && query.match !== null) {
        matchQueries += 1;
        if (matchQueries > MAX_GRAPH_MATCH_QUERIES) {
          throw new TypeError(
            `graph_queries accepts at most ${MAX_GRAPH_MATCH_QUERIES} match operations`
          );
        }
      }
      validateMatch(query, path);
      validateTraverse(query, path);
      validatePathEdgeTypes(query, path);
    }
  }
}
