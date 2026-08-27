// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { describe, expect, it } from "vitest";
import {
  GRAPH_IDENTIFIER_CONFORMANCE_CASES,
  isValidGraphIdentifier,
} from "../src/graph-identifier-policy.generated.js";
import {
  countGraphAlias,
  countGraphRows,
  graphDateRangeFilter,
  graphNumericRangeFilter,
  graphTermRangeFilter,
  validateGraphQueryIdentifiers,
} from "../src/graph-identifiers.js";

describe("graph identifier policy", () => {
  for (const testCase of GRAPH_IDENTIFIER_CONFORMANCE_CASES) {
    it(testCase.name, () => {
      expect(isValidGraphIdentifier(testCase.value)).toBe(testCase.valid);
    });
  }

  it("rejects unsafe aliases before sending a query", () => {
    expect(() =>
      validateGraphQueryIdentifiers({
        people: {
          match: {
            anchor: "person",
            nodes: { person: {}, "post\u200bauthor": {} },
            edges: [{ from: "person", to: "post\u200bauthor" }],
          },
          return: { bindings: ["person"] },
        },
      })
    ).toThrow("match.nodes key");
  });

  it("matches the server predicate-depth limit", () => {
    let where: Record<string, unknown> = {
      not_equal: { left: { alias: "person" }, right: { alias: "author" } },
    };
    for (let depth = 0; depth < 16; depth += 1) where = { and: [where] };

    expect(() =>
      validateGraphQueryIdentifiers({
        people: {
          match: {
            anchor: "person",
            nodes: { person: {}, author: {} },
            edges: [{ from: "person", to: "author" }],
            where,
          },
          return: { bindings: ["person"] },
        },
      })
    ).toThrow("maximum graph predicate depth");
  });

  it.each([
    [{ result_ref: "$graph_results.bad\u200bname" }, "result_ref query name"],
    [{ result_ref: "$graph_results.people", binding: "bad\u200bname" }, "traverse.start.binding"],
    [{ result_ref: "$query_results", binding: "person" }, "binding requires a $graph_results"],
  ])("rejects unsafe graph result selectors", (start, message) => {
    expect(() =>
      validateGraphQueryIdentifiers({
        walk: { index: "social", traverse: { start } },
      })
    ).toThrow(message);
  });

  it("rejects more than the server maximum named operations", () => {
    const graphQueries = Object.fromEntries(
      Array.from({ length: 65 }, (_, index) => [`query_${index}`, {}])
    );
    expect(() => validateGraphQueryIdentifiers(graphQueries)).toThrow(
      "at most 64 named operations"
    );
  });

  it.each([
    [["bad\ud800"], "valid UTF-8"],
    [["links", "links"], "duplicate edge types"],
    [["文".repeat(Math.floor(65_536 / 3) + 1)], "at most 65536 UTF-8 bytes"],
  ])("rejects edge types outside the durable wire policy", (types, message) => {
    expect(() =>
      validateGraphQueryIdentifiers({
        people: {
          match: {
            anchor: "person",
            nodes: { person: {}, author: {} },
            edges: [{ from: "person", to: "author", types }],
          },
          return: { bindings: ["person"] },
        },
      })
    ).toThrow(message);
  });

  it("validates graph match edge direction", () => {
    expect(() =>
      validateGraphQueryIdentifiers({
        people: {
          match: {
            anchor: "person",
            nodes: { person: {}, author: {} },
            edges: [{ from: "person", to: "author", direction: "both" }],
          },
          return: { bindings: ["person"] },
        },
      })
    ).not.toThrow();
    expect(() =>
      validateGraphQueryIdentifiers({
        people: {
          match: {
            anchor: "person",
            nodes: { person: {}, author: {} },
            edges: [{ from: "person", to: "author", direction: "sideways" }],
          },
          return: { bindings: ["person"] },
        },
      } as never)
    ).toThrow("direction must be out, in, or both");
  });

  it.each([false, true])("rejects distinct presence on count(*) (%s)", (distinct) => {
    expect(() =>
      validateGraphQueryIdentifiers({
        people: {
          match: { anchor: "person", nodes: { person: {} }, edges: [] },
          return: { aggregates: { rows: { count: "*", distinct } } },
        },
      })
    ).toThrow("distinct is only valid for alias counts");
  });

  it("constructs structurally valid graph counts", () => {
    expect(countGraphRows()).toEqual({ count: "*" });
    expect(countGraphAlias("person")).toEqual({ count: "person" });
    expect(countGraphAlias("person", true)).toEqual({ count: "person", distinct: true });
    expect(() => countGraphAlias("*")).toThrow("graph count alias");
  });

  it("constructs operation-keyed graph range filters", () => {
    expect(graphNumericRangeFilter("/score", { min: 0, inclusiveMin: true })).toEqual({
      numeric_range: { path: "/score", min: 0, inclusive_min: true },
    });
    expect(graphTermRangeFilter("/status", { max: "z" })).toEqual({
      term_range: { path: "/status", max: "z" },
    });
    expect(graphDateRangeFilter("/created_at", { start: "2026-01-01T00:00:00Z" })).toEqual({
      date_range: { path: "/created_at", start: "2026-01-01T00:00:00Z" },
    });
    expect(
      graphDateRangeFilter("/created_at", { end: "2026-01-01t00:00:00z", inclusiveEnd: true })
    ).toEqual({
      date_range: {
        path: "/created_at",
        end: "2026-01-01t00:00:00z",
        inclusive_end: true,
      },
    });
    expect(() => graphNumericRangeFilter("/score", {})).toThrow("requires min or max");
    expect(() => graphTermRangeFilter("score", { min: "a" })).toThrow("RFC 6901");
    expect(() => graphDateRangeFilter("/created~2at", { end: "2026-01-01T00:00:00Z" })).toThrow(
      "RFC 6901"
    );
    expect(() => graphDateRangeFilter("/created_at", { start: "2026-02-29T00:00:00Z" })).toThrow(
      "valid RFC 3339"
    );
    expect(() => graphDateRangeFilter("/created_at", { start: "2026-01-01T00:00:00" })).toThrow(
      "with a UTC offset"
    );
    expect(() =>
      graphNumericRangeFilter("/score", { min: 1, path: "not-a-pointer" } as never)
    ).toThrow("unsupported property");
    expect(() =>
      graphNumericRangeFilter("/score", { min: 1, inclusive_min: false } as never)
    ).toThrow("unsupported property");
    expect(() => graphTermRangeFilter("/status", { min: 1 } as never)).toThrow(
      "bounds must be strings"
    );
    expect(() => graphDateRangeFilter("/created_at", { start: 1 } as never)).toThrow("RFC 3339");
    expect(() =>
      graphDateRangeFilter("/created_at", { start: "1969-12-31T23:59:59.999999999Z" })
    ).toThrow("supported Unix-nanosecond range");
    expect(() =>
      graphDateRangeFilter("/created_at", { start: "2554-07-21T23:34:33.709551615Z" })
    ).not.toThrow();
    expect(() =>
      graphDateRangeFilter("/created_at", { start: "2554-07-21T23:34:33.709551616Z" })
    ).toThrow("supported Unix-nanosecond range");
  });
});
