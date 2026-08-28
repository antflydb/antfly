import { describe, expect, it } from "vitest";
import { validateGraphQueryResponses } from "../src/graph-results.js";
import type { QueryRequest, QueryResponses } from "../src/types.js";

const canonicalRequest = {
  graph_queries: {
    path: {
      index: "graph_idx",
      shortest_path: { from: { key: "a" }, to: { key: "b" }, direction: "both" },
    },
  },
} as QueryRequest;

function responses(graphResult: unknown, operation = "path"): QueryResponses {
  return {
    responses: [
      {
        status: 200,
        took: 1,
        graph_results: { [operation]: graphResult },
      },
    ],
  } as QueryResponses;
}

describe("graph result admission", () => {
  it("accepts lossless stored-edge orientation", () => {
    expect(() =>
      validateGraphQueryResponses(
        responses({
          kind: "nodes",
          nodes: [{ key: "b", depth: 1 }],
          paths: [
            {
              nodes: [{ key: "a" }, { key: "b" }],
              edges: [
                {
                  from: { key: "a" },
                  to: { key: "b" },
                  direction: "in",
                  type: "related",
                  weight: 1,
                },
              ],
              length: 1,
              objective: "min_hops",
              weight_sum: 1,
              objective_value: 1,
            },
          ],
          stats: { returned_items: 1, truncated: false },
        }),
        [canonicalRequest]
      )
    ).not.toThrow();
  });

  it("does not compute an overflowing product for a sum-independent objective", () => {
    expect(() =>
      validateGraphQueryResponses(
        responses({
          kind: "nodes",
          nodes: [{ key: "c", depth: 2 }],
          paths: [
            {
              nodes: [{ key: "a" }, { key: "b" }, { key: "c" }],
              edges: [
                {
                  from: { key: "a" },
                  to: { key: "b" },
                  direction: "out",
                  type: "related",
                  weight: 1e200,
                },
                {
                  from: { key: "b" },
                  to: { key: "c" },
                  direction: "out",
                  type: "related",
                  weight: 1e200,
                },
              ],
              length: 2,
              objective: "min_hops",
              weight_sum: 2e200,
              objective_value: 2,
            },
          ],
          stats: { returned_items: 1, truncated: false },
        }),
        [canonicalRequest]
      )
    ).not.toThrow();
  });

  it("rejects a legacy downgrade for a canonical request", () => {
    expect(() =>
      validateGraphQueryResponses(responses({ type: "neighbors", total: 1 }), [canonicalRequest])
    ).toThrow('must be "nodes" for the requested operation');
  });

  it("rejects mismatched operation names and missing edge orientation", () => {
    expect(() => validateGraphQueryResponses({ responses: [] }, [canonicalRequest])).toThrow(
      "must contain exactly one response per request"
    );

    expect(() =>
      validateGraphQueryResponses(
        responses(
          {
            kind: "nodes",
            nodes: [],
            paths: [],
            stats: { returned_items: 0, truncated: false },
          },
          "other"
        ),
        [canonicalRequest]
      )
    ).toThrow("operation names do not match the request");

    const malformed = responses({
      kind: "nodes",
      nodes: [{ key: "b", depth: 1 }],
      paths: [
        {
          nodes: [{ key: "a" }, { key: "b" }],
          edges: [{ from: { key: "a" }, to: { key: "b" }, type: "related", weight: 1 }],
          length: 1,
          objective: "min_hops",
          weight_sum: 1,
          objective_value: 1,
        },
      ],
      stats: { returned_items: 1, truncated: false },
    });
    expect(() => validateGraphQueryResponses(malformed, [canonicalRequest])).toThrow(
      'is missing required member "direction"'
    );
  });

  it("binds canonical result kinds and projections to the requested operation", () => {
    expect(() =>
      validateGraphQueryResponses(
        responses({
          kind: "aggregates",
          aggregates: { count: { value: "1", exact: true } },
          stats: { returned_items: 1, truncated: false },
        }),
        [canonicalRequest]
      )
    ).toThrow('must be "nodes" for the requested operation');

    const bindingsRequest = {
      graph_queries: {
        matched: {
          index: "graph_idx",
          match: {
            anchor: "a",
            nodes: { a: {}, b: {} },
            edges: [{ from: "a", to: "b" }],
          },
          return: { bindings: ["a", "b"] },
        },
      },
    } as QueryRequest;
    expect(() =>
      validateGraphQueryResponses(
        responses(
          {
            kind: "bindings",
            rows: [{ a: { key: "a" }, c: null }],
            stats: { returned_items: 1, truncated: false },
          },
          "matched"
        ),
        [bindingsRequest]
      )
    ).toThrow("binding aliases do not match the requested projection");

    const aggregateRequest = {
      graph_queries: {
        counted: {
          index: "graph_idx",
          match: { anchor: "a", nodes: { a: {} }, edges: [] },
          return: { aggregates: { rows: { count: "*" } } },
        },
      },
    } as QueryRequest;
    expect(() =>
      validateGraphQueryResponses(
        responses(
          {
            kind: "aggregates",
            aggregates: { other: { value: "1", exact: true } },
            stats: { returned_items: 1, truncated: false },
          },
          "counted"
        ),
        [aggregateRequest]
      )
    ).toThrow("names do not match the requested aggregates");
  });

  it("rejects unrequested documents while allowing sparse requested hydration", () => {
    const nodeResult = (document?: Record<string, unknown>) => ({
      kind: "nodes",
      nodes: [{ key: "b", depth: 1, ...(document === undefined ? {} : { document }) }],
      paths: [
        {
          nodes: [{ key: "a" }, { key: "b" }],
          edges: [
            {
              from: { key: "a" },
              to: { key: "b" },
              direction: "out",
              type: "related",
              weight: 1,
            },
          ],
          length: 1,
          objective: "min_hops",
          weight_sum: 1,
          objective_value: 1,
        },
      ],
      stats: { returned_items: 1, truncated: false },
    });
    expect(() =>
      validateGraphQueryResponses(responses(nodeResult({ private: true })), [canonicalRequest])
    ).toThrow("was returned without being requested");

    const hydratedPathRequest = {
      graph_queries: {
        path: {
          index: "graph_idx",
          shortest_path: {
            from: { key: "a" },
            to: { key: "b" },
            include_documents: true,
          },
        },
      },
    } as QueryRequest;
    expect(() =>
      validateGraphQueryResponses(responses(nodeResult({ private: true })), [hydratedPathRequest])
    ).not.toThrow();
    expect(() =>
      validateGraphQueryResponses(responses(nodeResult()), [hydratedPathRequest])
    ).not.toThrow();

    const bindingsRequest = (includeDocuments: boolean) =>
      ({
        graph_queries: {
          matched: {
            index: "graph_idx",
            match: { anchor: "a", nodes: { a: {} }, edges: [] },
            return: { bindings: ["a"], include_documents: includeDocuments },
          },
        },
      }) as QueryRequest;
    const bindingResult = {
      kind: "bindings",
      rows: [{ a: { key: "a", document: { private: true } } }],
      stats: { returned_items: 1, truncated: false },
    };
    expect(() =>
      validateGraphQueryResponses(responses(bindingResult, "matched"), [bindingsRequest(false)])
    ).toThrow("was returned without being requested");
    expect(() =>
      validateGraphQueryResponses(responses(bindingResult, "matched"), [bindingsRequest(true)])
    ).not.toThrow();
  });

  it("enforces request-derived cardinality and path ownership", () => {
    const zeroHopPath = {
      nodes: [{ key: "a" }],
      edges: [],
      length: 0,
      objective: "min_hops",
      weight_sum: 0,
      objective_value: 0,
    };
    const pathResult = (nodes: unknown[], paths: unknown[], truncated = false) => ({
      kind: "nodes",
      nodes,
      paths,
      stats: { returned_items: paths.length, truncated },
    });

    expect(() =>
      validateGraphQueryResponses(
        responses(
          pathResult(
            [
              { key: "a", depth: 0 },
              { key: "a", depth: 0 },
            ],
            [zeroHopPath, zeroHopPath]
          )
        ),
        [canonicalRequest]
      )
    ).toThrow("exceeds the requested result limit");

    expect(() =>
      validateGraphQueryResponses(responses(pathResult([], [], true)), [canonicalRequest])
    ).toThrow("must be false for an exact result");

    expect(() =>
      validateGraphQueryResponses(
        responses(pathResult([{ key: "a", depth: 0, path: [{ key: "a" }] }], [zeroHopPath])),
        [canonicalRequest]
      )
    ).toThrow("duplicates its authoritative top-level path");

    const traversalRequest = {
      graph_queries: {
        walk: { index: "graph_idx", traverse: { start: { keys: ["a"] }, limit: 1 } },
      },
    } as QueryRequest;
    expect(() =>
      validateGraphQueryResponses(
        responses(
          {
            kind: "nodes",
            nodes: [{ key: "a", depth: 0, path: [{ key: "a" }] }],
            paths: [],
            stats: { returned_items: 1, truncated: false },
          },
          "walk"
        ),
        [traversalRequest]
      )
    ).toThrow("contains a path that was not requested");

    const traversalWithPaths = {
      graph_queries: {
        walk: {
          index: "graph_idx",
          traverse: { start: { keys: ["a"] }, include_paths: true },
        },
      },
    } as QueryRequest;
    expect(() =>
      validateGraphQueryResponses(
        responses(
          {
            kind: "nodes",
            nodes: [{ key: "a", depth: 0 }],
            paths: [],
            stats: { returned_items: 1, truncated: false },
          },
          "walk"
        ),
        [traversalWithPaths]
      )
    ).toThrow("is missing its requested path");
  });
});
