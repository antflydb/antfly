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
          kind: "paths",
          paths: [
            {
              path: {
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
            },
          ],
          stats: { returned_items: 1 },
        }),
        [canonicalRequest]
      )
    ).not.toThrow();
  });

  it("does not compute an overflowing product for a sum-independent objective", () => {
    expect(() =>
      validateGraphQueryResponses(
        responses({
          kind: "paths",
          paths: [
            {
              path: {
                nodes: [{ key: "a" }, { key: "x" }, { key: "b" }],
                edges: [
                  {
                    from: { key: "a" },
                    to: { key: "x" },
                    direction: "out",
                    type: "related",
                    weight: 1e200,
                  },
                  {
                    from: { key: "x" },
                    to: { key: "b" },
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
            },
          ],
          stats: { returned_items: 1 },
        }),
        [canonicalRequest]
      )
    ).not.toThrow();
  });

  it("rejects a legacy downgrade for a canonical request", () => {
    expect(() =>
      validateGraphQueryResponses(responses({ type: "neighbors", total: 1 }), [canonicalRequest])
    ).toThrow('must be "paths" for the requested operation');
  });

  it("rejects mismatched operation names and missing edge orientation", () => {
    expect(() => validateGraphQueryResponses({ responses: [] }, [canonicalRequest])).toThrow(
      "must contain exactly one response per request"
    );

    expect(() =>
      validateGraphQueryResponses(
        responses(
          {
            kind: "paths",
            paths: [],
            stats: { returned_items: 0 },
          },
          "other"
        ),
        [canonicalRequest]
      )
    ).toThrow("operation names do not match the request");

    const malformed = responses({
      kind: "paths",
      paths: [
        {
          path: {
            nodes: [{ key: "a" }, { key: "b" }],
            edges: [{ from: { key: "a" }, to: { key: "b" }, type: "related", weight: 1 }],
            length: 1,
            objective: "min_hops",
            weight_sum: 1,
            objective_value: 1,
          },
        },
      ],
      stats: { returned_items: 1 },
    });
    expect(() => validateGraphQueryResponses(malformed, [canonicalRequest])).toThrow(
      'is missing required member "direction"'
    );
  });

  it.each([" ", "\t\r\n"])("rejects ASCII-whitespace table qualifiers", (table) => {
    const request = {
      graph_queries: {
        walk: { index: "graph", traverse: { start: { result_ref: "$query_results" } } },
      },
    };
    expect(() =>
      validateGraphQueryResponses(
        responses(
          {
            kind: "nodes",
            nodes: [{ key: "a", table, depth: 0 }],
            stats: { returned_items: 1, truncated: false },
          },
          "walk"
        ),
        [request]
      )
    ).toThrow("table: must contain a non-whitespace character");
  });

  it("binds canonical result kinds and projections to the requested operation", () => {
    expect(() =>
      validateGraphQueryResponses(
        responses({
          kind: "aggregates",
          aggregates: { count: { value: "1", exact: true } },
          stats: { returned_items: 1 },
        }),
        [canonicalRequest]
      )
    ).toThrow('must be "paths" for the requested operation');

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
            stats: { returned_items: 1 },
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
            stats: { returned_items: 1 },
          },
          "counted"
        ),
        [aggregateRequest]
      )
    ).toThrow("names do not match the requested aggregates");
  });

  it("rejects unrequested documents while allowing sparse requested hydration", () => {
    const nodeResult = (document?: Record<string, unknown>) => ({
      kind: "paths",
      paths: [
        {
          ...(document === undefined ? {} : { document }),
          path: {
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
        },
      ],
      stats: { returned_items: 1 },
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
    const pathResult = (paths: unknown[], includeInvalidTruncated = false) => ({
      kind: "paths",
      paths: paths.map((path) => ({ path })),
      stats: includeInvalidTruncated
        ? { returned_items: paths.length, truncated: true }
        : { returned_items: paths.length },
    });

    expect(() =>
      validateGraphQueryResponses(responses(pathResult([zeroHopPath, zeroHopPath])), [
        canonicalRequest,
      ])
    ).toThrow("exceeds the requested result limit");

    expect(() =>
      validateGraphQueryResponses(responses(pathResult([], true)), [canonicalRequest])
    ).toThrow('contains unknown member "truncated"');

    expect(() =>
      validateGraphQueryResponses(
        responses({
          ...pathResult([zeroHopPath]),
          paths: [{ path: zeroHopPath, unexpected: true }],
        }),
        [canonicalRequest]
      )
    ).toThrow("contains unknown member");

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
            stats: { returned_items: 1, truncated: false },
          },
          "walk"
        ),
        [traversalWithPaths]
      )
    ).toThrow("is missing its requested path");
  });

  it("enforces observable path and traversal semantics from the request", () => {
    const constrained = {
      graph_queries: {
        path: {
          index: "graph_idx",
          shortest_path: {
            from: { key: "a", table: "docs" },
            to: { key: "b", table: "entities" },
            direction: "in",
            edge_types: ["cites"],
            edge_weight: { max: 0.5 },
            max_depth: 1,
            objective: "min_weight_sum",
          },
        },
      },
    } as QueryRequest;
    const validPath = {
      nodes: [{ key: "a" }, { key: "b", table: "entities" }],
      edges: [
        {
          from: { key: "a" },
          to: { key: "b", table: "entities" },
          direction: "in",
          type: "cites",
          weight: 0.5,
        },
      ],
      length: 1,
      objective: "min_weight_sum",
      weight_sum: 0.5,
      objective_value: 0.5,
    };
    const result = (path: unknown) => ({
      kind: "paths",
      paths: [{ path }],
      stats: { returned_items: 1 },
    });

    expect(() =>
      validateGraphQueryResponses(responses(result(validPath)), [constrained], "docs")
    ).not.toThrow();
    expect(() =>
      validateGraphQueryResponses(
        responses(result(validPath)),
        [{ ...constrained, table: "other" }],
        "docs"
      )
    ).not.toThrow();
    expect(() =>
      validateGraphQueryResponses(
        responses(
          result({
            ...validPath,
            nodes: [{ key: "a" }, { key: "wrong" }],
            edges: [{ ...validPath.edges[0], to: { key: "wrong" } }],
          })
        ),
        [constrained],
        "docs"
      )
    ).toThrow("terminal endpoint");
    expect(() =>
      validateGraphQueryResponses(
        responses(
          result({
            ...validPath,
            edges: [{ ...validPath.edges[0], type: "links" }],
          })
        ),
        [constrained],
        "docs"
      )
    ).toThrow("was not requested");
    const unfiltered = {
      graph_queries: {
        path: {
          ...constrained.graph_queries.path,
          shortest_path: {
            ...constrained.graph_queries.path.shortest_path,
            edge_types: [],
          },
        },
      },
    } as QueryRequest;
    expect(() =>
      validateGraphQueryResponses(
        responses(
          result({
            ...validPath,
            edges: [{ ...validPath.edges[0], type: "links" }],
          })
        ),
        [unfiltered],
        "docs"
      )
    ).not.toThrow();

    const traversal = {
      graph_queries: {
        walk: {
          index: "graph_idx",
          traverse: { start: { keys: ["a"] }, max_depth: 1 },
        },
      },
    } as QueryRequest;
    expect(() =>
      validateGraphQueryResponses(
        responses(
          {
            kind: "nodes",
            nodes: [{ key: "wrong", depth: 0 }],
            stats: { returned_items: 1, truncated: false },
          },
          "walk"
        ),
        [traversal]
      )
    ).toThrow("requested traversal identity");

    const direct = {
      nodes: [{ key: "a" }, { key: "b" }],
      edges: [
        {
          from: { key: "a" },
          to: { key: "b" },
          direction: "out",
          type: "links",
          weight: 1,
        },
      ],
      length: 1,
      objective: "min_hops",
      weight_sum: 1,
      objective_value: 1,
    };
    const kRequest = {
      graph_queries: {
        path: {
          index: "graph_idx",
          k_shortest_paths: { from: { key: "a" }, to: { key: "b" }, k: 2 },
        },
      },
    } as QueryRequest;
    expect(() =>
      validateGraphQueryResponses(
        responses({
          kind: "paths",
          paths: [{ path: direct }, { path: direct }],
          stats: { returned_items: 2 },
        }),
        [kRequest]
      )
    ).toThrow("duplicates an earlier path");
  });
});
