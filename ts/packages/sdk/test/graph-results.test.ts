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
              weight_mode: "min_hops",
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

  it("rejects a legacy downgrade for a canonical request", () => {
    expect(() =>
      validateGraphQueryResponses(responses({ type: "neighbors", total: 1 }), [canonicalRequest])
    ).toThrow("canonical graph results require bindings, aggregates, or nodes");
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
          weight_mode: "min_hops",
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

  it("retains stateful legacy compatibility only for legacy requests", () => {
    const request = {
      graph_searches: { "legacy operation": { type: "neighbors", index_name: "graph_idx" } },
    } as QueryRequest;
    expect(() =>
      validateGraphQueryResponses(responses({ type: "neighbors", total: 1 }, "legacy operation"), [
        request,
      ])
    ).not.toThrow();
  });
});
