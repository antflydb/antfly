import type { GraphValue } from "@antfly/sdk";
import { expect, it } from "vitest";
import { graphEdgeLanes } from "./graph-edge-lanes";

type Edge = Extract<GraphValue, { type: "relationship" }>;
function edge(source: string, target: string): Edge {
  return {
    type: "relationship",
    value: {
      identity: { table: "docs", key: `${source}:${target}` },
      index: "links",
      type: "R",
      ownership: "explicit",
      source: { table: "docs", key: source },
      target: { table: "docs", key: target },
      properties: { type: "map", value: {} },
    },
  };
}
it("assigns encounter-order lanes across reverse edges, loops and unusual identities", () => {
  const edges = [
    edge("a", "b"),
    edge("b", "a"),
    edge("a", "a"),
    edge("a", "b"),
    edge("a", "a"),
    edge("a|b", 'c"'),
  ];
  const lanes = graphEdgeLanes(edges);
  expect(lanes.map(({ lane, count }) => [lane, count])).toEqual([
    [0, 3],
    [1, 3],
    [0, 2],
    [2, 3],
    [1, 2],
    [0, 1],
  ]);
  expect(lanes[0].direction).toBe(-lanes[1].direction);
  expect(graphEdgeLanes([])).toEqual([]);
});
