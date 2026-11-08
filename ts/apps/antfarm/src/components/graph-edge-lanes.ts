import type { GraphValue } from "@antfly/sdk";

type Edge = Extract<GraphValue, { type: "relationship" }>;
export const graphIdentity = (value: { table: string; key: string }) =>
  JSON.stringify([value.table, value.key]);

/** Stable encounter-order lanes shared by both graph views. */
export function graphEdgeLanes(edges: readonly Edge[]) {
  const buckets = new Map<string, number[]>();
  const lanes = edges.map((edge, index) => {
    const source = graphIdentity(edge.value.source);
    const target = graphIdentity(edge.value.target);
    const direction = source < target ? 1 : -1;
    const pair = JSON.stringify(direction === 1 ? [source, target] : [target, source]);
    let bucket = buckets.get(pair);
    if (!bucket) {
      bucket = [];
      buckets.set(pair, bucket);
    }
    const lane = bucket.length;
    bucket.push(index);
    return { source, target, direction, lane, count: 0 };
  });
  for (const bucket of buckets.values())
    for (const index of bucket) lanes[index].count = bucket.length;
  return lanes;
}
