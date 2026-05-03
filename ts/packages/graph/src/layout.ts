import {
  forceCenter,
  forceCollide,
  forceLink,
  forceManyBody,
  forceSimulation,
  type SimulationLinkDatum,
  type SimulationNodeDatum,
} from "d3-force";
import type { ForceLayoutOptions, GraphData } from "./types";

interface SimNode extends SimulationNodeDatum {
  id: string;
}

interface SimLink extends SimulationLinkDatum<SimNode> {
  weight: number;
}

export function computeForceLayout(
  data: GraphData,
  width: number,
  height: number,
  options?: ForceLayoutOptions
): Map<string, { x: number; y: number }> {
  const ticks = options?.ticks ?? 300;
  const linkDistance = options?.linkDistance ?? 100;
  const linkStrength = options?.linkStrength ?? 0.3;
  const chargeStrength = options?.chargeStrength ?? -200;
  const collideRadius = options?.collideRadius ?? 30;

  const simNodes: SimNode[] = data.nodes.map((n) => ({ id: n.id }));
  const nodeSet = new Set(simNodes.map((n) => n.id));

  const simLinks: SimLink[] = data.edges
    .filter((e) => nodeSet.has(e.source) && nodeSet.has(e.target))
    .map((e) => ({ source: e.source, target: e.target, weight: e.weight ?? 1 }));

  const sim = forceSimulation<SimNode>(simNodes)
    .force(
      "link",
      forceLink<SimNode, SimLink>(simLinks)
        .id((d) => d.id)
        .distance(linkDistance)
        .strength((d) => Math.min(1, d.weight * linkStrength))
    )
    .force("charge", forceManyBody().strength(chargeStrength))
    .force("center", forceCenter(width / 2, height / 2))
    .force("collide", forceCollide(collideRadius))
    .stop();

  for (let i = 0; i < ticks; i++) sim.tick();

  const positions = new Map<string, { x: number; y: number }>();
  for (const sn of simNodes) {
    positions.set(sn.id, { x: sn.x ?? 0, y: sn.y ?? 0 });
  }
  return positions;
}
