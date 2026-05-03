import type { GraphColorConfig, GraphNode, NodeSizeConfig } from "./types";

const DEFAULT_MIN_RADIUS = 12;
const DEFAULT_MAX_RADIUS = 32;

export function buildRadiusFn(
  nodes: GraphNode[],
  config?: NodeSizeConfig
): (metric: number) => number {
  const minR = config?.minRadius ?? DEFAULT_MIN_RADIUS;
  const maxR = config?.maxRadius ?? DEFAULT_MAX_RADIUS;

  if (!nodes.length) return () => minR;

  let minS = Infinity;
  let maxS = -Infinity;
  for (const n of nodes) {
    const s = Math.sqrt((n.metric ?? 0) + 1);
    if (s < minS) minS = s;
    if (s > maxS) maxS = s;
  }

  const span = maxS - minS;
  if (span < 1e-9) {
    const mid = (minR + maxR) / 2;
    return () => mid;
  }

  return (metric: number) => {
    const s = Math.sqrt((metric ?? 0) + 1);
    const t = (s - minS) / span;
    return minR + t * (maxR - minR);
  };
}

export function resolveTypeOrder(nodes: GraphNode[]): string[] {
  const seen = new Set<string>();
  const order: string[] = [];
  for (const n of nodes) {
    if (!seen.has(n.type)) {
      seen.add(n.type);
      order.push(n.type);
    }
  }
  return order;
}

export function assignTypeColors(
  types: string[],
  colorConfig?: GraphColorConfig
): Map<string, string> {
  const map = new Map<string, string>();
  for (let i = 0; i < types.length; i++) {
    const type = types[i];
    const cfg = colorConfig?.[type];
    if (cfg?.color) {
      map.set(type, cfg.color);
    } else if (cfg?.theme) {
      map.set(type, `var(--graph-color-${type})`);
    } else {
      map.set(type, `var(--chart-${(i % 6) + 1})`);
    }
  }
  return map;
}

export const CHART_FALLBACK_COLORS = [
  "#9A94FF", // chart-1 purple (primary)
  "#8B8B96", // chart-2 cool gray (muted-foreground)
  "#C9A834", // chart-3 amber
  "#477F4F", // chart-4 green
  "#F7978D", // chart-5 coral
  "#4A82B0", // chart-6 blue
] as const;

export function getFallbackColor(typeIndex: number): string {
  return CHART_FALLBACK_COLORS[typeIndex % CHART_FALLBACK_COLORS.length];
}

export function clampEdgeWidth(weight?: number): number {
  return Math.min(5, 1 + (weight ?? 1) * 0.5);
}
