/**
 * Pure layout computation for the DFA pipeline graph.
 *
 * ≤3 steps: single horizontal row (L→R)
 * 4-5 steps: two-row U-shape (row 1 L→R, row 2 R→L)
 */

export const NODE_WIDTH = 160;
export const NODE_HEIGHT = 56;
export const GAP_X = 30;
export const GAP_Y = 44;

export interface NodeLayout {
  index: number;
  row: number;
  col: number;
  x: number;
  y: number;
}

export interface EdgeLayout {
  from: number;
  to: number;
  /** SVG path d attribute (quadratic bezier) */
  d: string;
  /** Total path length hint for animations */
  pathId: string;
}

export interface GraphLayout {
  nodes: NodeLayout[];
  edges: EdgeLayout[];
  width: number;
  height: number;
}

export function computeLayout(stepCount: number): GraphLayout {
  if (stepCount === 0) {
    return { nodes: [], edges: [], width: 0, height: 0 };
  }

  const nodes: NodeLayout[] = [];

  if (stepCount <= 3) {
    // Single horizontal row
    for (let i = 0; i < stepCount; i++) {
      nodes.push({
        index: i,
        row: 0,
        col: i,
        x: i * (NODE_WIDTH + GAP_X),
        y: 0,
      });
    }
  } else {
    // Two-row U-shape: first row L→R, second row R→L
    const topCount = Math.ceil(stepCount / 2);
    const bottomCount = stepCount - topCount;

    // Top row: left to right
    for (let i = 0; i < topCount; i++) {
      nodes.push({
        index: i,
        row: 0,
        col: i,
        x: i * (NODE_WIDTH + GAP_X),
        y: 0,
      });
    }

    // Bottom row: right to left (placed under the right end of top row)
    for (let i = 0; i < bottomCount; i++) {
      const col = topCount - 1 - i;
      nodes.push({
        index: topCount + i,
        row: 1,
        col,
        x: col * (NODE_WIDTH + GAP_X),
        y: NODE_HEIGHT + GAP_Y,
      });
    }
  }

  // Compute edges between consecutive nodes
  const edges: EdgeLayout[] = [];
  for (let i = 0; i < nodes.length - 1; i++) {
    const from = nodes[i];
    const to = nodes[i + 1];
    edges.push({
      from: i,
      to: i + 1,
      d: computeEdgePath(from, to),
      pathId: `edge-${i}-${i + 1}`,
    });
  }

  // Compute bounding box
  const maxX = Math.max(...nodes.map((n) => n.x + NODE_WIDTH));
  const maxY = Math.max(...nodes.map((n) => n.y + NODE_HEIGHT));

  return { nodes, edges, width: maxX, height: maxY };
}

function computeEdgePath(from: NodeLayout, to: NodeLayout): string {
  const fromCx = from.x + NODE_WIDTH / 2;
  const fromCy = from.y + NODE_HEIGHT / 2;
  const toCx = to.x + NODE_WIDTH / 2;
  const toCy = to.y + NODE_HEIGHT / 2;

  // Same row: horizontal connection
  if (from.row === to.row) {
    const goingRight = to.x > from.x;
    const startX = goingRight ? from.x + NODE_WIDTH : from.x;
    const endX = goingRight ? to.x : to.x + NODE_WIDTH;
    const startY = fromCy;
    const endY = toCy;
    // Straight horizontal line with slight curve
    const midX = (startX + endX) / 2;
    return `M ${startX} ${startY} Q ${midX} ${startY} ${endX} ${endY}`;
  }

  // Different rows: vertical drop from right end of top row to right end of bottom row
  // From is on top row, To is on bottom row (U-shape turn)
  const startX = fromCx;
  const startY = from.y + NODE_HEIGHT;
  const endX = toCx;
  const endY = to.y;

  // Use a smooth curve that drops down and across
  const midY = (startY + endY) / 2;
  return `M ${startX} ${startY} C ${startX} ${midY} ${endX} ${midY} ${endX} ${endY}`;
}
