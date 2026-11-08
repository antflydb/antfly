import { Button } from "@antfly/design-system";
import type { GraphValue, SQLResponse } from "@antfly/sdk";
import { useId, useMemo, useState } from "react";
import { collectGraph } from "./GraphQueryWorkbench";
import { graphEdgeLanes, graphIdentity as key } from "./graph-edge-lanes";

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function identity(value: unknown): boolean {
  return object(value) && typeof value.table === "string" && typeof value.key === "string";
}
function graphValue(value: unknown, depth = 0): value is GraphValue {
  if (depth > 64 || !object(value)) return false;
  const body = value.value;
  switch (value.type) {
    case "node":
      return (
        object(body) &&
        identity(body.identity) &&
        Array.isArray(body.labels) &&
        body.labels.every((label) => typeof label === "string") &&
        object(body.properties) &&
        body.properties.type === "map" &&
        object(body.properties.value)
      );
    case "relationship":
      return (
        object(body) &&
        identity(body.identity) &&
        identity(body.source) &&
        identity(body.target) &&
        typeof body.index === "string" &&
        typeof body.type === "string" &&
        ["explicit", "document", "field", "artifact"].includes(String(body.ownership)) &&
        object(body.properties) &&
        body.properties.type === "map" &&
        object(body.properties.value)
      );
    case "path":
      return (
        object(body) &&
        Array.isArray(body.nodes) &&
        Array.isArray(body.relationships) &&
        body.nodes.every((node) => graphValue(node, depth + 1) && node.type === "node") &&
        body.relationships.every(
          (edge) => graphValue(edge, depth + 1) && edge.type === "relationship"
        )
      );
    case "list":
      return Array.isArray(body) && body.every((item) => graphValue(item, depth + 1));
    case "map":
      return object(body) && Object.values(body).every((item) => graphValue(item, depth + 1));
    case "null":
      return body === null;
    case "boolean":
      return typeof body === "boolean";
    case "string":
      return typeof body === "string";
    case "integer":
      return typeof body === "string" && /^-?\d+$/.test(body);
    case "number":
      return typeof body === "number" && Number.isFinite(body);
    default:
      return false;
  }
}
function hasEntity(value: GraphValue): boolean {
  if (["node", "relationship", "path"].includes(value.type)) return true;
  if (value.type === "list") return value.value.some(hasEntity);
  if (value.type === "map") return Object.values(value.value).some(hasEntity);
  return false;
}
export function sqlGraphValues(result: SQLResponse): GraphValue[] {
  const values: GraphValue[] = [];
  function collect(value: unknown, depth = 0) {
    if (depth > 64) return;
    if (graphValue(value)) {
      if (hasEntity(value)) values.push(value);
      return;
    }
    // SQL GRAPH_TABLE lowers data lists/maps to ordinary JSON while retaining
    // envelopes around nested entities. Visit those ordinary containers too.
    if (Array.isArray(value)) for (const item of value) collect(item, depth + 1);
    else if (object(value)) for (const item of Object.values(value)) collect(item, depth + 1);
  }
  for (const [ri, row] of result.rows.entries())
    for (const [ci, cell] of row.entries()) {
      if (result.sql_nulls?.[ri]?.[ci]) continue;
      let value: unknown = cell;
      if (typeof value === "string" && ["json", "jsonb"].includes(result.columns[ci]?.type ?? "")) {
        try {
          value = JSON.parse(value);
        } catch {
          continue;
        }
      }
      collect(value);
    }
  return values;
}

/** Displays only values already returned by SQL. No implicit document fetches. */
export default function SQLGraphPreview({ values }: { values: GraphValue[] }) {
  const visual = useMemo(() => collectGraph(values), [values]);
  const edgeLanes = useMemo(() => graphEdgeLanes(visual.edges), [visual.edges]);
  const arrow = useId();
  const [selected, setSelected] = useState<GraphValue>();
  const [zoom, setZoom] = useState(1);
  const positions = useMemo(
    () =>
      new Map(
        visual.nodes.map(([id], index) => {
          const angle = (2 * Math.PI * index) / Math.max(1, visual.nodes.length);
          return [id, { x: 400 + 280 * Math.cos(angle), y: 210 + 145 * Math.sin(angle) }];
        })
      ),
    [visual.nodes]
  );
  function select(value: GraphValue) {
    setSelected(value);
  }
  return (
    <div className="space-y-3">
      <p>
        {visual.nodes.length} displayed nodes · {visual.edges.length} displayed relationships
      </p>
      {visual.clipped && (
        <p>
          The visualization shows up to 200 nodes and 400 relationships. SQL rows are unchanged.
        </p>
      )}
      <label className="flex items-center gap-2">
        Zoom
        <input
          aria-label="SQL graph zoom"
          type="range"
          min="0.5"
          max="4"
          step="0.1"
          value={zoom}
          onChange={(event) => setZoom(Number(event.target.value))}
        />
      </label>
      <Button variant="outline" onClick={() => setZoom(1)}>
        Reset graph view
      </Button>
      <svg
        role="img"
        aria-label="SQL result graph"
        className="w-full rounded border"
        viewBox={`${400 - 400 / zoom} ${210 - 210 / zoom} ${800 / zoom} ${420 / zoom}`}
      >
        <defs>
          <marker id={arrow} markerWidth="8" markerHeight="8" refX="18" refY="4" orient="auto">
            <path d="M0,0 L8,4 L0,8" fill="currentColor" />
          </marker>
        </defs>
        {visual.edges.map((edge, edgeIndex) => {
          const laneInfo = edgeLanes[edgeIndex];
          const source = positions.get(laneInfo.source),
            target = positions.get(laneInfo.target);
          if (!source || !target) return null;
          const { lane, count, direction } = laneInfo;
          const offset = (lane - (count - 1) / 2) * 28 * direction;
          const radius = 48 + lane * 18,
            dx = target.x - source.x,
            dy = target.y - source.y,
            length = Math.hypot(dx, dy) || 1;
          const path =
            laneInfo.source === laneInfo.target
              ? `M ${source.x + 10} ${source.y - 14} C ${source.x + radius} ${source.y - radius - 20}, ${source.x - radius} ${source.y - radius - 20}, ${source.x - 10} ${source.y - 14}`
              : `M ${source.x} ${source.y} Q ${(source.x + target.x) / 2 - (dy / length) * offset} ${(source.y + target.y) / 2 + (dx / length) * offset} ${target.x} ${target.y}`;
          return (
            <g
              key={JSON.stringify([edge.value.identity, edge.value.index])}
              role="button"
              tabIndex={0}
              aria-label={`Inspect SQL relationship ${edge.value.identity.key}`}
              onClick={() => select(edge)}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  select(edge);
                }
              }}
            >
              <path
                d={path}
                fill="none"
                stroke="currentColor"
                strokeWidth="3"
                markerEnd={`url(#${arrow})`}
              />
              <path d={path} fill="none" stroke="transparent" strokeWidth="14" />
              <title>{edge.value.type}</title>
            </g>
          );
        })}
        {visual.nodes.map(([id, node]) => {
          const point = positions.get(id);
          if (!point) return null;
          return (
            <g
              key={id}
              role="button"
              tabIndex={0}
              aria-label={`Inspect SQL node ${node.value.identity.key}`}
              onClick={() => select(node)}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  select(node);
                }
              }}
            >
              <circle
                cx={point.x}
                cy={point.y}
                r="16"
                fill={
                  selected?.type === "node" && key(selected.value.identity) === id
                    ? "#eab308"
                    : "#3b82f6"
                }
              />
              <text
                x={point.x}
                y={point.y + 31}
                textAnchor="middle"
                fill="currentColor"
                fontSize="12"
              >
                {node.value.identity.key.slice(0, 24)}
              </text>
              <title>{node.value.labels.join(", ")}</title>
            </g>
          );
        })}
      </svg>
      {selected && (
        <pre
          role="region"
          aria-label="SQL graph entity"
          className="max-h-80 overflow-auto rounded border p-3 text-xs"
        >
          {JSON.stringify(selected, null, 2)}
        </pre>
      )}
    </div>
  );
}
