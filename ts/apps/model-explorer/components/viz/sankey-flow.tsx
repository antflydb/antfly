"use client";

import { cn } from "@antfly/design-system";
import { sankey, sankeyLinkHorizontal } from "d3-sankey";
import { useId, useMemo, useState } from "react";
import { useReducedMotion } from "@/lib/use-reduced-motion";
import type { SankeySpec } from "@/lib/schema";

interface SankeyNodeDatum {
  id: string;
  label: string;
  colorVar?: string;
  value?: number;
  x0?: number;
  x1?: number;
  y0?: number;
  y1?: number;
}
interface SankeyLinkDatum {
  source: SankeyNodeDatum | string | number;
  target: SankeyNodeDatum | string | number;
  value: number;
  label?: string;
  width?: number;
  y0?: number;
  y1?: number;
}

const WIDTH = 760;
const PAD = 10;
/** Room for the widest end-column label, which is drawn outside the node. */
const LABEL_GUTTER = 108;

const linkKey = (l: { source: SankeyNodeDatum; target: SankeyNodeDatum; label?: string }) =>
  `${l.source.id}:${l.target.id}:${l.label ?? ""}`;

/**
 * Forward-pass flow: d3-sankey computes the layout, React owns the SVG.
 * Ribbon width is proportional to the spec's own `value` (the caller states
 * what that measures); ribbons take a gradient from source colour to target
 * colour so a lane stays followable across a column.
 *
 * Focusing a node isolates the paths *through* it — the upstream cone and the
 * downstream cone — rather than only its immediate neighbours, so a single
 * lane can be traced end to end.
 */
export function SankeyFlow({
  spec,
  highlight,
  unit,
  className,
}: {
  spec: SankeySpec;
  /** Node ids to emphasize; everything else dims. */
  highlight?: string[];
  /** Appended to node totals, e.g. "MB/token". Omitted when the value is unitless. */
  unit?: string;
  className?: string;
}) {
  const uid = useId();
  const descriptionId = `${uid}-desc`;
  const [focus, setFocus] = useState<string | null>(null);
  const reducedMotion = useReducedMotion();

  const height = Math.max(240, spec.nodes.length * 52);

  const { nodes, links } = useMemo(() => {
    const layout = sankey<SankeyNodeDatum, SankeyLinkDatum>()
      .nodeId((d) => d.id)
      .nodeWidth(13)
      .nodePadding(20)
      .extent([
        [PAD, PAD],
        [WIDTH - PAD, height - PAD],
      ]);
    return layout({
      nodes: spec.nodes.map((n) => ({ ...n })),
      links: spec.links.map((l) => ({
        source: l.source,
        target: l.target,
        value: l.value,
        label: l.label,
      })),
    });
  }, [spec, height]);

  /** Ancestor and descendant cones of the focused node, focus included in both. */
  const cones = useMemo(() => {
    if (!focus) return null;
    const fwd = new Map<string, string[]>();
    const rev = new Map<string, string[]>();
    for (const l of spec.links) {
      fwd.set(l.source, [...(fwd.get(l.source) ?? []), l.target]);
      rev.set(l.target, [...(rev.get(l.target) ?? []), l.source]);
    }
    const walk = (edges: Map<string, string[]>) => {
      const seen = new Set([focus]);
      const stack = [focus];
      while (stack.length) {
        const id = stack.pop();
        if (id === undefined) break;
        for (const next of edges.get(id) ?? []) {
          if (!seen.has(next)) {
            seen.add(next);
            stack.push(next);
          }
        }
      }
      return seen;
    };
    return { down: walk(fwd), up: walk(rev) };
  }, [focus, spec.links]);

  const nodeLit = (id: string) => {
    if (highlight && !highlight.includes(id)) return false;
    if (!cones) return true;
    return cones.up.has(id) || cones.down.has(id);
  };
  /** A link is on a focused path only if both ends sit in the same cone. */
  const linkLit = (sourceId: string, targetId: string) => {
    if (highlight && !highlight.includes(sourceId) && !highlight.includes(targetId)) return false;
    if (!cones) return true;
    return (
      (cones.up.has(sourceId) && cones.up.has(targetId)) ||
      (cones.down.has(sourceId) && cones.down.has(targetId))
    );
  };

  const path = sankeyLinkHorizontal();
  const fmt = (n: number) => n.toLocaleString("en-US");

  return (
    <svg
      viewBox={`0 0 ${WIDTH} ${height}`}
      className={cn("w-full", className)}
      role="group"
      aria-label="Forward-pass flow"
      aria-describedby={descriptionId}
    >
      <title>Forward-pass flow</title>
      <desc id={descriptionId}>
        Focus a stage to isolate the paths running through it.{" "}
        {spec.links
          .map(
            (link) =>
              `${spec.nodes.find((n) => n.id === link.source)?.label ?? link.source} to ${
                spec.nodes.find((n) => n.id === link.target)?.label ?? link.target
              }${link.label ? `: ${link.label}` : ""}`
          )
          .join(". ")}
      </desc>
      <defs>
        {links.map((link) => {
          const source = link.source as SankeyNodeDatum;
          const target = link.target as SankeyNodeDatum;
          return (
            <linearGradient
              key={linkKey({ source, target, label: link.label })}
              id={`${uid}-${linkKey({ source, target, label: link.label })}`.replace(
                /[^\w-]/g,
                "_"
              )}
              gradientUnits="userSpaceOnUse"
              x1={source.x1 ?? 0}
              x2={target.x0 ?? 0}
            >
              <stop offset="0%" stopColor={source.colorVar ?? "var(--muted-foreground)"} />
              <stop offset="100%" stopColor={target.colorVar ?? "var(--muted-foreground)"} />
            </linearGradient>
          );
        })}
      </defs>

      {links.map((link) => {
        const source = link.source as SankeyNodeDatum;
        const target = link.target as SankeyNodeDatum;
        const key = linkKey({ source, target, label: link.label });
        const lit = linkLit(source.id, target.id);
        const d = path(link) ?? undefined;
        const width = Math.max(1.5, link.width ?? 1);
        return (
          <g key={key}>
            <path
              d={d}
              fill="none"
              stroke={`url(#${`${uid}-${key}`.replace(/[^\w-]/g, "_")})`}
              strokeWidth={width}
              strokeOpacity={lit ? 0.45 : 0.07}
              className="transition-[stroke-opacity] duration-200"
            >
              <title>{`${source.label} → ${target.label}${link.label ? ` · ${link.label}` : ""}`}</title>
            </path>
            {/* Travelling dashes read as direction of flow; motion is opt-out. */}
            {lit && !reducedMotion && (
              <path
                d={d}
                fill="none"
                stroke={target.colorVar ?? "var(--muted-foreground)"}
                strokeWidth={Math.min(2, width * 0.5)}
                strokeOpacity={0.8}
                strokeDasharray="3 30"
                className="sankey-flow-dash"
                pointerEvents="none"
              />
            )}
          </g>
        );
      })}

      {nodes.map((node) => {
        const lit = nodeLit(node.id);
        const x0 = node.x0 ?? 0;
        const x1 = node.x1 ?? 0;
        const y0 = node.y0 ?? 0;
        const y1 = node.y1 ?? 0;
        const leftSide = x0 < WIDTH - LABEL_GUTTER;
        const selected = focus === node.id;
        return (
          <g
            key={node.id}
            role="button"
            tabIndex={0}
            aria-pressed={selected}
            aria-label={`Isolate the flow through ${node.label}${
              node.value ? `, ${fmt(node.value)}${unit ? ` ${unit}` : ""}` : ""
            }`}
            onMouseEnter={() => setFocus(node.id)}
            onMouseLeave={() => setFocus(null)}
            onFocus={() => setFocus(node.id)}
            onBlur={() => setFocus(null)}
            onClick={() => setFocus((f) => (f === node.id ? null : node.id))}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                setFocus((f) => (f === node.id ? null : node.id));
              }
              if (event.key === "Escape") setFocus(null);
            }}
            className="cursor-pointer focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary"
          >
            <rect
              x={x0}
              y={y0}
              width={x1 - x0}
              height={Math.max(2, y1 - y0)}
              rx={3}
              fill={node.colorVar ?? "var(--muted-foreground)"}
              fillOpacity={lit ? 0.9 : 0.25}
              className="transition-[fill-opacity] duration-200"
            />
            <text
              x={leftSide ? x1 + 7 : x0 - 7}
              y={(y0 + y1) / 2}
              dominantBaseline="central"
              textAnchor={leftSide ? "start" : "end"}
              fontSize={11.5}
              fillOpacity={lit ? 1 : 0.4}
              className="fill-foreground font-mono transition-[fill-opacity] duration-200"
            >
              {node.label}
              {node.value !== undefined && (
                <tspan className="fill-muted-foreground" dx={6}>
                  {fmt(node.value)}
                  {unit ? ` ${unit}` : ""}
                </tspan>
              )}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
