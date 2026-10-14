// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import * as d3 from "d3";
import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";
import type { Graph, GraphLink, GraphNode } from "./graph";

type SimNode = d3.SimulationNodeDatum & GraphNode & { r: number };
type SimLink = d3.SimulationLinkDatum<SimNode> & GraphLink & { curve: number };

export type Focus = { kind: "node"; id: string } | { kind: "line"; line: number } | null;

export type GraphViewHandle = { fit: () => void };

type Props = {
  graph: Graph;
  color: (label: string) => string;
  focus: Focus;
  onHoverNode: (id: string | null) => void;
  /** Client Y of a source line's midpoint, so new nodes enter beside their line. */
  lineY: (line: number) => number | null;
};

const radius = (n: GraphNode) => 7 + 4.5 * (Math.sqrt(n.lines.length) - 1);

export const GraphView = forwardRef<GraphViewHandle, Props>(function GraphView(
  { graph, color, focus, onHoverNode, lineY },
  ref,
) {
  const svgRef = useRef<SVGSVGElement>(null);
  const state = useRef<{
    sim: d3.Simulation<SimNode, SimLink>;
    zoom: d3.ZoomBehavior<SVGSVGElement, unknown>;
    nodes: SimNode[];
    links: SimLink[];
    userMoved: boolean;
    fitTimer?: number;
  } | null>(null);
  const reduceMotion = useRef(window.matchMedia("(prefers-reduced-motion: reduce)").matches);

  const fit = (animate = true) => {
    const s = state.current;
    const svg = svgRef.current;
    if (!s || !svg || s.nodes.length === 0) return;
    const { width, height } = svg.getBoundingClientRect();
    // Bounds include each node's label, which sits to the right of its dot.
    let [x0, y0, x1, y1] = [Infinity, Infinity, -Infinity, -Infinity];
    d3.select(svg)
      .selectAll<SVGGElement, SimNode>("g.node")
      .each(function (d) {
        const label = this.querySelector<SVGTextElement>("text.name")?.getComputedTextLength() ?? 0;
        const x = d.x ?? 0;
        const y = d.y ?? 0;
        x0 = Math.min(x0, x - d.r);
        x1 = Math.max(x1, x + d.r + 7 + label);
        y0 = Math.min(y0, y - d.r);
        y1 = Math.max(y1, y + 16);
      });
    const pad = 40;
    [x0, y0, x1, y1] = [x0 - pad, y0 - pad, x1 + pad, y1 + pad + 30];
    const k = Math.min(1.4, Math.max(0.3, Math.min(width / (x1 - x0), height / (y1 - y0))));
    const t = d3.zoomIdentity
      .translate(width / 2, height / 2)
      .scale(k)
      .translate(-(x0 + x1) / 2, -(y0 + y1) / 2);
    const sel = d3.select(svg);
    (animate && !reduceMotion.current ? sel.transition().duration(600) : sel).call(
      s.zoom.transform as never,
      t,
    );
    s.userMoved = false;
  };

  useImperativeHandle(ref, () => ({ fit: () => fit(true) }));

  // One-time setup: layers, zoom, simulation.
  useEffect(() => {
    const svg = d3.select(svgRef.current!);
    const root = svg.append("g").attr("class", "viewport");
    root.append("g").attr("class", "links");
    root.append("g").attr("class", "link-labels");
    root.append("g").attr("class", "nodes");

    const zoom = d3
      .zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.25, 3])
      .on("zoom", (ev) => {
        root.attr("transform", ev.transform.toString());
        if (ev.sourceEvent && state.current) state.current.userMoved = true;
      });
    svg.call(zoom).on("dblclick.zoom", null);

    const sim = d3
      .forceSimulation<SimNode, SimLink>()
      .force("charge", d3.forceManyBody<SimNode>().strength(-900).distanceMax(600))
      .force(
        "link",
        d3
          .forceLink<SimNode, SimLink>()
          .id((d) => d.id)
          .distance(170)
          .strength(0.4),
      )
      .force("x", d3.forceX<SimNode>(0).strength(0.035))
      .force("y", d3.forceY<SimNode>(0).strength(0.05))
      .force("collide", d3.forceCollide<SimNode>((d) => d.r + 22 + Math.min(d.name.length, 24) * 3.6))
      .alphaDecay(0.035)
      .on("tick", () => render())
      .on("end", () => {
        if (state.current && !state.current.userMoved) fit(true);
      });

    const { width, height } = svgRef.current!.getBoundingClientRect();
    svg.call(zoom.transform, d3.zoomIdentity.translate(width / 2, height / 2));
    state.current = { sim, zoom, nodes: [], links: [], userMoved: false };
    if (import.meta.env.DEV) {
      // Lets headless checks settle the layout without animation frames.
      (window as unknown as { __kgSettle: () => void }).__kgSettle = () => {
        sim.alpha(1).stop().tick(300);
        render();
        fit(false);
      };
    }
    return () => {
      sim.stop();
      root.remove();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const render = () => {
    const svg = d3.select(svgRef.current!);
    svg
      .select<SVGGElement>(".nodes")
      .selectAll<SVGGElement, SimNode>("g.node")
      .attr("transform", (d) => `translate(${d.x},${d.y})`);
    svg
      .select(".links")
      .selectAll<SVGPathElement, SimLink>("path")
      .attr("d", (d) => linkPath(d));
    svg
      .select(".link-labels")
      .selectAll<SVGTextElement, SimLink>("text")
      .attr("transform", (d) => {
        const [x, y] = linkMid(d);
        return `translate(${x},${y})`;
      });
  };

  // Data join whenever the graph changes.
  useEffect(() => {
    const s = state.current;
    const svgEl = svgRef.current;
    if (!s || !svgEl) return;
    const svg = d3.select(svgEl);
    const prev = new Map(s.nodes.map((n) => [n.id, n]));
    const transform = d3.zoomTransform(svgEl);
    const box = svgEl.getBoundingClientRect();
    let added = 0;

    const nodes: SimNode[] = graph.nodes.map((n) => {
      const old = prev.get(n.id);
      if (old) return Object.assign(old, n, { r: radius(n) });
      added++;
      // Enter from the left edge, level with the line that introduced it.
      const cy = lineY(n.lines[0]);
      const [x, y] = transform.invert([
        24 + Math.random() * 30,
        cy === null ? box.height / 2 : Math.min(box.height - 24, Math.max(24, cy - box.top)),
      ]);
      return { ...n, r: radius(n), x, y, vx: 6, vy: 0 };
    });

    const pairCount = new Map<string, number>();
    const links: SimLink[] = graph.links.map((l) => {
      const pair = [l.source, l.target].sort().join("|");
      const i = pairCount.get(pair) ?? 0;
      pairCount.set(pair, i + 1);
      // Alternate sides for parallel edges: 0, +1, -1, +2, ...
      const curve = i === 0 ? 0 : (i % 2 ? 1 : -1) * Math.ceil(i / 2);
      return { ...l, curve };
    });

    s.nodes = nodes;
    s.links = links;

    const node = svg
      .select<SVGGElement>(".nodes")
      .selectAll<SVGGElement, SimNode>("g.node")
      .data(nodes, (d) => d.id)
      .join((enter) => {
        const g = enter.append("g").attr("class", "node entering");
        g.append("circle").attr("class", "halo");
        g.append("circle").attr("class", "dot");
        g.append("text").attr("class", "name");
        g.append("text").attr("class", "type");
        g.on("mouseenter", (_, d) => onHoverNode(d.id)).on("mouseleave", () => onHoverNode(null));
        g.call(
          d3
            .drag<SVGGElement, SimNode>()
            .on("start", (ev, d) => {
              if (!ev.active) s.sim.alphaTarget(0.2).restart();
              d.fx = d.x;
              d.fy = d.y;
            })
            .on("drag", (ev, d) => {
              d.fx = ev.x;
              d.fy = ev.y;
            })
            .on("end", (ev, d) => {
              if (!ev.active) s.sim.alphaTarget(0);
              d.fx = null;
              d.fy = null;
            }),
        );
        window.setTimeout(() => g.classed("entering", false), 700);
        return g;
      });
    node
      .select<SVGCircleElement>("circle.dot")
      .attr("r", (d) => d.r)
      .style("fill", (d) => color(d.label));
    node.select<SVGCircleElement>("circle.halo").attr("r", (d) => d.r + 6).style("stroke", (d) => color(d.label));
    node
      .select<SVGTextElement>("text.name")
      .attr("x", (d) => d.r + 7)
      .attr("y", 1)
      .text((d) => d.name);
    node
      .select<SVGTextElement>("text.type")
      .attr("x", (d) => d.r + 7)
      .attr("y", 14)
      .text((d) => d.label);

    svg
      .select(".links")
      .selectAll<SVGPathElement, SimLink>("path")
      .data(links, (d) => d.id)
      .join((enter) => enter.append("path").attr("class", "link entering").attr("marker-end", "url(#arrow)"))
      .call((sel) => window.setTimeout(() => sel.classed("entering", false), 700));
    svg
      .select(".link-labels")
      .selectAll<SVGTextElement, SimLink>("text")
      .data(links, (d) => d.id)
      .join((enter) => enter.append("text").attr("class", "link-label entering"))
      .text((d) => d.type.replace(/_/g, " "))
      .call((sel) => window.setTimeout(() => sel.classed("entering", false), 700));

    s.sim.nodes(nodes);
    (s.sim.force("link") as d3.ForceLink<SimNode, SimLink>).links(links);
    if (reduceMotion.current) {
      s.sim.alpha(1).stop();
      for (let i = 0; i < 200; i++) s.sim.tick();
      render();
    } else {
      s.sim.alpha(added > 0 ? 0.7 : 0.25).restart();
    }

    // Keep the whole graph in view as it grows, until the user pans or zooms.
    window.clearTimeout(s.fitTimer);
    if (!s.userMoved && nodes.length > 0) s.fitTimer = window.setTimeout(() => fit(true), 900);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [graph, color]);

  // Focus: dim everything that isn't connected to the hovered node or line.
  useEffect(() => {
    const svg = d3.select(svgRef.current!);
    const s = state.current;
    if (!s) return;
    let nodeOn: (n: SimNode) => boolean = () => true;
    let linkOn: (l: SimLink) => boolean = () => true;
    if (focus?.kind === "node") {
      const near = new Set([focus.id]);
      for (const l of s.links) {
        const a = (l.source as SimNode).id;
        const b = (l.target as SimNode).id;
        if (a === focus.id) near.add(b);
        if (b === focus.id) near.add(a);
      }
      nodeOn = (n) => near.has(n.id);
      linkOn = (l) => (l.source as SimNode).id === focus.id || (l.target as SimNode).id === focus.id;
    } else if (focus?.kind === "line") {
      nodeOn = (n) => n.lines.includes(focus.line);
      linkOn = (l) => l.lines.includes(focus.line);
    }
    svg.classed("focused", focus !== null);
    svg.selectAll<SVGGElement, SimNode>("g.node").classed("on", nodeOn);
    svg.selectAll<SVGPathElement, SimLink>("path.link").classed("on", linkOn);
    svg.selectAll<SVGTextElement, SimLink>("text.link-label").classed("on", linkOn);
  }, [focus, graph]);

  return (
    <svg ref={svgRef} className="graph" role="img" aria-label="Knowledge graph">
      <defs>
        <marker
          id="arrow"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto-start-reverse"
        >
          <path d="M 0 1 L 9 5 L 0 9 z" className="arrowhead" />
        </marker>
      </defs>
    </svg>
  );
});

function ends(d: SimLink) {
  const s = d.source as SimNode;
  const t = d.target as SimNode;
  const sx = s.x ?? 0;
  const sy = s.y ?? 0;
  const tx = t.x ?? 0;
  const ty = t.y ?? 0;
  const dx = tx - sx;
  const dy = ty - sy;
  const len = Math.hypot(dx, dy) || 1;
  // Control point offset perpendicular to the edge for parallel links.
  const off = d.curve * 34;
  const cx = (sx + tx) / 2 - (dy / len) * off;
  const cy = (sy + ty) / 2 + (dx / len) * off;
  // Stop short of the target circle so the arrowhead stays visible.
  const ux = tx - cx;
  const uy = ty - cy;
  const ul = Math.hypot(ux, uy) || 1;
  const ex = tx - (ux / ul) * (t.r + 5);
  const ey = ty - (uy / ul) * (t.r + 5);
  return { sx, sy, cx, cy, ex, ey };
}

function linkPath(d: SimLink) {
  const { sx, sy, cx, cy, ex, ey } = ends(d);
  return `M${sx},${sy} Q${cx},${cy} ${ex},${ey}`;
}

function linkMid(d: SimLink): [number, number] {
  const { sx, sy, cx, cy, ex, ey } = ends(d);
  // Point at t = 0.5 on the quadratic curve.
  return [0.25 * sx + 0.5 * cx + 0.25 * ex, 0.25 * sy + 0.5 * cy + 0.25 * ey];
}
