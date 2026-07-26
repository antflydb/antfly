#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Render a TLA+ ndjson trace as a self-contained HTML swimlane timeline.

Input is the ndjson format emitted by the Zig tracing writers: one JSON object
per line with a `*-trace` tag (or `trace` for raft) and an `event`. Non-JSON
line prefixes (logger timestamps) are stripped, matching
scripts/tla-validate-trace.sh.

Each trace family is assigned a visualization archetype in
specs/tla/traces/viz.json (see archetypes.py): `consensus` (node lanes, role
tenure bands, message arrows, multi-run segmentation), `dialogue` (lanes from
event-name rules, declarative causal pairs), or `narrative` (single actor,
per-event state diffs). Tenure-band colors can be linked to the model's phase
domain (phasecolors.py) so the same phase value gets the same hue here and in
the generated Mermaid state diagrams. Unbound tags render with the narrative
fallback.

Output is one HTML file with no external dependencies: category/lane filters,
hover detail, a table view, light/dark support. Faults (crash/corruption
events) always render in the reserved status-critical red.

Usage:
  render_trace.py trace.ndjson [-o out.html] [--bindings viz.json] [--max-events N]
"""

from __future__ import annotations

import argparse
import html
import json
import os
import sys
from pathlib import Path

import archetypes
import phasecolors
from archetypes import Binding

BOOTSTRAP_EVENTS = {"InitState", "BecomeFollower", "ApplyConfChange"}


def parse_lines(path: Path):
    """Yield parsed trace event objects, skipping non-trace lines."""
    with path.open() as f:
        for line in f:
            start = line.find("{")
            if start < 0:
                continue
            try:
                obj = json.loads(line[start:])
            except json.JSONDecodeError:
                continue
            tag = obj.get("tag", "")
            if (tag == "trace" or tag.endswith("-trace")) \
                    and isinstance(obj.get("event"), dict):
                yield obj


def flatten_event(ev: dict) -> dict:
    """Scalar view of an event's state for lanes, bands, and diffing. Nested
    dicts get dotted keys; lists are inlined when short. The message payload
    is excluded (it changes every event and is shown in the tooltip/table)."""
    out: dict = {}

    def add(key, val):
        if isinstance(val, (str, int, float, bool)) or val is None:
            out[key] = val
        elif isinstance(val, list):
            s = json.dumps(val)
            out[key] = s if len(s) <= 24 else f"[{len(val)} items]"
        elif isinstance(val, dict):
            for k2, v2 in val.items():
                add(f"{key}.{k2}", v2)

    for k, v in ev.items():
        if k not in ("name", "msg"):
            add(k, v)
    # "after.status" -> "status": the writers' post-state envelope adds noise.
    return {(k[6:] if k.startswith("after.") else k): v for k, v in out.items()}


def fmt(val) -> str:
    if isinstance(val, str) and val[:1] in ("[", "{"):
        return val  # already a compact JSON literal from flatten_event
    return json.dumps(val) if isinstance(val, (bool, str)) or val is None else str(val)


def is_state_reset(seg_events: list[dict], ev: dict) -> bool:
    """Same rule as tla-segment-raft-trace.py: a node's commit/log dropping
    back to zero marks a re-initialized engine (new test run)."""
    if ev.get("state", {}).get("commit", 0) != 0 or ev.get("log", 0) != 0:
        return False
    for prev in reversed(seg_events):
        if prev["lane"] == str(ev.get("nid", "?")):
            if prev.get("commit", 0) > 0 or prev.get("log", 0) > 0:
                return True
            break
    return False


def build_segments(objs, binding: Binding) -> list[list[dict]]:
    """Flatten trace objects into per-segment event records."""
    segments: list[list[dict]] = []
    current: list[dict] = []
    init_nodes: set[str] = set()
    lane_state: dict[str, dict] = {}
    lane_band: dict[str, object] = {}

    for obj in objs:
        ev = obj["event"]
        name = ev.get("name", "?")
        flat = flatten_event(ev)
        lane = binding.lane_for(name, flat)

        if binding.segmented:
            new_run = (name == "InitState" and lane in init_nodes) or (
                name not in BOOTSTRAP_EVENTS and current and is_state_reset(current, ev)
            )
            if new_run:
                segments.append(current)
                current, init_nodes = [], set()
                lane_state, lane_band = {}, {}
            if name == "InitState":
                init_nodes.add(lane)

        # What changed in this lane's observable state, for direct labels.
        prev = lane_state.get(lane)
        if prev is None:
            changes = [f"{k}={fmt(v)}" for k, v in flat.items()][:6]
        else:
            changes = [f"{k}: {fmt(prev[k])}→{fmt(v)}"
                       for k, v in flat.items() if k in prev and prev[k] != v]
            changes += [f"{k}={fmt(v)}" for k, v in flat.items() if k not in prev]
        lane_state[lane] = {**(prev or {}), **flat}

        # Tenure band value carries forward until a new one is reported —
        # per lane for actor state, trace-wide for global protocol state.
        band_key = "*" if binding.band_global else lane
        if binding.band_field and flat.get(binding.band_field) is not None:
            lane_band[band_key] = flat[binding.band_field]
        band = lane_band.get(band_key)

        state = ev.get("state") or {}
        rec = {
            "lane": lane,
            "name": name,
            "cat": binding.categorize(name),
            "band": band,
            "ms": binding.is_milestone(name),
            "term": state.get("term"),
            "commit": state.get("commit"),
            "log": ev.get("log"),
            "msg": ev.get("msg"),
            "changes": changes,
            "flat": flat,
            "detail": json.dumps(ev, indent=1),
        }
        current.append(rec)
    if current:
        segments.append(current)
    return segments


def match_msg_arrows(events: list[dict]) -> list[dict]:
    """Consensus archetype: pair SendX events with the ReceiveX that consumes
    the same message envelope."""
    pending: dict[tuple, list[int]] = {}
    arrows = []
    for i, ev in enumerate(events):
        msg = ev.get("msg")
        if not msg:
            continue
        key = (msg.get("type"), msg.get("from"), msg.get("to"),
               msg.get("term"), msg.get("index"), msg.get("logTerm"),
               msg.get("reject"))
        if ev["name"].startswith("Send"):
            pending.setdefault(key, []).append(i)
        elif ev["name"].startswith("Receive"):
            queue = pending.get(key)
            if queue:
                arrows.append({"from": queue.pop(0), "to": i, "cat": ev["cat"]})
    return arrows


def lane_order(events: list[dict], binding: Binding) -> list[str]:
    lanes = list(dict.fromkeys(ev["lane"] for ev in events))
    if binding.lane_order:
        known = [l for l in binding.lane_order if l in lanes]
        return known + sorted(l for l in lanes if l not in binding.lane_order)
    try:
        return sorted(lanes, key=int)
    except ValueError:
        return sorted(lanes)


def resolve_band_colors(binding: Binding, segments: list[list[dict]],
                        specs_dir: Path | None) -> dict[str, str]:
    """Explicit binding colors win; then the model's phase domain (shared with
    the diagrams layer); then first-appearance palette order."""
    colors = dict(binding.band_colors)
    if binding.model and binding.phase_var and specs_dir is not None:
        for value, hex_color in phasecolors.model_phase_colors(
                specs_dir, binding.model, binding.phase_var).items():
            colors.setdefault(value, hex_color)
    observed = list(dict.fromkeys(
        ev["band"] for seg in segments for ev in seg if ev["band"] is not None))
    unassigned = [v for v in observed if v not in colors]
    taken = set(colors.values())
    free = [s for s in phasecolors.SLOTS if s not in taken]
    for value, hex_color in zip(unassigned, free):
        colors[value] = hex_color
    return {v: c for v, c in colors.items() if v in observed}


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
  .viz-root {
    color-scheme: light;
    --surface-1: #fcfcfb; --page: #f9f9f7;
    --text-primary: #0b0b0b; --text-secondary: #52514e; --text-muted: #898781;
    --grid: #e1e0d9; --baseline: #c3c2b7; --border: rgba(11,11,11,0.10);
  }
  @media (prefers-color-scheme: dark) {
    :root:where(:not([data-theme="light"])) .viz-root {
      color-scheme: dark;
      --surface-1: #1a1a19; --page: #0d0d0d;
      --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #898781;
      --grid: #2c2c2a; --baseline: #383835; --border: rgba(255,255,255,0.10);
    }
  }
  :root[data-theme="dark"] .viz-root {
    color-scheme: dark;
    --surface-1: #1a1a19; --page: #0d0d0d;
    --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #898781;
    --grid: #2c2c2a; --baseline: #383835; --border: rgba(255,255,255,0.10);
  }
  body.viz-root {
    margin: 0; background: var(--page); color: var(--text-primary);
    font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif;
    overflow: hidden;
  }
  #app { display: flex; height: 100vh; }
  nav#sidebar { width: 250px; flex: none; overflow-y: auto;
    border-right: 1px solid var(--grid); padding: 8px 0 24px;
    background: var(--surface-1); }
  nav#sidebar h3 { font-size: 11px; text-transform: uppercase;
    letter-spacing: 0.04em; color: var(--text-muted); margin: 14px 12px 4px; }
  nav#sidebar .entry { display: block; padding: 4px 12px 4px 14px;
    font-size: 12.5px; color: var(--text-secondary); cursor: pointer;
    border-left: 2px solid transparent;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  nav#sidebar .entry:hover { background: var(--page); }
  nav#sidebar .entry.active { color: var(--text-primary);
    border-left-color: #2a78d6; background: var(--page); }
  nav#sidebar .entry .meta { color: var(--text-muted); font-size: 11px; }
  #main { flex: 1; overflow-y: auto; min-width: 0; }
  header {
    position: sticky; top: 0; z-index: 3; background: var(--page);
    padding: 10px 16px 8px; border-bottom: 1px solid var(--grid);
  }
  header h1 { font-size: 15px; margin: 0 0 6px; }
  header .sub { color: var(--text-secondary); font-size: 12px; margin-bottom: 8px; }
  .controls { display: flex; flex-wrap: wrap; gap: 6px 14px; align-items: center; }
  .controls .group { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; }
  .chip {
    display: inline-flex; align-items: center; gap: 6px;
    border: 1px solid var(--border); border-radius: 999px;
    padding: 2px 10px; cursor: pointer; user-select: none;
    color: var(--text-secondary); font-size: 12px; background: var(--surface-1);
  }
  .chip input { display: none; }
  .chip.off { opacity: 0.38; }
  .swatch { width: 10px; height: 10px; display: inline-block; }
  .bandlegend { font-size: 12px; color: var(--text-secondary);
    display: inline-flex; gap: 10px; align-items: center; }
  .bandlegend .swatch { border: 1px solid var(--border); }
  section { padding: 8px 16px 24px; }
  section h2 { font-size: 13px; color: var(--text-secondary); margin: 14px 0 4px; }
  .laneheads { display: flex; margin-left: 46px; position: sticky;
    top: var(--header-h, 84px);
    z-index: 2; background: var(--page); border-bottom: 1px solid var(--grid); }
  .laneheads div { text-align: center; font-size: 12px; color: var(--text-secondary);
    padding: 3px 0; }
  svg.timeline { display: block; background: var(--surface-1);
    border: 1px solid var(--border); }
  svg.timeline text { fill: var(--text-muted); font-size: 10px;
    font-variant-numeric: tabular-nums; }
  svg.timeline text.evlabel { fill: var(--text-primary); font-size: 11px; }
  svg.timeline text.chlabel { fill: var(--text-muted); font-size: 9.5px;
    font-variant-numeric: tabular-nums; }
  .lane-line { stroke: var(--grid); stroke-width: 1; }
  .mark { stroke: var(--surface-1); stroke-width: 1; }
  .hit { fill: transparent; cursor: pointer; }
  .arrow { fill: none; stroke-width: 1.2; opacity: 0.55; }
  .hidden { display: none !important; }
  #tooltip {
    position: fixed; z-index: 10; max-width: 420px; padding: 8px 10px;
    background: var(--surface-1); color: var(--text-primary);
    border: 1px solid var(--border); border-radius: 6px;
    box-shadow: 0 4px 14px rgba(0,0,0,0.18); font-size: 12px;
    pointer-events: none; white-space: pre-wrap; display: none;
  }
  table.events { border-collapse: collapse; font-size: 12px; margin-top: 6px;
    background: var(--surface-1); }
  table.events th, table.events td {
    border: 1px solid var(--grid); padding: 2px 8px; text-align: left;
    font-variant-numeric: tabular-nums; color: var(--text-secondary);
  }
  table.events th { color: var(--text-primary); position: sticky; }
  button.toggleview {
    border: 1px solid var(--border); background: var(--surface-1);
    color: var(--text-secondary); border-radius: 6px; padding: 2px 10px;
    font-size: 12px; cursor: pointer;
  }
</style>
</head>
<body class="viz-root">
<div id="app">
<nav id="sidebar" class="hidden"></nav>
<div id="main">
<header>
  <h1 id="title"></h1>
  <div class="sub" id="subtitle"></div>
  <div class="controls">
    <div class="group" id="cat-filters"></div>
    <div class="group" id="lane-filters"></div>
    <div class="group bandlegend" id="band-legend"></div>
    <div class="group"><button class="toggleview" id="viewbtn">Table view</button></div>
  </div>
</header>
<div id="charts"></div>
</div>
</div>
<div id="tooltip"></div>
<script>
// One or more traces; with several, a sidebar switches between them.
const TRACES = __DATA__;

// Per-trace globals, set by buildUI(). Small traces get a "narrated" layout:
// taller rows, every event labeled with its name and the state fields it
// changed.
let CUR, SPARSE, ROW_H, LANE_W, CATS, state;
const GUTTER = 46, TOP_PAD = 10;
const FAULT = 4;
const SVG = "http://www.w3.org/2000/svg";

function el(tag, attrs, parent) {
  const node = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  if (parent) parent.appendChild(node);
  return node;
}

// Mark shapes: shape is the secondary (color-independent) encoding.
function drawMark(parent, cat, x, y) {
  const c = CATS[cat].color, shape = CATS[cat].shape;
  if (shape === "circle") {
    return el("circle", {cx: x, cy: y, r: 3.5, fill: c, class: "mark"}, parent);
  }
  if (shape === "diamond") {
    const d = 4.6;
    return el("path", {d: `M ${x} ${y - d} L ${x + d} ${y} L ${x} ${y + d} L ${x - d} ${y} Z`,
                       fill: c, class: "mark"}, parent);
  }
  if (shape === "square") {
    return el("rect", {x: x - 3.2, y: y - 3.2, width: 6.4, height: 6.4,
                       fill: c, class: "mark"}, parent);
  }
  if (shape === "cross") {
    const d = 4.4, w = 1.6;
    return el("path", {d: `M ${x - d} ${y - d + w} L ${x - w} ${y} L ${x - d} ${y + d - w} L ${x - d + w} ${y + d} L ${x} ${y + w} L ${x + d - w} ${y + d} L ${x + d} ${y + d - w} L ${x + w} ${y} L ${x + d} ${y - d + w} L ${x + d - w} ${y - d} L ${x} ${y - w} L ${x - d + w} ${y - d} Z`,
                       fill: c, class: "mark"}, parent);
  }
  const d = 4.4;  // triangle
  return el("path", {d: `M ${x} ${y - d} L ${x + d} ${y + d} L ${x - d} ${y + d} Z`,
                     fill: c, class: "mark"}, parent);
}

function laneX(seg, lane) { return GUTTER + seg.lanes.indexOf(lane) * LANE_W + LANE_W / 2; }
function laneName(lane) { return CUR.lanePrefix + lane; }

function renderSegment(seg, idx) {
  const wrap = document.createElement("section");
  const events = seg.events;
  const h = TOP_PAD + events.length * ROW_H + 20;
  const w = GUTTER + seg.lanes.length * LANE_W + (SPARSE ? 460 : 10);

  const title = document.createElement("h2");
  title.textContent = CUR.segments.length > 1
    ? `Segment ${idx + 1} — ${events.length} events, ${seg.lanes.length} lanes`
    : `${events.length} events, ${seg.lanes.length} lanes`;
  wrap.appendChild(title);

  const heads = document.createElement("div");
  heads.className = "laneheads";
  heads.style.width = (w - GUTTER) + "px";
  for (const lane of seg.lanes) {
    const d = document.createElement("div");
    d.style.width = LANE_W + "px";
    d.textContent = laneName(lane);
    d.dataset.lane = lane;
    heads.appendChild(d);
  }
  wrap.appendChild(heads);

  const svg = el("svg", {class: "timeline", width: w, height: h,
                         viewBox: `0 0 ${w} ${h}`});
  wrap.appendChild(svg);

  // Tenure bands: per-lane washes for actor state, or one full-width strip
  // per span for global protocol state (bandGlobal).
  const bands = el("g", {}, svg);
  const bandLanes = CUR.bandGlobal ? [null] : seg.lanes;
  for (const lane of bandLanes) {
    let start = null, band = null;
    const bx = lane === null ? GUTTER - 6 : laneX(seg, lane) - LANE_W / 2 + 4;
    const bw = lane === null ? w - GUTTER - 8 : LANE_W - 8;
    const flush = (endIdx) => {
      if (band !== null && CUR.bands[band] && !CUR.bandQuiet.includes(band)) {
        const attrs = {x: bx, y: TOP_PAD + start * ROW_H - 5,
                       width: bw, height: (endIdx - start) * ROW_H,
                       fill: CUR.bands[band] + "30"};
        if (lane !== null) attrs["data-lane"] = lane;
        el("rect", attrs, bands);
      }
    };
    events.forEach((ev, i) => {
      if ((lane !== null && ev.lane !== lane) || ev.band === band) return;
      if (start !== null) flush(i);
      start = i; band = ev.band;
    });
    if (start !== null) flush(events.length);
  }

  // Lane spines + step ruler.
  for (const lane of seg.lanes) {
    const x = laneX(seg, lane);
    el("line", {x1: x, y1: TOP_PAD - 6, x2: x, y2: h - 10,
                class: "lane-line", "data-lane": lane}, svg);
  }
  for (let i = 0; i < events.length; i += (SPARSE ? 5 : 50)) {
    el("text", {x: 4, y: TOP_PAD + i * ROW_H + 3}, svg).textContent = String(i);
  }

  // Causal / message arrows.
  const defs = el("defs", {}, svg);
  CATS.forEach((cat, ci) => {
    const m = el("marker", {id: `arr-${idx}-${ci}`, viewBox: "0 0 8 8",
                            refX: 7, refY: 4, markerWidth: 6, markerHeight: 6,
                            orient: "auto-start-reverse"}, defs);
    el("path", {d: "M 0 0 L 8 4 L 0 8 Z", fill: cat.color}, m);
  });
  const arrowsG = el("g", {}, svg);
  for (const a of seg.arrows) {
    const evA = events[a.from], evB = events[a.to];
    el("line", {x1: laneX(seg, evA.lane), y1: TOP_PAD + a.from * ROW_H,
                x2: laneX(seg, evB.lane), y2: TOP_PAD + a.to * ROW_H,
                stroke: CATS[a.cat].color, class: "arrow",
                "marker-end": `url(#arr-${idx}-${a.cat})`,
                "data-cat": a.cat,
                "data-lanes": evA.lane + "," + evB.lane}, arrowsG);
  }

  // Event marks + labels.
  const marksG = el("g", {}, svg);
  const tooltip = document.getElementById("tooltip");
  events.forEach((ev, i) => {
    const x = laneX(seg, ev.lane), y = TOP_PAD + i * ROW_H;
    const g = el("g", {"data-cat": ev.cat, "data-lane": ev.lane}, marksG);
    drawMark(g, ev.cat, x, y);
    const warn = ev.cat === FAULT ? "⚠ " : "";
    if (SPARSE) {
      el("text", {x: x + 10, y: y, class: "evlabel"}, g).textContent = warn + ev.name;
      if (ev.changes && ev.changes.length) {
        const maxCh = seg.lanes.length > 1 ? 42 : 120;
        const txt = ev.changes.join("   ");
        el("text", {x: x + 10, y: y + 11, class: "chlabel"}, g).textContent =
          txt.length > maxCh ? txt.slice(0, maxCh - 1) + "…" : txt;
      }
    } else if (ev.ms || ev.cat === FAULT) {
      el("text", {x: x + 8, y: y + 3, class: "evlabel"}, g).textContent =
        warn + ev.name + (ev.term != null ? ` (t${ev.term})` : "");
    }
    const hit = el("circle", {cx: x, cy: y, r: 8, class: "hit"}, g);
    hit.addEventListener("mouseenter", () => {
      tooltip.textContent = `#${i} ${ev.name} — ${laneName(ev.lane)}\\n` + ev.detail;
      tooltip.style.display = "block";
    });
    hit.addEventListener("mousemove", (e) => {
      const pad = 14;
      tooltip.style.left = Math.min(e.clientX + pad, window.innerWidth - 440) + "px";
      tooltip.style.top = Math.min(e.clientY + pad, window.innerHeight - tooltip.offsetHeight - 10) + "px";
    });
    hit.addEventListener("mouseleave", () => { tooltip.style.display = "none"; });
  });

  // Table view (accessibility / exact values).
  const hasMsg = events.some(ev => ev.msg);
  const table = document.createElement("table");
  table.className = "events hidden";
  table.innerHTML = "<thead><tr><th>#</th><th>Lane</th><th>Event</th>" +
    (CUR.bandField ? `<th>${CUR.bandField}</th>` : "") +
    (hasMsg ? "<th>Message</th>" : "") +
    "<th>Changed</th></tr></thead>";
  const tbody = document.createElement("tbody");
  events.forEach((ev, i) => {
    const tr = document.createElement("tr");
    tr.dataset.cat = ev.cat; tr.dataset.lane = ev.lane;
    const cells = [i, ev.lane, (ev.cat === FAULT ? "⚠ " : "") + ev.name];
    if (CUR.bandField) cells.push(ev.band ?? "");
    if (hasMsg) cells.push(ev.msg ? `${ev.msg.type} ${ev.msg.from}→${ev.msg.to}` : "");
    cells.push((ev.changes || []).join("; "));
    for (const v of cells) {
      const td = document.createElement("td");
      td.textContent = String(v);
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  wrap.appendChild(table);

  return wrap;
}

function applyFilters() {
  for (const node of document.querySelectorAll("[data-cat],[data-lane]")) {
    const catOK = !node.dataset.cat || state.cats.has(Number(node.dataset.cat));
    let laneOK = true;
    if (node.dataset.lane) laneOK = state.lanes.has(node.dataset.lane);
    if (node.dataset.lanes) {
      laneOK = node.dataset.lanes.split(",").every(l => state.lanes.has(l));
    }
    node.classList.toggle("hidden", !(catOK && laneOK));
  }
}

function chip(parent, label, swatchColor, onToggle) {
  const lab = document.createElement("label");
  lab.className = "chip";
  if (swatchColor) {
    const sw = document.createElement("span");
    sw.className = "swatch";
    sw.style.background = swatchColor;
    lab.appendChild(sw);
  }
  lab.appendChild(document.createTextNode(label));
  lab.addEventListener("click", () => {
    const on = lab.classList.toggle("off");
    onToggle(!on);
    applyFilters();
  });
  parent.appendChild(lab);
}

const SHAPE_GLYPH = {circle: "●", diamond: "◆", square: "■", triangle: "▲", cross: "✕"};

function buildSidebar() {
  if (TRACES.length < 2) return;
  const nav = document.getElementById("sidebar");
  nav.classList.remove("hidden");
  const groups = new Map();
  TRACES.forEach((t, i) => {
    if (!groups.has(t.label)) groups.set(t.label, []);
    groups.get(t.label).push(i);
  });
  for (const [label, idxs] of groups) {
    const h = document.createElement("h3");
    h.textContent = label;
    nav.appendChild(h);
    for (const i of idxs) {
      const a = document.createElement("a");
      a.className = "entry";
      a.title = TRACES[i].name;
      a.dataset.idx = i;
      const n = TRACES[i].data.segments.reduce((s, seg) => s + seg.events.length, 0);
      a.appendChild(document.createTextNode(TRACES[i].name + " "));
      const meta = document.createElement("span");
      meta.className = "meta";
      meta.textContent = `· ${n}`;
      a.appendChild(meta);
      a.addEventListener("click", () => buildUI(i));
      nav.appendChild(a);
    }
  }
}

function buildUI(idx) {
  CUR = TRACES[idx].data;
  SPARSE = CUR.sparse;
  ROW_H = SPARSE ? 24 : 12;
  LANE_W = SPARSE ? 240 : 130;
  CATS = CUR.categories;
  state = {
    cats: new Set(CATS.map((_, i) => i)),
    lanes: new Set(CUR.segments.flatMap(s => s.lanes)),
    table: false,
  };
  document.getElementById("title").textContent = TRACES[idx].name;
  document.getElementById("subtitle").textContent = TRACES[idx].subtitle;
  document.getElementById("viewbtn").textContent = "Table view";
  for (const id of ["cat-filters", "lane-filters", "band-legend", "charts"])
    document.getElementById(id).replaceChildren();

  const usedCats = new Set(CUR.segments.flatMap(s => s.events.map(e => e.cat)));
  const catBox = document.getElementById("cat-filters");
  CATS.forEach((cat, i) => {
    if (!usedCats.has(i)) return;
    chip(catBox, `${cat.label} ${SHAPE_GLYPH[cat.shape]}`, cat.color,
         on => on ? state.cats.add(i) : state.cats.delete(i));
  });
  const laneBox = document.getElementById("lane-filters");
  const allLanes = [...state.lanes];
  if (allLanes.length > 1 && allLanes.length <= 12) {
    for (const lane of allLanes) {
      chip(laneBox, laneName(lane), null,
           on => on ? state.lanes.add(lane) : state.lanes.delete(lane));
    }
  }

  // Band (phase) legend: colors shared with the model's state diagram.
  const bandBox = document.getElementById("band-legend");
  const bandEntries = Object.entries(CUR.bands);
  if (bandEntries.length) {
    const label = document.createElement("span");
    label.textContent = CUR.bandField + ":";
    bandBox.appendChild(label);
    for (const [value, color] of bandEntries) {
      const item = document.createElement("span");
      const quiet = CUR.bandQuiet.includes(value);
      const sw = document.createElement("span");
      sw.className = "swatch";
      sw.style.background = quiet ? "transparent" : color + "60";
      item.appendChild(sw);
      item.appendChild(document.createTextNode(" " + value +
        (quiet ? " (untinted)" : "")));
      bandBox.appendChild(item);
    }
  }

  const charts = document.getElementById("charts");
  CUR.segments.forEach((seg, i) => charts.appendChild(renderSegment(seg, i)));

  for (const e of document.querySelectorAll("#sidebar .entry"))
    e.classList.toggle("active", Number(e.dataset.idx) === idx);
  document.getElementById("main").scrollTop = 0;
  syncHeaderHeight();
}

function syncHeaderHeight() {
  const h = document.querySelector("header").offsetHeight;
  document.documentElement.style.setProperty("--header-h", h + "px");
}
window.addEventListener("resize", syncHeaderHeight);

document.getElementById("viewbtn").addEventListener("click", (e) => {
  state.table = !state.table;
  e.target.textContent = state.table ? "Timeline view" : "Table view";
  for (const svg of document.querySelectorAll("svg.timeline, .laneheads"))
    svg.classList.toggle("hidden", state.table);
  for (const t of document.querySelectorAll("table.events"))
    t.classList.toggle("hidden", !state.table);
});

buildSidebar();
buildUI(0);
</script>
</body>
</html>
"""


def prepare_trace(path: Path, bindings: dict[str, "Binding"],
                  specs_dir: Path | None, max_events: int) -> dict | None:
    """Parse and lay out one trace file into a sidebar entry, or None with a
    warning if it is empty or oversized."""
    objs = list(parse_lines(path))
    if not objs:
        print(f"skip {path}: no trace events found", file=sys.stderr)
        return None
    if len(objs) > max_events:
        print(f"skip {path}: {len(objs)} events exceeds --max-events="
              f"{max_events}; segment it first "
              "(scripts/tla-segment-raft-trace.py)", file=sys.stderr)
        return None

    kind = objs[0]["tag"]
    binding = bindings.get(kind) or archetypes.fallback(kind)
    segments = build_segments(objs, binding)
    band_colors = resolve_band_colors(binding, segments, specs_dir)

    seg_data = []
    for events in segments:
        if binding.msg_arrows:
            arrows = match_msg_arrows(events)
        elif binding.pairs:
            arrows = archetypes.match_pairs(events, binding.pairs)
        else:
            arrows = []
        for ev in events:
            del ev["flat"]
        seg_data.append({"lanes": lane_order(events, binding),
                         "events": events, "arrows": arrows})

    n_arrows = sum(len(s["arrows"]) for s in seg_data)
    subtitle = (
        f"{len(objs)} events, {len(segments)} segment(s) — {binding.label} trace, "
        f"{binding.archetype} archetype. Rows are event order (top to bottom)."
        + (f" {n_arrows} matched causal arrows." if n_arrows else "")
        + ("" if binding.bound else
           " No viz binding for this tag — rendered with the generic narrative "
           "preset; add one in specs/tla/traces/viz.json.")
    )
    return {
        "name": path.name,
        "label": binding.label,
        "subtitle": subtitle,
        "data": {
            "kind": kind,
            "sparse": len(objs) <= 200,
            "lanePrefix": binding.lane_prefix,
            "categories": binding.category_legend(),
            "bandField": binding.band_field,
            "bandGlobal": binding.band_global,
            "bands": band_colors,
            "bandQuiet": binding.band_quiet,
            "segments": seg_data,
        },
    }


def expand_inputs(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for p in paths:
        if p.is_dir():
            files += sorted(f for f in p.rglob("*.ndjson"))
        else:
            files.append(p)
    return list(dict.fromkeys(files))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("traces", type=Path, nargs="+",
                    help="ndjson trace file(s) and/or directories to scan; "
                         "several inputs produce one HTML with a sidebar")
    ap.add_argument("-o", "--output", type=Path,
                    help="output HTML path (default: <trace>.html)")
    ap.add_argument("--bindings", type=Path,
                    help="viz.json binding table (default: found next to the "
                         "first trace or under specs/tla/traces/)")
    ap.add_argument("--max-events", type=int, default=20000,
                    help="skip traces with more events than this (default "
                         "20000); pre-split with scripts/tla-segment-*.py")
    args = ap.parse_args()

    files = expand_inputs(args.traces)
    if not files:
        print("no .ndjson files found", file=sys.stderr)
        return 1

    bindings_path = archetypes.find_bindings_file(files[0], args.bindings)
    bindings, specs_dir = {}, None
    if bindings_path is not None:
        bindings = archetypes.load_bindings(bindings_path)
        specs_dir = bindings_path.parent.parent

    pairs = [(f, e) for f in files
             if (e := prepare_trace(f, bindings, specs_dir, args.max_events))]
    if not pairs:
        print("no renderable traces", file=sys.stderr)
        return 1
    entries = [e for _, e in pairs]

    # Sidebar names: paths relative to the inputs' common directory.
    if len(pairs) > 1:
        common = os.path.commonpath([str(f.parent) for f, _ in pairs])
        for f, e in pairs:
            e["name"] = os.path.relpath(f, common)

    title = (f"TLA+ trace — {entries[0]['name']}" if len(entries) == 1
             else f"TLA+ traces — {len(entries)} files")
    out = args.output or files[0].with_suffix(".html")
    page = (HTML_TEMPLATE
            .replace("__TITLE__", html.escape(title))
            .replace("__DATA__", json.dumps(entries).replace("</", "<\\/")))
    out.write_text(page)
    print(f"wrote {out} ({out.stat().st_size // 1024} KiB, "
          f"{len(entries)} trace(s))")
    return 0


if __name__ == "__main__":
    sys.exit(main())
