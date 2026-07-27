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
import re
import subprocess
import sys
import tempfile
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


def replay_enabled(binding: Binding) -> bool:
    return binding.scene is not None or binding.archetype == "narrative"


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
        if replay_enabled(binding):
            # Replay frames need the carried-forward observable state and the
            # display-invariant verdicts at this step.
            rec["state"] = dict(lane_state[lane])
            rec["inv"] = binding.eval_invariants(lane_state[lane])
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
                        specs_dir: Path | None) -> dict[str, tuple[str, str]]:
    """Explicit binding colors win; then the model's phase domain (shared with
    the diagrams layer); then first-appearance palette order. Values are
    (light-surface, dark-surface) hex pairs."""
    # viz.json bandColors entries may be a single hex or a [light, dark] pair.
    colors = {v: tuple(c) if isinstance(c, list) else (c, c)
              for v, c in binding.band_colors.items()}
    if binding.model and binding.phase_var and specs_dir is not None:
        for value, pair in phasecolors.model_phase_colors(
                specs_dir, binding.model, binding.phase_var).items():
            colors.setdefault(value, pair)
    observed = list(dict.fromkeys(
        ev["band"] for seg in segments for ev in seg if ev["band"] is not None))
    unassigned = [v for v in observed if v not in colors]
    taken = set(colors.values())
    free = [s for s in phasecolors.SLOTS if s not in taken]
    for value, pair in zip(unassigned, free):
        colors[value] = pair
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
  #viewbtns button, button.ctrl {
    border: 1px solid var(--border); background: var(--surface-1);
    color: var(--text-secondary); border-radius: 6px; padding: 2px 10px;
    font-size: 12px; cursor: pointer;
  }
  #viewbtns button.active { color: var(--text-primary);
    border-color: #2a78d6; }
  /* ---- Replay view ---- */
  .replay { display: flex; gap: 0; align-items: stretch; }
  .rail { width: 240px; flex: none; overflow-y: auto; max-height: calc(100vh - 140px);
    border-right: 1px solid var(--grid); padding: 4px 0; }
  .rail .rentry { padding: 2px 10px; font-size: 12px; cursor: pointer;
    color: var(--text-secondary); display: flex; gap: 6px; align-items: baseline;
    border-left: 2px solid transparent; white-space: nowrap; overflow: hidden;
    text-overflow: ellipsis; }
  .rail .rentry .ridx { color: var(--text-muted); font-size: 10px;
    font-variant-numeric: tabular-nums; min-width: 18px; }
  .rail .rentry.active { color: var(--text-primary); background: var(--surface-1);
    border-left-color: #2a78d6; }
  .rail .rentry.invfail { border-left-color: #d03b3b; }
  .rail .rentry .vmark { color: #d03b3b; font-weight: 600; }
  .stage { flex: 1; min-width: 0; padding: 10px 18px 24px; }
  .banner { border-radius: 6px; padding: 6px 12px; font-size: 12.5px;
    margin-bottom: 10px; border: 1px solid var(--border); }
  .banner.pass { border-color: #0ca30c; color: var(--text-primary); }
  .banner.fail { border-color: #d03b3b; color: var(--text-primary);
    background: #d03b3b18; }
  .banner.unavailable { color: var(--text-muted); }
  .rcontrols { display: flex; gap: 8px; align-items: center; margin-bottom: 12px;
    font-size: 12.5px; color: var(--text-secondary); flex-wrap: wrap; }
  .rcontrols .evname { font-weight: 600; color: var(--text-primary);
    font-size: 14px; }
  .rcontrols .evname.fault { color: #d03b3b; }
  .scenebox { background: var(--surface-1); border: 1px solid var(--border);
    border-radius: 10px; padding: 14px 16px; margin-bottom: 10px; }
  .scenebox.fault { border-color: #d03b3b; box-shadow: 0 0 0 1px #d03b3b; }
  .srow { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 10px;
    align-items: stretch; }
  .store { border: 1px solid var(--baseline); border-radius: 8px;
    padding: 8px 12px; min-width: 96px; text-align: center;
    background: var(--page); transition: box-shadow 0.2s; }
  .store .slabel { font-size: 10.5px; color: var(--text-muted);
    margin-bottom: 4px; max-width: 130px; }
  .store .sval { font-size: 20px; font-variant-numeric: tabular-nums;
    color: var(--text-primary); }
  .store .sval .delta { font-size: 11px; margin-left: 4px; }
  .store .schips { display: flex; gap: 4px; flex-wrap: wrap;
    justify-content: center; min-height: 18px; }
  .store .schips span { border: 1px solid var(--baseline); border-radius: 4px;
    padding: 0 5px; font-size: 11px; color: var(--text-primary); }
  .store.durable { border-style: dashed; }
  .store.public { border-color: #0ca30c; }
  .store.record .sval { font-size: 13px; padding: 2px 8px; border-radius: 999px;
    display: inline-block; }
  .store.pulse { box-shadow: 0 0 0 2px #2a78d6; }
  .actor { border: 1px solid var(--baseline); border-radius: 8px;
    padding: 6px 12px; background: var(--page); font-size: 12px;
    color: var(--text-primary); }
  .actor .dots { display: flex; gap: 10px; margin-top: 4px; }
  .actor .dots span { font-size: 10.5px; color: var(--text-muted);
    display: inline-flex; gap: 4px; align-items: center; }
  .actor .dots i { width: 9px; height: 9px; border-radius: 50%;
    border: 1px solid var(--text-muted); display: inline-block; }
  .actor .dots i.on { background: #0ca30c; border-color: #0ca30c; }
  .actor.pulse { box-shadow: 0 0 0 2px #2a78d6; }
  .lamp { display: inline-flex; gap: 6px; align-items: center; font-size: 12px;
    color: var(--text-secondary); margin-right: 14px; }
  .lamp i { width: 11px; height: 11px; border-radius: 50%;
    border: 1px solid var(--text-muted); display: inline-block; }
  .lamp i.on { background: #0ca30c; border-color: #0ca30c; }
  .flowcap { font-size: 12.5px; color: var(--text-primary); min-height: 18px; }
  .flowcap .arrow { color: #2a78d6; font-weight: 600; }
  .invbadges { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }
  .invbadge { border-radius: 999px; padding: 1px 10px; font-size: 11.5px;
    border: 1px solid var(--baseline); color: var(--text-muted); }
  .invbadge.ok { border-color: #0ca30c; color: var(--text-primary); }
  .invbadge.bad { border-color: #d03b3b; background: #d03b3b22;
    color: var(--text-primary); font-weight: 600; }
  .statecard { display: grid; grid-template-columns: repeat(auto-fill, minmax(190px, 1fr));
    gap: 4px 16px; font-size: 12px; background: var(--surface-1);
    border: 1px solid var(--border); border-radius: 10px; padding: 10px 14px; }
  .statecard div { color: var(--text-muted); white-space: nowrap;
    overflow: hidden; text-overflow: ellipsis; }
  .statecard b { color: var(--text-primary); font-weight: 500;
    font-variant-numeric: tabular-nums; }
  .statecard div.chg b { color: #2a78d6; }
  .pill { padding: 1px 10px; border-radius: 999px; font-size: 12px;
    border: 1px solid var(--baseline); color: var(--text-primary); }
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
    <div class="group" id="viewbtns"></div>
  </div>
</header>
<div id="replay" class="replay hidden"></div>
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
let CUR, CUR_IDX = 0, SPARSE, ROW_H, LANE_W, CATS, state;
const GUTTER = 46, TOP_PAD = 10;
const FAULT = 4;
const SVG = "http://www.w3.org/2000/svg";

// Colors ship as [light-surface, dark-surface] hex pairs; pick by the mode
// actually rendering, and re-render if the OS theme flips. Band washes need
// more alpha on the dark surface to keep their hue identity.
const darkMq = window.matchMedia("(prefers-color-scheme: dark)");
const pick = (c) => Array.isArray(c) ? c[darkMq.matches ? 1 : 0] : c;
const WASH = () => darkMq.matches ? "4A" : "30";
darkMq.addEventListener("change", () => buildUI(CUR_IDX));

function el(tag, attrs, parent) {
  const node = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  if (parent) parent.appendChild(node);
  return node;
}

// Mark shapes: shape is the secondary (color-independent) encoding.
function drawMark(parent, cat, x, y) {
  const c = pick(CATS[cat].color), shape = CATS[cat].shape;
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
                       fill: pick(CUR.bands[band]) + WASH()};
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
    el("path", {d: "M 0 0 L 8 4 L 0 8 Z", fill: pick(cat.color)}, m);
  });
  const arrowsG = el("g", {}, svg);
  for (const a of seg.arrows) {
    const evA = events[a.from], evB = events[a.to];
    el("line", {x1: laneX(seg, evA.lane), y1: TOP_PAD + a.from * ROW_H,
                x2: laneX(seg, evB.lane), y2: TOP_PAD + a.to * ROW_H,
                stroke: pick(CATS[a.cat].color), class: "arrow",
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
  CUR_IDX = idx;
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
  const sub = document.getElementById("subtitle");
  sub.textContent = TRACES[idx].subtitle + " ";
  if (TRACES[idx].data.modelDiagram) {
    const md = TRACES[idx].data.modelDiagram;
    const a = document.createElement("a");
    a.textContent = `model diagram: ${md.model}`;
    a.title = md.path;
    a.style.color = "#2a78d6";
    if (md.href) { a.href = md.href; a.target = "_blank"; }
    sub.appendChild(a);
  }
  for (const id of ["cat-filters", "lane-filters", "band-legend", "charts",
                    "replay", "viewbtns"])
    document.getElementById(id).replaceChildren();
  if (PLAYING) { clearInterval(PLAYING); PLAYING = null; }

  const usedCats = new Set(CUR.segments.flatMap(s => s.events.map(e => e.cat)));
  const catBox = document.getElementById("cat-filters");
  CATS.forEach((cat, i) => {
    if (!usedCats.has(i)) return;
    chip(catBox, `${cat.label} ${SHAPE_GLYPH[cat.shape]}`, pick(cat.color),
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
      sw.style.background = quiet ? "transparent" : pick(color) + "60";
      item.appendChild(sw);
      item.appendChild(document.createTextNode(" " + value +
        (quiet ? " (untinted)" : "")));
      bandBox.appendChild(item);
    }
  }

  const charts = document.getElementById("charts");
  CUR.segments.forEach((seg, i) => charts.appendChild(renderSegment(seg, i)));

  buildViews();
  if (CUR.replay) buildReplay();
  setView(CUR.replay ? "replay" : "timeline");

  for (const e of document.querySelectorAll("#sidebar .entry"))
    e.classList.toggle("active", Number(e.dataset.idx) === idx);
  document.getElementById("main").scrollTop = 0;
  syncHeaderHeight();
}

// ---- View switching (Replay | Timeline | Table) ----

let VIEW = "timeline", FRAMES = [], FRAME = 0, PLAYING = null, SCENE_FLOWS = [];

function setView(v) {
  VIEW = v;
  document.getElementById("replay").classList.toggle("hidden", v !== "replay");
  document.getElementById("charts").classList.toggle("hidden", v === "replay");
  for (const el2 of document.querySelectorAll("svg.timeline, .laneheads"))
    el2.classList.toggle("hidden", v !== "timeline");
  for (const t of document.querySelectorAll("table.events"))
    t.classList.toggle("hidden", v !== "table");
  for (const b of document.querySelectorAll("#viewbtns button"))
    b.classList.toggle("active", b.dataset.view === v);
  syncHeaderHeight();
}

function buildViews() {
  const box = document.getElementById("viewbtns");
  const views = (CUR.replay ? ["replay"] : []).concat(["timeline", "table"]);
  for (const v of views) {
    const b = document.createElement("button");
    b.dataset.view = v;
    b.textContent = v[0].toUpperCase() + v.slice(1);
    b.addEventListener("click", () => setView(v));
    box.appendChild(b);
  }
}

// ---- Replay view: scene + state card, stepped frame by frame ----

function parseList(v) {
  if (Array.isArray(v)) return v;
  if (typeof v === "string" && v.startsWith("[")) {
    try { return JSON.parse(v); } catch { return []; }
  }
  return [];
}

function h(tag, cls, parent, text) {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text !== undefined) n.textContent = text;
  if (parent) parent.appendChild(n);
  return n;
}

function buildReplay() {
  FRAMES = CUR.segments.flatMap(s => s.events);
  const root = document.getElementById("replay");
  const scene = CUR.scene || {};
  SCENE_FLOWS = (scene.flows || []).map(f => ({...f, re: new RegExp(f.on)}));

  const rail = h("div", "rail", root);
  const verdict = CUR.verdict;
  FRAMES.forEach((ev, i) => {
    const e = h("div", "rentry", rail);
    e.dataset.frame = i;
    const invFail = (ev.inv || []).some(v => v === false);
    if (invFail) e.classList.add("invfail");
    h("span", "ridx", e, String(i));
    if (verdict && verdict.step === i) h("span", "vmark", e, "⚑");
    h("span", ev.cat === FAULT ? "vmark" : "", e,
      (ev.cat === FAULT ? "⚠ " : "") + ev.name);
    e.addEventListener("click", () => setFrame(i));
  });

  const stage = h("div", "stage", root);
  if (verdict) {
    const cls = verdict.status === "pass" ? "pass"
              : verdict.status === "fail" ? "fail" : "unavailable";
    h("div", `banner ${cls}`, stage,
      (verdict.status === "fail" ? "✗ " : verdict.status === "pass" ? "✓ " : "") +
      verdict.message);
  }
  const ctr = h("div", "rcontrols", stage);
  for (const [label, d] of [["⏮", -Infinity], ["◀", -1], ["▶", 1], ["⏭", Infinity]]) {
    const b = h("button", "ctrl", ctr, label);
    b.addEventListener("click", () => stepFrame(d));
  }
  const playBtn = h("button", "ctrl", ctr, "▶ play");
  playBtn.addEventListener("click", () => {
    if (PLAYING) { clearInterval(PLAYING); PLAYING = null; playBtn.textContent = "▶ play"; }
    else {
      playBtn.textContent = "⏸ pause";
      PLAYING = setInterval(() => {
        if (FRAME >= FRAMES.length - 1) { clearInterval(PLAYING); PLAYING = null; playBtn.textContent = "▶ play"; }
        else stepFrame(1);
      }, 700);
    }
  });
  h("span", "fcount", ctr, "");
  h("span", "evname", ctr, "");
  h("span", "pill statuspill hidden", ctr, "");
  if (CUR.modelDiagram) {
    const a = document.createElement("a");
    a.textContent = `model: ${CUR.modelDiagram.model}`;
    a.title = CUR.modelDiagram.path;
    a.style.cssText = "font-size:12px;color:#2a78d6;margin-left:auto;";
    if (CUR.modelDiagram.href) { a.href = CUR.modelDiagram.href; a.target = "_blank"; }
    ctr.appendChild(a);
  }

  const box = h("div", "scenebox", stage);
  if (scene.stores && scene.stores.length) {
    const row = h("div", "srow", box);
    for (const s of scene.stores) {
      const d = h("div", `store ${s.style || ""}`, row);
      d.dataset.store = s.id;
      h("div", "slabel", d, s.label || s.id);
      if (s.listField) h("div", "schips", d);
      else h("div", "sval", d, "");
    }
  }
  if (scene.actors && scene.actors.length) {
    const row = h("div", "srow", box);
    for (const a of scene.actors) {
      const d = h("div", "actor", row, a.label || a.id);
      d.dataset.store = a.id;
      const dots = h("div", "dots", d);
      for (const dotLabel of Object.keys(a.memberOf || {})) {
        const sp = h("span", "", dots);
        const i2 = h("i", "", sp);
        i2.dataset.actor = a.id;
        i2.dataset.dot = dotLabel;
        sp.appendChild(document.createTextNode(dotLabel));
      }
    }
  }
  if (scene.lamps && scene.lamps.length) {
    const row = h("div", "srow", box);
    for (const l of scene.lamps) {
      const sp = h("span", "lamp", row);
      const i2 = h("i", "", sp);
      i2.dataset.lamp = l.field;
      sp.appendChild(document.createTextNode(l.label || l.field));
    }
  }
  h("div", "flowcap", box, "");

  if ((CUR.invariantIds || []).length) {
    const badges = h("div", "invbadges", stage);
    CUR.invariantIds.forEach((id, j) => {
      const b = h("span", "invbadge", badges, id);
      b.dataset.inv = j;
    });
  }
  h("div", "statecard", stage);

  setFrame(verdict && verdict.step != null && verdict.step >= 0
           ? Math.min(verdict.step, FRAMES.length - 1) : 0);
}

function sceneFieldSet() {
  const scene = CUR.scene || {};
  const covered = new Set();
  for (const s of scene.stores || [])
    for (const k of [s.countField, s.listField, s.valueField]) if (k) covered.add(k);
  for (const a of scene.actors || [])
    for (const f of Object.values(a.memberOf || {})) covered.add(f);
  for (const l of scene.lamps || []) covered.add(l.field);
  if (CUR.bandField) covered.add(CUR.bandField);
  return covered;
}

function setFrame(i) {
  FRAME = Math.max(0, Math.min(i, FRAMES.length - 1));
  const ev = FRAMES[FRAME];
  const st = ev.state || {};
  const prev = FRAME > 0 ? (FRAMES[FRAME - 1].state || {}) : {};
  const root = document.getElementById("replay");

  for (const e of root.querySelectorAll(".rentry"))
    e.classList.toggle("active", Number(e.dataset.frame) === FRAME);
  const active = root.querySelector(".rentry.active");
  if (active) active.scrollIntoView({block: "nearest"});

  root.querySelector(".fcount").textContent = `${FRAME + 1} / ${FRAMES.length}`;
  const evname = root.querySelector(".evname");
  evname.textContent = (ev.cat === FAULT ? "⚠ " : "") + ev.name;
  evname.classList.toggle("fault", ev.cat === FAULT);

  // Status pill from the band field + shared phase colors.
  const pill = root.querySelector(".statuspill");
  const status = CUR.bandField ? st[CUR.bandField] : null;
  pill.classList.toggle("hidden", status == null);
  if (status != null) {
    pill.textContent = `${CUR.bandField}: ${status}`;
    const c = CUR.bands[status] ? pick(CUR.bands[status]) : null;
    pill.style.background = c ? c + WASH() : "transparent";
    pill.style.borderColor = c || "var(--baseline)";
  }

  // Stores / actors / lamps.
  const scene = CUR.scene || {};
  for (const s of scene.stores || []) {
    const d = root.querySelector(`.store[data-store="${s.id}"]`);
    if (!d) continue;
    d.classList.remove("pulse");
    if (s.listField) {
      const chips = d.querySelector(".schips");
      chips.replaceChildren();
      for (const item of parseList(st[s.listField]))
        h("span", "", chips, String(item));
      if (String(st[s.listField]) !== String(prev[s.listField])) d.classList.add("pulse");
    } else {
      const field = s.countField || s.valueField;
      const val = st[field];
      const sval = d.querySelector(".sval");
      sval.textContent = val == null ? "–" : String(val);
      if (s.valueField && CUR.bands[val]) {
        const c = pick(CUR.bands[val]);
        sval.style.background = c + WASH();
        sval.style.borderRadius = "999px";
      } else if (s.valueField) {
        sval.style.background = "transparent";
      }
      if (st[field] !== prev[field]) {
        d.classList.add("pulse");
        if (s.countField && typeof val === "number" && typeof prev[field] === "number") {
          const delta = val - prev[field];
          const dspan = h("span", "delta", sval);
          dspan.textContent = (delta > 0 ? "+" : "") + delta;
          dspan.style.color = delta > 0 ? "#0ca30c" : "#d03b3b";
        }
      }
    }
  }
  for (const a of scene.actors || []) {
    const node = root.querySelector(`.actor[data-store="${a.id}"]`);
    if (node) node.classList.remove("pulse");
    for (const [dotLabel, field] of Object.entries(a.memberOf || {})) {
      const dot = root.querySelector(`i[data-actor="${a.id}"][data-dot="${dotLabel}"]`);
      if (dot) dot.classList.toggle("on", parseList(st[field]).includes(a.id));
    }
  }
  for (const l of scene.lamps || []) {
    const dot = root.querySelector(`i[data-lamp="${l.field}"]`);
    if (dot) dot.classList.toggle("on", st[l.field] === true);
  }

  // Flow caption + pulses for flows fired by this event.
  const cap = root.querySelector(".flowcap");
  cap.replaceChildren();
  for (const f of SCENE_FLOWS) {
    if (!f.re.test(ev.name)) continue;
    if (f.when && !Object.entries(f.when).every(([k, v]) => st[k] === v)) continue;
    const from = f.fromField ? st[f.fromField] : f.from;
    const to = f.toField ? st[f.toField] : f.to;
    const line = h("div", "", cap);
    h("span", "", line, `${from} `);
    h("span", "arrow", line, "⟶");
    h("span", "", line, ` ${to}` + (f.label ? ` — ${f.label}` : ""));
    for (const id of [from, to]) {
      const t = root.querySelector(`[data-store="${id}"]`);
      if (t) t.classList.add("pulse");
    }
  }

  root.querySelector(".scenebox").classList.toggle("fault", ev.cat === FAULT);

  // Invariant badges.
  for (const b of root.querySelectorAll(".invbadge")) {
    const v = (ev.inv || [])[Number(b.dataset.inv)];
    b.classList.toggle("ok", v === true);
    b.classList.toggle("bad", v === false);
  }

  // State card: everything the scene doesn't already show.
  const covered = sceneFieldSet();
  const card = root.querySelector(".statecard");
  card.replaceChildren();
  for (const [k, v] of Object.entries(st)) {
    if (covered.has(k)) continue;
    const d = h("div", prev[k] !== v ? "chg" : "", card);
    d.appendChild(document.createTextNode(k + ": "));
    h("b", "", d, String(v));
  }
}

function stepFrame(d) {
  if (d === -Infinity) return setFrame(0);
  if (d === Infinity) return setFrame(FRAMES.length - 1);
  setFrame(FRAME + d);
}

document.addEventListener("keydown", (e) => {
  if (VIEW !== "replay" || !FRAMES.length) return;
  if (e.key === "ArrowRight") { stepFrame(1); e.preventDefault(); }
  if (e.key === "ArrowLeft") { stepFrame(-1); e.preventDefault(); }
});

function syncHeaderHeight() {
  const h2 = document.querySelector("header").offsetHeight;
  document.documentElement.style.setProperty("--header-h", h2 + "px");
}
window.addEventListener("resize", syncHeaderHeight);

buildSidebar();
buildUI(0);
</script>
</body>
</html>
"""


def run_tlc_verdict(trace_path: Path, binding: Binding,
                    specs_dir: Path) -> dict:
    """Optionally validate the trace with TLC via scripts/tla-check.sh and
    distill a verdict for the artifact. Never raises: if TLC/java or the
    runner is unavailable, the render proceeds with a neutral note."""
    if not binding.trace_family:
        return {"status": "unavailable",
                "message": "no traceFamily in this tag's binding"}
    zig_dir = specs_dir.parent.parent
    script = zig_dir.parent / "scripts" / "tla-check.sh"
    # On counterexamples TLC drops *_TTrace_* files next to the spec, which
    # the suite's audit rightly flags; remove any this run creates.
    pre_existing = set(specs_dir.rglob("*_TTrace_*"))
    try:
        with tempfile.TemporaryDirectory(prefix="tla-viz-tlc-") as statedir:
            proc = subprocess.run(
                ["bash", str(script), "trace", binding.trace_family],
                cwd=zig_dir,
                env={**os.environ, "STATEDIR": statedir,
                     "TRACE_FILES": str(trace_path.resolve())},
                capture_output=True, text=True, timeout=600)
            logs = sorted(Path(statedir).glob("*/tlc.log"))
            tlc_log = "\n".join(p.read_text() for p in logs)
    except (OSError, subprocess.TimeoutExpired) as e:
        return {"status": "unavailable", "message": f"TLC unavailable: {e}"}
    finally:
        for leftover in set(specs_dir.rglob("*_TTrace_*")) - pre_existing:
            leftover.unlink(missing_ok=True)

    if proc.returncode == 0:
        return {"status": "pass",
                "message": f"TLC trace validation passed "
                           f"({binding.trace_family})"}

    # Distill the counterexample: which invariant, and how far the replay got
    # (`l` is the Trace specs' 1-based next-log-line index).
    log_text = tlc_log or (proc.stdout + proc.stderr)
    inv = re.search(r"Invariant (\w+) is violated", log_text)
    prop = re.search(r"Temporal property (\w+)", log_text)
    err = re.search(r"^Error: (.+)$", log_text, re.M)
    l_values = [int(x) for x in re.findall(r"/\\ l = (\d+)", log_text)]
    # `l` is the 1-based index of the NEXT log line to consume. An invariant
    # violation happens in the post-state of the last consumed event
    # (0-based: l-2); a TraceMatched failure means line l itself could not
    # be matched to any model action (0-based: l-1).
    if inv:
        name = inv.group(1)
        step = max(l_values) - 2 if l_values else None
        message = f"TLC: invariant {name} violated"
    elif "TraceMatched" in log_text:
        name = "TraceMatched"
        step = max(l_values) - 1 if l_values else None
        message = ("TLC: implementation step not matched by any model "
                   "action (TraceMatched)")
    else:
        name = prop.group(1) if prop else None
        step = max(l_values) - 2 if l_values else None
        message = (f"TLC: property {name} violated" if name else
                   f"TLC validation failed: "
                   f"{err.group(1) if err else 'see TLC log'}")
    if step is not None and step >= 0:
        message += f" at step {step}"
    else:
        step = None
    return {"status": "fail", "invariant": name, "step": step,
            "message": message}


def prepare_trace(path: Path, bindings: dict[str, "Binding"],
                  specs_dir: Path | None, max_events: int,
                  tlc: bool = False) -> dict | None:
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
    verdict = None
    if tlc and specs_dir is not None:
        verdict = run_tlc_verdict(path, binding, specs_dir)
        mark = {"pass": "✓", "fail": "✗"}.get(verdict["status"], "•")
        subtitle += f" {mark} {verdict['message']}."

    model_diagram = None
    if binding.model and specs_dir is not None:
        md = (specs_dir / "diagrams" / f"{binding.model}.md").resolve()
        model_diagram = {"model": binding.model,
                         "path": f"specs/tla/diagrams/{binding.model}.md",
                         "href": md.as_uri() if md.is_file() else None}

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
            "replay": replay_enabled(binding),
            "scene": binding.scene,
            "invariantIds": binding.invariant_ids(),
            "verdict": verdict,
            "modelDiagram": model_diagram,
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
    ap.add_argument("--binding", type=Path, action="append", default=[],
                    help="overlay binding file(s) merged over the base table "
                         "per tag — bind a new trace tag or override display "
                         "of an existing one without editing the repo")
    ap.add_argument("--tlc", action="store_true",
                    help="also run TLC trace validation (scripts/tla-check.sh "
                         "trace <family>) and bake the verdict into the "
                         "artifact; skipped gracefully if TLC is unavailable")
    ap.add_argument("--max-events", type=int, default=20000,
                    help="skip traces with more events than this (default "
                         "20000); pre-split with scripts/tla-segment-*.py")
    args = ap.parse_args()

    files = expand_inputs(args.traces)
    if not files:
        print("no .ndjson files found", file=sys.stderr)
        return 1

    bindings_path = archetypes.find_bindings_file(files[0], args.bindings)
    binding_paths = ([bindings_path] if bindings_path else []) + args.binding
    missing = [p for p in args.binding if not p.is_file()]
    if missing:
        print(f"binding overlay not found: {missing}", file=sys.stderr)
        return 1
    bindings, specs_dir = {}, None
    if binding_paths:
        bindings = archetypes.load_bindings(binding_paths)
    if bindings_path is not None:
        specs_dir = bindings_path.resolve().parent.parent

    pairs = [(f, e) for f in files
             if (e := prepare_trace(f, bindings, specs_dir, args.max_events,
                                    tlc=args.tlc))]
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
