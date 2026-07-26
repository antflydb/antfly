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

Input is the trace format emitted by the Zig tracing writers (and the older Go
harness): one JSON object per line with tag "trace" (raft events: nid, state,
role, msg) or "antfly-trace" (transaction events: txnId, shardId, state).
Non-JSON line prefixes (logger timestamps) are stripped, matching
scripts/tla-validate-trace.sh.

Output is one HTML file with no external dependencies: one swimlane per node
(or shard), one row per event, role shown as lane background bands, matched
Send/Receive message arrows, hover tooltips, category/node filters, and a
table view. Multi-run raft traces are split into segments using the same
boundary rules as scripts/tla-segment-raft-trace.py.

Usage:
  render_trace.py trace.ndjson [-o out.html] [--max-events N]
"""

from __future__ import annotations

import argparse
import html
import json
import sys
from pathlib import Path

BOOTSTRAP_EVENTS = {"InitState", "BecomeFollower", "ApplyConfChange"}

# Event categories. Slot colors come from the validated reference palette
# (first three categorical slots are all-pairs safe; "other" is neutral ink)
# and each category also carries a distinct mark shape as secondary encoding.
CATEGORIES = [
    ("replication", "Replication"),
    ("election", "Election"),
    ("apply", "Commit / apply"),
    ("other", "Other"),
]


def categorize(name: str) -> str:
    n = name.lower()
    if "appendentries" in n or "snapshot" in n or "heartbeat" in n:
        return "replication"
    if "vote" in n or "become" in n or "timeout" in n or "campaign" in n:
        return "election"
    if any(k in n for k in ("commit", "replicate", "ready", "confchange",
                            "changeconf", "apply", "abort", "intent", "resolve")):
        return "apply"
    return "other"


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
            # "trace" is the raft trace; the rest of the suite's writers use
            # <subsystem>-trace tags (antfly-trace, ha-trace, ...).
            if (tag == "trace" or tag.endswith("-trace")) \
                    and isinstance(obj.get("event"), dict):
                yield obj


def is_state_reset(seg_events: list[dict], ev: dict) -> bool:
    """Same rule as tla-segment-raft-trace.py: a node's commit/log dropping
    back to zero marks a re-initialized engine (new test run)."""
    if ev.get("state", {}).get("commit", 0) != 0 or ev.get("log", 0) != 0:
        return False
    for prev in reversed(seg_events):
        if prev["lane"] == ev["nid"]:
            if prev.get("commit", 0) > 0 or prev.get("log", 0) > 0:
                return True
            break
    return False


def flatten_event(ev: dict) -> dict:
    """Scalar view of an event's state for per-lane diffing. Nested dicts get
    dotted keys; lists are inlined when short. The message payload is excluded
    (it changes every event and is shown in the tooltip/table instead)."""
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


def build_segments(objs) -> list[list[dict]]:
    """Flatten trace objects into per-segment event records."""
    segments: list[list[dict]] = []
    current: list[dict] = []
    init_nodes: set[str] = set()
    lane_state: dict[str, dict] = {}

    for obj in objs:
        ev = obj["event"]
        name = ev.get("name", "?")
        if obj["tag"] == "trace":
            lane = str(ev.get("nid", "?"))
        else:
            lane = str(ev.get("nid") or ev.get("shardId") or ev.get("txnId")
                       or ev.get("sessionId") or "events")

        if obj["tag"] == "trace":
            new_run = (name == "InitState" and lane in init_nodes) or (
                name not in BOOTSTRAP_EVENTS and current and is_state_reset(current, ev)
            )
            if new_run:
                segments.append(current)
                current, init_nodes, lane_state = [], set(), {}
            if name == "InitState":
                init_nodes.add(lane)

        # What changed in this lane's observable state, for direct labels.
        snap = flatten_event(ev)
        prev = lane_state.get(lane)
        if prev is None:
            changes = [f"{k}={fmt(v)}" for k, v in snap.items()][:6]
        else:
            changes = [f"{k}: {fmt(prev[k])}→{fmt(v)}"
                       for k, v in snap.items() if k in prev and prev[k] != v]
            changes += [f"{k}={fmt(v)}" for k, v in snap.items() if k not in prev]
        lane_state[lane] = {**(prev or {}), **snap}

        state = ev.get("state") or {}
        rec = {
            "lane": lane,
            "name": name,
            "cat": categorize(name),
            "role": ev.get("role"),
            "term": state.get("term"),
            "commit": state.get("commit"),
            "log": ev.get("log"),
            "msg": ev.get("msg"),
            "txn": ev.get("txnId"),
            "changes": changes,
            "detail": json.dumps(ev, indent=1),
        }
        current.append(rec)
    if current:
        segments.append(current)
    return segments


def match_arrows(events: list[dict]) -> list[dict]:
    """Pair SendX events with the ReceiveX that consumes the same message."""
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


def lane_order(events: list[dict]) -> list[str]:
    lanes = list(dict.fromkeys(ev["lane"] for ev in events))
    try:
        return sorted(lanes, key=int)
    except ValueError:
        return sorted(lanes)


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
    --cat-replication: #2a78d6; --cat-election: #eb6834;
    --cat-apply: #1baf7a; --cat-other: #898781;
    --band-leader: rgba(42,120,214,0.12); --band-candidate: rgba(235,104,52,0.14);
    --band-precandidate: rgba(235,104,52,0.07);
  }
  @media (prefers-color-scheme: dark) {
    :root:where(:not([data-theme="light"])) .viz-root {
      color-scheme: dark;
      --surface-1: #1a1a19; --page: #0d0d0d;
      --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #898781;
      --grid: #2c2c2a; --baseline: #383835; --border: rgba(255,255,255,0.10);
      --cat-replication: #3987e5; --cat-election: #d95926;
      --cat-apply: #199e70; --cat-other: #898781;
      --band-leader: rgba(57,135,229,0.18); --band-candidate: rgba(217,89,38,0.20);
      --band-precandidate: rgba(217,89,38,0.10);
    }
  }
  :root[data-theme="dark"] .viz-root {
    color-scheme: dark;
    --surface-1: #1a1a19; --page: #0d0d0d;
    --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #898781;
    --grid: #2c2c2a; --baseline: #383835; --border: rgba(255,255,255,0.10);
    --cat-replication: #3987e5; --cat-election: #d95926;
    --cat-apply: #199e70; --cat-other: #898781;
    --band-leader: rgba(57,135,229,0.18); --band-candidate: rgba(217,89,38,0.20);
    --band-precandidate: rgba(217,89,38,0.10);
  }
  body.viz-root {
    margin: 0; background: var(--page); color: var(--text-primary);
    font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif;
  }
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
<header>
  <h1>__TITLE__</h1>
  <div class="sub">__SUBTITLE__</div>
  <div class="controls">
    <div class="group" id="cat-filters"></div>
    <div class="group" id="lane-filters"></div>
    <div class="group"><button class="toggleview" id="viewbtn">Table view</button></div>
  </div>
</header>
<div id="charts"></div>
<div id="tooltip"></div>
<script>
const DATA = __DATA__;

// Small traces get a "narrated" layout: taller rows, every event labeled
// with its name and the state fields it changed.
const SPARSE = DATA.sparse;
const ROW_H = SPARSE ? 24 : 12, LANE_W = SPARSE ? 240 : 130,
      GUTTER = 46, TOP_PAD = 10;
const CAT_COLOR = {
  replication: "var(--cat-replication)",
  election: "var(--cat-election)",
  apply: "var(--cat-apply)",
  other: "var(--cat-other)",
};
const BAND = {
  StateLeader: "var(--band-leader)",
  StateCandidate: "var(--band-candidate)",
  StatePreCandidate: "var(--band-precandidate)",
};
const SVG = "http://www.w3.org/2000/svg";

function el(tag, attrs, parent) {
  const node = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  if (parent) parent.appendChild(node);
  return node;
}

// Mark shapes: shape is the secondary (color-independent) encoding.
function drawMark(parent, cat, x, y) {
  const c = CAT_COLOR[cat];
  if (cat === "replication") {
    return el("circle", {cx: x, cy: y, r: 3.5, fill: c, class: "mark"}, parent);
  }
  if (cat === "election") {  // diamond
    const d = 4.6;
    return el("path", {d: `M ${x} ${y - d} L ${x + d} ${y} L ${x} ${y + d} L ${x - d} ${y} Z`,
                       fill: c, class: "mark"}, parent);
  }
  if (cat === "apply") {  // square
    return el("rect", {x: x - 3.2, y: y - 3.2, width: 6.4, height: 6.4,
                       fill: c, class: "mark"}, parent);
  }
  const d = 4.4;  // triangle
  return el("path", {d: `M ${x} ${y - d} L ${x + d} ${y + d} L ${x - d} ${y + d} Z`,
                     fill: c, class: "mark"}, parent);
}

const state = {
  cats: new Set(Object.keys(CAT_COLOR)),
  lanes: new Set(DATA.segments.flatMap(s => s.lanes)),
  table: false,
};

function laneX(seg, lane) { return GUTTER + seg.lanes.indexOf(lane) * LANE_W + LANE_W / 2; }

function renderSegment(seg, idx) {
  const wrap = document.createElement("section");
  const events = seg.events;
  const h = TOP_PAD + events.length * ROW_H + 20;
  const w = GUTTER + seg.lanes.length * LANE_W + (SPARSE ? 460 : 10);

  const title = document.createElement("h2");
  title.textContent = DATA.segments.length > 1
    ? `Segment ${idx + 1} — ${events.length} events, ${seg.lanes.length} lanes`
    : `${events.length} events, ${seg.lanes.length} lanes`;
  wrap.appendChild(title);

  const heads = document.createElement("div");
  heads.className = "laneheads";
  heads.style.width = (w - GUTTER) + "px";
  for (const lane of seg.lanes) {
    const d = document.createElement("div");
    d.style.width = LANE_W + "px";
    d.textContent = DATA.kind === "trace" ? `node ${lane}` : lane;
    d.dataset.lane = lane;
    heads.appendChild(d);
  }
  wrap.appendChild(heads);

  const svg = el("svg", {class: "timeline", width: w, height: h,
                         viewBox: `0 0 ${w} ${h}`});
  wrap.appendChild(svg);

  // Role bands: contiguous spans of each lane's role.
  const bands = el("g", {}, svg);
  for (const lane of seg.lanes) {
    let start = null, role = null;
    const flush = (endIdx) => {
      if (role && BAND[role]) {
        el("rect", {x: laneX(seg, lane) - LANE_W / 2 + 4, y: TOP_PAD + start * ROW_H - 5,
                    width: LANE_W - 8, height: (endIdx - start) * ROW_H,
                    fill: BAND[role], "data-lane": lane}, bands);
      }
    };
    events.forEach((ev, i) => {
      if (ev.lane !== lane || ev.role === role) return;
      if (start !== null) flush(i);
      start = i; role = ev.role;
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

  // Message arrows.
  const defs = el("defs", {}, svg);
  for (const [cat, color] of Object.entries(CAT_COLOR)) {
    const m = el("marker", {id: `arr-${idx}-${cat}`, viewBox: "0 0 8 8",
                            refX: 7, refY: 4, markerWidth: 6, markerHeight: 6,
                            orient: "auto-start-reverse"}, defs);
    el("path", {d: "M 0 0 L 8 4 L 0 8 Z", fill: color}, m);
  }
  const arrowsG = el("g", {}, svg);
  for (const a of seg.arrows) {
    const evA = events[a.from], evB = events[a.to];
    el("line", {x1: laneX(seg, evA.lane), y1: TOP_PAD + a.from * ROW_H,
                x2: laneX(seg, evB.lane), y2: TOP_PAD + a.to * ROW_H,
                stroke: CAT_COLOR[a.cat], class: "arrow",
                "marker-end": `url(#arr-${idx}-${a.cat})`,
                "data-cat": a.cat,
                "data-lanes": evA.lane + "," + evB.lane}, arrowsG);
  }

  // Event marks + selective direct labels (role transitions only).
  const marksG = el("g", {}, svg);
  const tooltip = document.getElementById("tooltip");
  events.forEach((ev, i) => {
    const x = laneX(seg, ev.lane), y = TOP_PAD + i * ROW_H;
    const g = el("g", {"data-cat": ev.cat, "data-lane": ev.lane}, marksG);
    drawMark(g, ev.cat, x, y);
    if (SPARSE) {
      el("text", {x: x + 10, y: y, class: "evlabel"}, g).textContent = ev.name;
      if (ev.changes && ev.changes.length) {
        const maxCh = seg.lanes.length > 1 ? 42 : 120;
        const txt = ev.changes.join("   ");
        el("text", {x: x + 10, y: y + 11, class: "chlabel"}, g).textContent =
          txt.length > maxCh ? txt.slice(0, maxCh - 1) + "…" : txt;
      }
    } else if (ev.name.startsWith("Become") || ev.name === "InitState") {
      el("text", {x: x + 8, y: y + 3, class: "evlabel"}, g).textContent =
        `${ev.name} (t${ev.term ?? "?"})`;
    }
    const hit = el("circle", {cx: x, cy: y, r: 8, class: "hit"}, g);
    hit.addEventListener("mouseenter", (e) => {
      tooltip.textContent = `#${i} ${ev.name} — ${DATA.kind === "trace" ? "node " : ""}${ev.lane}\n` + ev.detail;
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
  const table = document.createElement("table");
  table.className = "events hidden";
  table.innerHTML = "<thead><tr><th>#</th><th>" +
    (DATA.kind === "trace" ? "Node" : "Lane") +
    "</th><th>Event</th><th>Role</th><th>Term</th><th>Commit</th><th>Log</th><th>Message</th></tr></thead>";
  const tbody = document.createElement("tbody");
  events.forEach((ev, i) => {
    const tr = document.createElement("tr");
    tr.dataset.cat = ev.cat; tr.dataset.lane = ev.lane;
    const msg = ev.msg ? `${ev.msg.type} ${ev.msg.from}→${ev.msg.to}` : "";
    for (const v of [i, ev.lane, ev.name, ev.role ?? "", ev.term ?? "",
                     ev.commit ?? "", ev.log ?? "", msg]) {
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
    const catOK = !node.dataset.cat || state.cats.has(node.dataset.cat);
    let laneOK = true;
    if (node.dataset.lane) laneOK = state.lanes.has(node.dataset.lane);
    if (node.dataset.lanes) {
      laneOK = node.dataset.lanes.split(",").every(l => state.lanes.has(l));
    }
    node.classList.toggle("hidden", !(catOK && laneOK));
  }
}

function chip(parent, label, swatchColor, isOn, onToggle) {
  const lab = document.createElement("label");
  lab.className = "chip" + (isOn ? "" : " off");
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

const catNames = {replication: "Replication ●", election: "Election ◆",
                  apply: "Commit/apply ■", other: "Other ▲"};
const catBox = document.getElementById("cat-filters");
for (const cat of Object.keys(CAT_COLOR)) {
  chip(catBox, catNames[cat], CAT_COLOR[cat], true,
       on => on ? state.cats.add(cat) : state.cats.delete(cat));
}
const laneBox = document.getElementById("lane-filters");
const allLanes = [...state.lanes];
if (allLanes.length <= 12) {
  for (const lane of allLanes) {
    chip(laneBox, (DATA.kind === "trace" ? "node " : "") + lane, null, true,
         on => on ? state.lanes.add(lane) : state.lanes.delete(lane));
  }
}

const charts = document.getElementById("charts");
DATA.segments.forEach((seg, i) => charts.appendChild(renderSegment(seg, i)));

function syncHeaderHeight() {
  const h = document.querySelector("header").offsetHeight;
  document.documentElement.style.setProperty("--header-h", h + "px");
}
syncHeaderHeight();
window.addEventListener("resize", syncHeaderHeight);

document.getElementById("viewbtn").addEventListener("click", (e) => {
  state.table = !state.table;
  e.target.textContent = state.table ? "Timeline view" : "Table view";
  for (const svg of document.querySelectorAll("svg.timeline, .laneheads"))
    svg.classList.toggle("hidden", state.table);
  for (const t of document.querySelectorAll("table.events"))
    t.classList.toggle("hidden", !state.table);
});
</script>
</body>
</html>
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("trace", type=Path, help="ndjson trace file")
    ap.add_argument("-o", "--output", type=Path,
                    help="output HTML path (default: <trace>.html)")
    ap.add_argument("--max-events", type=int, default=20000,
                    help="refuse to render more events than this (default 20000); "
                         "pre-split large traces with scripts/tla-segment-*.py")
    args = ap.parse_args()

    objs = list(parse_lines(args.trace))
    if not objs:
        print(f"{args.trace}: no trace events found", file=sys.stderr)
        return 1
    if len(objs) > args.max_events:
        print(f"{args.trace}: {len(objs)} events exceeds --max-events="
              f"{args.max_events}; segment the trace first "
              "(scripts/tla-segment-raft-trace.py)", file=sys.stderr)
        return 1

    kind = objs[0]["tag"]
    segments = build_segments(objs)
    data = {
        "kind": kind,
        "sparse": len(objs) <= 200,
        "segments": [
            {
                "lanes": lane_order(events),
                "events": events,
                "arrows": match_arrows(events),
            }
            for events in segments
        ],
    }

    title = f"TLA+ trace — {args.trace.name}"
    subtitle = (
        f"{sum(len(s) for s in segments)} events, {len(segments)} segment(s), "
        f"{'raft' if kind == 'trace' else kind.removesuffix('-trace')} trace. "
        "Rows are event order (top to bottom), lanes are "
        + ("nodes; lane tint marks leader (blue) / candidate (orange) tenure; "
           "arrows are matched send→receive messages."
           if kind == "trace" else "shards / sessions.")
    )
    out = args.output or args.trace.with_suffix(".html")
    page = (HTML_TEMPLATE
            .replace("__TITLE__", html.escape(title))
            .replace("__SUBTITLE__", html.escape(subtitle))
            .replace("__DATA__", json.dumps(data).replace("</", "<\\/")))
    out.write_text(page)
    print(f"wrote {out} ({out.stat().st_size // 1024} KiB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
