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

"""Visualization archetypes and per-trace-family bindings.

The timeline renderer's useful encodings are domain knowledge (what is an
actor, what deserves a tenure band, which events form causal pairs). Rather
than inferring that per trace, this database ships three preset archetypes and
a checked-in binding table (specs/tla/traces/viz.json) assigning each trace
family to one:

  consensus  symmetric node actors with an id field, a role field for tenure
             bands, a message envelope for arrows, and multi-run segmentation
             (raft).
  dialogue   a small named cast encoded in event names (primary/standby,
             parent/child/metadata); lanes come from name rules and arrows
             from declarative causal pairs matched on event fields.
  narrative  a single actor whose observable state progresses; the narrated
             layout (per-event state diffs) plus an optional status band.
             Also the fallback for unbound tags.

Every archetype shares: up to three family category slots (+ Other), a
reserved fault category (crash/corruption events in status-critical red with
a warning label), and optional model-linked phase colors for bands.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import exprs

# Category slot presentation, fixed across families: three categorical slots,
# a neutral "other", and the reserved fault slot, each as a
# (light-surface, dark-surface) hex pair. Shape is the secondary
# (color-independent) encoding.
CATEGORY_COLORS = [
    ("#2a78d6", "#3987e5"),
    ("#eb6834", "#d95926"),
    ("#1baf7a", "#199e70"),
    ("#898781", "#898781"),
    ("#d03b3b", "#d03b3b"),
]
CATEGORY_SHAPES = ["circle", "diamond", "square", "triangle", "cross"]
OTHER_INDEX, FAULT_INDEX = 3, 4

FAULT_PATTERNS = ["crash", "corrupt", "panic", "orphan", "stale"]


@dataclass
class Binding:
    tag: str
    archetype: str = "narrative"
    label: str = ""
    # Lanes: a field on the event, ordered (regex -> lane) name rules, or a
    # single static lane.
    lane_field: str | None = None
    lane_rules: list[tuple[re.Pattern, str]] = field(default_factory=list)
    lane_static: str | None = None
    lane_order: list[str] = field(default_factory=list)
    lane_prefix: str = ""
    # Tenure bands. band_global: the field is global protocol state (one
    # full-width strip) rather than per-actor state (per-lane wash).
    band_field: str | None = None
    band_global: bool = False
    band_quiet: list[str] = field(default_factory=list)
    band_colors: dict[str, str] = field(default_factory=dict)
    model: str | None = None
    phase_var: str | None = None
    # Arrows.
    msg_arrows: bool = False
    pairs: list[dict] = field(default_factory=list)
    # Categories / labels.
    categories: list[dict] = field(default_factory=list)  # {label, patterns}
    fault_patterns: list[str] = field(default_factory=list)
    milestones: list[str] = field(default_factory=list)
    segmented: bool = False
    bound: bool = True
    # Replay scene (stores/actors/lamps/flows), display invariants, and the
    # tla-check.sh trace family for the optional TLC verdict overlay.
    scene: dict | None = None
    invariants: list[dict] = field(default_factory=list)  # {id, expr}
    trace_family: str | None = None

    def __post_init__(self):
        self._cat_res = [
            [re.compile(p, re.I) for p in c.get("patterns", [])]
            for c in self.categories[:3]
        ]
        self._fault_re = [re.compile(p, re.I)
                          for p in FAULT_PATTERNS + self.fault_patterns]
        self._milestone_re = [re.compile(p) for p in self.milestones]
        # Compile display invariants; a malformed expression is reported and
        # dropped rather than breaking the render.
        self._invariants = []
        for inv in self.invariants:
            try:
                self._invariants.append(
                    {"id": inv["id"], "expr": exprs.Expr(inv["expr"])})
            except (ValueError, KeyError) as e:
                print(f"warning: dropping invariant {inv!r}: {e}",
                      file=sys.stderr)

    def eval_invariants(self, env: dict) -> list[bool | None]:
        return [inv["expr"].evaluate(env) for inv in self._invariants]

    def invariant_ids(self) -> list[str]:
        return [inv["id"] for inv in self._invariants]

    def categorize(self, name: str) -> int:
        if any(r.search(name) for r in self._fault_re):
            return FAULT_INDEX
        for i, regexes in enumerate(self._cat_res):
            if any(r.search(name) for r in regexes):
                return i
        return OTHER_INDEX

    def lane_for(self, name: str, flat: dict) -> str:
        if self.lane_field is not None:
            val = flat.get(self.lane_field)
            if val is not None:
                return str(val)
        for pattern, lane in self.lane_rules:
            if pattern.search(name):
                return lane
        return self.lane_static or "events"

    def is_milestone(self, name: str) -> bool:
        return any(r.search(name) for r in self._milestone_re)

    def category_legend(self) -> list[dict]:
        """Always exactly five slots — categorize() returns fixed indices
        (0-2 family, 3 Other, 4 Faults), so unused family slots are padded
        (they never render: the UI only shows categories that occur)."""
        labels = [c["label"] for c in self.categories[:3]]
        labels += [f"category {i + 1}" for i in range(len(labels), 3)]
        labels += ["Other", "Faults"]
        return [{"label": label, "color": CATEGORY_COLORS[i],
                 "shape": CATEGORY_SHAPES[i]}
                for i, label in enumerate(labels)]


def _parse(tag: str, raw: dict) -> Binding:
    return Binding(
        tag=tag,
        archetype=raw.get("archetype", "narrative"),
        label=raw.get("label", tag),
        lane_field=raw.get("laneField"),
        lane_rules=[(re.compile(p), lane) for p, lane in raw.get("laneRules", [])],
        lane_static=raw.get("lane"),
        lane_order=raw.get("laneOrder", []),
        lane_prefix=raw.get("lanePrefix", ""),
        band_field=raw.get("bandField"),
        band_global=raw.get("bandGlobal", False),
        band_quiet=raw.get("bandQuiet", []),
        band_colors=raw.get("bandColors", {}),
        model=raw.get("model"),
        phase_var=raw.get("phaseVar"),
        msg_arrows=raw.get("msgArrows", False),
        pairs=raw.get("pairs", []),
        categories=raw.get("categories", []),
        fault_patterns=raw.get("faultPatterns", []),
        milestones=raw.get("milestones", []),
        segmented=raw.get("segmented", False),
        scene=raw.get("scene"),
        invariants=raw.get("invariants", []),
        trace_family=raw.get("traceFamily"),
    )


def load_bindings(paths: list[Path]) -> dict[str, Binding]:
    """Load the base binding table plus overlay files. Later files win per
    tag at the top-level-key granularity, so an agent can bind a brand-new
    trace tag (or override one field of an existing one) without touching
    the checked-in table."""
    merged: dict[str, dict] = {}
    for path in paths:
        raw = json.loads(path.read_text())
        for tag, entry in raw.items():
            if not isinstance(entry, dict):
                continue
            merged[tag] = {**merged.get(tag, {}), **entry}
    return {tag: _parse(tag, entry) for tag, entry in merged.items()}


def fallback(tag: str) -> Binding:
    """Unbound trace families still render: narrative archetype, no vocab."""
    b = Binding(tag=tag, label=tag.removesuffix("-trace"))
    b.bound = False
    return b


def find_bindings_file(trace_path: Path, explicit: Path | None) -> Path | None:
    if explicit is not None:
        return explicit if explicit.is_file() else None
    candidates = [
        trace_path.parent / "viz.json",
        trace_path.parent / "traces" / "viz.json",
        Path("specs/tla/traces/viz.json"),
        Path("zig/specs/tla/traces/viz.json"),
    ]
    return next((c for c in candidates if c.is_file()), None)


def match_pairs(events: list[dict], pairs: list[dict]) -> list[dict]:
    """Declarative causal arrows: FIFO-match `from` events to `to` events on
    equal values of the named keys (e.g. PrimaryAppend -> StandbyReceive on
    timeline+lsn). When the two sides name the value differently
    (dbDeltaSeq vs dbReplaySeq), use fromKeys/toKeys."""
    arrows = []
    for rule in pairs:
        from_keys = rule.get("fromKeys", rule.get("keys", []))
        to_keys = rule.get("toKeys", rule.get("keys", []))
        pending: dict[tuple, list[int]] = {}
        for i, ev in enumerate(events):
            if ev["name"] == rule["from"]:
                key = tuple(ev["flat"].get(k) for k in from_keys)
                pending.setdefault(key, []).append(i)
            elif ev["name"] == rule["to"]:
                key = tuple(ev["flat"].get(k) for k in to_keys)
                queue = pending.get(key)
                if queue:
                    arrows.append({"from": queue.pop(0), "to": i,
                                   "cat": ev["cat"]})
    return sorted(arrows, key=lambda a: a["from"])
