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

"""Generate structural Mermaid diagrams from TLA+ specifications.

For each protocol spec this emits a markdown file containing:

  1. Phase state machines: one stateDiagram-v2 per variable whose domain is a
     finite set of strings (detected from TypeOK-style membership conjuncts),
     with transitions extracted from action guards (var = "x", var \\in {..})
     and updates (var' = "y", var' = [var EXCEPT ![k] = "y"]).
  2. An action/variable table: which state variables each Next-action reads
     (including reads through helper operators) and writes.
  3. An action -> variable write graph (flowchart), with dotted edges for the
     variables an action's own guards read directly.

Extraction is static and heuristic: it relies on the disciplined action style
used in these specs (conjunct lists, explicit UNCHANGED tuples). Transitions
whose source state cannot be determined statically are listed as notes below
the diagram rather than drawn as edges.

Usage:
  gen_structural.py --specs-dir zig/specs/tla --out-dir zig/specs/tla/diagrams
  gen_structural.py --check ...   # exit 1 if committed diagrams are stale
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import warnings
from pathlib import Path

import tree_sitter_tlaplus
from tree_sitter import Language, Node, Parser

import phasecolors

GENERATED_NOTE = (
    "<!-- GENERATED FILE: do not edit. "
    "Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->"
)

# Harness modules (model-checking configs, trace validation) are not protocol
# specs and produce no useful structure.
SKIP_MODULE_RE = re.compile(r"^(MC|Trace)|MC$")

MERMAID_STATE_RESERVED = {"state", "end", "fork", "join", "choice", "note",
                          "direction", "default", "as", "class", "classdef"}


def parse_module(path: Path) -> Node:
    with warnings.catch_warnings():
        # tree-sitter-tlaplus 1.5 still hands an int capsule to Language().
        warnings.simplefilter("ignore", DeprecationWarning)
        parser = Parser(Language(tree_sitter_tlaplus.language()))
    tree = parser.parse(path.read_bytes())
    for child in tree.root_node.children:
        if child.type == "module":
            return child
    raise ValueError(f"{path}: no module node found")


def node_text(node: Node) -> str:
    return node.text.decode()


def string_value(node: Node) -> str:
    return node_text(node)[1:-1]


def iter_tree(node: Node):
    """Yield node and all its descendants."""
    stack = [node]
    while stack:
        n = stack.pop()
        yield n
        stack.extend(reversed(n.children))


class Spec:
    """Structural facts extracted from one TLA+ module."""

    def __init__(self, path: Path):
        self.path = path
        module = parse_module(path)
        self.name = ""
        self.variables: list[str] = []
        self.constants: list[str] = []
        self.defs: dict[str, Node] = {}  # operator name -> body node
        self._usage_cache: dict[str, tuple[frozenset, frozenset]] = {}

        for child in module.children:
            if child.type == "identifier" and not self.name:
                self.name = node_text(child)
            elif child.type == "variable_declaration":
                self.variables += [node_text(c) for c in child.children if c.type == "identifier"]
            elif child.type == "constant_declaration":
                for c in child.children:
                    if c.type == "identifier":
                        self.constants.append(node_text(c))
                    elif c.type == "operator_declaration":  # CONSTANT Op(_, _)
                        ident = c.child(0)
                        if ident is not None and ident.type == "identifier":
                            self.constants.append(node_text(ident))
            elif child.type == "operator_definition":
                ident = next((c for c in child.children if c.type == "identifier"), None)
                if ident is not None and child.children:
                    self.defs[node_text(ident)] = child.children[-1]

        # Display name: prefer the file stem when the module name is
        # uninformative (occ-2pc.tla's module is literally named "model").
        self.display_name = (self.name if self.name.lower() == path.stem.lower()
                             else path.stem)
        self.var_set = set(self.variables)
        self.actions = self._next_actions()
        # Expected-failure mutants (this suite's convention: actions gated by
        # a Buggy* constant) are not part of the protocol; keep them out of
        # pedagogical diagrams but report how many were dropped.
        mutants = [a for a in self.actions if self._is_mutant(a)]
        self.mutant_actions = mutants
        self.actions = [a for a in self.actions if a not in mutants]
        self.phase_domains = self._phase_domains()

    def _is_mutant(self, name: str) -> bool:
        """An expected-failure mutant action: named Buggy*, or enabled by a
        bare Buggy* flag as a top-level conjunct (/\\ BuggySomething). Actions
        that merely branch on a Buggy flag internally (IF Buggy... THEN) are
        real protocol actions and are kept."""
        if name.startswith("Buggy"):
            return True
        return any(
            item.type == "identifier_ref" and node_text(item).startswith("Buggy")
            for item in self._conjunct_spine(self.defs[name])
        )

    # ---- Next decomposition -------------------------------------------------

    def _next_actions(self) -> list[str]:
        """Action names referenced from Next, in source order. Operators whose
        body is itself just a disjunction of / reference to other operators
        (e.g. etcdraft's NextAsync) are expanded rather than listed."""
        body = self.defs.get("Next")
        if body is None:
            return []
        actions: list[str] = []
        expanding: set[str] = {"Next"}

        def is_composite(node: Node) -> bool:
            """A pure dispatch body: disjunctions/quantifiers/refs, no conjuncts."""
            if node.type in ("identifier_ref", "bound_op", "disj_list"):
                return True
            if node.type == "bounded_quantification":
                return is_composite(node.children[-1])
            if node.type == "parentheses" and node.named_child_count == 1:
                return is_composite(node.named_children[0])
            if node.type == "bound_infix_op" and any(c.type == "lor" for c in node.children):
                return True
            return False

        def visit(node: Node):
            if node.type in ("identifier_ref", "bound_op"):
                ref = node if node.type == "identifier_ref" else node.child(0)
                name = node_text(ref) if ref is not None else ""
                if name not in self.defs or name in expanding:
                    return
                target = self.defs[name]
                if is_composite(target):
                    expanding.add(name)
                    visit(target)
                elif name not in actions:
                    actions.append(name)
                return
            if node.type == "bounded_quantification":
                seen_colon = False
                for c in node.children:
                    if c.type == ":":
                        seen_colon = True
                    elif seen_colon:
                        visit(c)
                return
            if node.type in ("disj_list", "disj_item", "conj_list", "conj_item",
                             "parentheses", "if_then_else"):
                for c in node.children:
                    visit(c)
                return
            # Infix \//\ (Next == Tick \/ Ready) without a bulleted list.
            if node.type == "bound_infix_op" and any(
                    c.type in ("lor", "land") for c in node.children):
                for c in node.children:
                    visit(c)

        visit(body)
        return actions

    # ---- variable usage -----------------------------------------------------

    def _is_unchanged(self, node: Node) -> bool:
        return node.type == "bound_prefix_op" \
            and any(c.type == "unchanged" for c in node.children)

    def _is_var_tuple_op(self, name: str) -> bool:
        """True for operators like `vars == <<x, y, z>>` that exist only to be
        used in UNCHANGED / [Next]_vars and must not count as reads."""
        body = self.defs.get(name)
        if body is None or body.type != "tuple_literal":
            return False
        refs = [c for c in body.named_children if c.type == "identifier_ref"]
        return bool(refs) and all(node_text(c) in self.var_set for c in refs)

    def _walk_usage(self, node: Node, reads: set, writes: set,
                    expand: bool, stack: frozenset):
        """Collect variable reads/writes below `node`, pruning UNCHANGED
        subtrees and treating primed occurrences as writes."""
        if self._is_unchanged(node):
            return
        if node.type == "bound_postfix_op" and any(c.type == "prime" for c in node.children):
            for ref in iter_tree(node):
                if ref.type == "identifier_ref" and node_text(ref) in self.var_set:
                    writes.add(node_text(ref))
            return
        if node.type == "identifier_ref":
            name = node_text(node)
            if name in self.var_set:
                reads.add(name)
            elif expand and name in self.defs and name not in stack \
                    and not self._is_var_tuple_op(name):
                r, w = self.op_usage(name, stack)
                reads |= r
                writes |= w
            return
        for child in node.children:
            self._walk_usage(child, reads, writes, expand, stack)

    def op_usage(self, name: str, stack: frozenset = frozenset()) -> tuple[frozenset, frozenset]:
        """(reads, writes) of state variables for operator `name`, expanding
        references to other defined operators."""
        if name in self._usage_cache:
            return self._usage_cache[name]
        if name in stack or name not in self.defs:
            return frozenset(), frozenset()
        reads: set[str] = set()
        writes: set[str] = set()
        self._walk_usage(self.defs[name], reads, writes, True, stack | {name})
        result = (frozenset(reads), frozenset(writes))
        self._usage_cache[name] = result
        return result

    def direct_guard_reads(self, name: str) -> set[str]:
        """State variables read directly in the operator's own body (no helper
        expansion)."""
        reads: set[str] = set()
        writes: set[str] = set()
        self._walk_usage(self.defs[name], reads, writes, False, frozenset())
        return reads

    # ---- phase domains ------------------------------------------------------

    def _resolve_string_set(self, node: Node, depth: int = 0) -> list[str] | None:
        """Resolve a node to a finite set of string values, following one level
        of named-operator indirection (CacheLocs == {"Absent", ...})."""
        if depth > 3:
            return None
        if node.type == "finite_set_literal":
            values = []
            for c in node.children:
                if c.type == "string":
                    values.append(string_value(c))
                elif c.type not in ("{", "}", ","):
                    return None
            return values or None
        if node.type == "identifier_ref":
            body = self.defs.get(node_text(node))
            if body is not None:
                return self._resolve_string_set(body, depth + 1)
        if node.type == "parentheses" and node.named_child_count == 1:
            return self._resolve_string_set(node.named_children[0], depth + 1)
        return None

    def _phase_domains(self) -> dict[str, list[str]]:
        """var -> ordered string domain, from TypeOK-style membership conjuncts."""
        domains: dict[str, list[str]] = {}
        type_ops = [n for n in self.defs if n.startswith("Type")] or list(self.defs)
        for op in type_ops:
            for n in iter_tree(self.defs[op]):
                if n.type != "bound_infix_op" or not any(c.type == "in" for c in n.children):
                    continue
                lhs, rhs = n.children[0], n.children[-1]
                var = self._membership_var(lhs)
                if var is None:
                    continue
                if rhs.type == "set_of_functions":
                    rhs = rhs.children[-2]  # codomain
                values = self._resolve_string_set(rhs)
                if values:
                    domains.setdefault(var, values)
        return domains

    def _membership_var(self, lhs: Node) -> str | None:
        if lhs.type == "identifier_ref" and node_text(lhs) in self.var_set:
            return node_text(lhs)
        if lhs.type == "function_evaluation":
            ref = lhs.child(0)
            if ref is not None and ref.type == "identifier_ref" \
                    and node_text(ref) in self.var_set:
                return node_text(ref)
        return None

    # ---- transitions --------------------------------------------------------

    def _conjunct_spine(self, body: Node) -> list[Node]:
        """Flatten the top-level conjunction of an action, looking through
        quantifiers and parentheses but stopping at disjunctions."""
        spine: list[Node] = []

        def visit(node: Node):
            if node.type in ("conj_list",):
                for c in node.children:
                    if c.type == "conj_item":
                        for cc in c.children:
                            if cc.type != "bullet_conj":
                                visit(cc)
                return
            if node.type == "bounded_quantification":
                seen_colon = False
                for c in node.children:
                    if c.type == ":":
                        seen_colon = True
                    elif seen_colon:
                        visit(c)
                return
            if node.type == "parentheses":
                for c in node.named_children:
                    visit(c)
                return
            spine.append(node)

        visit(body)
        return spine

    def transitions(self, phase_var: str) -> tuple[list[tuple[str, str, str]], list[tuple[str, str]]]:
        """For one phase variable, extract (source, target, action) edges plus
        (action, target) notes where the source state is not statically known."""
        domain = set(self.phase_domains[phase_var])
        edges: list[tuple[str, str, str]] = []
        notes: list[tuple[str, str]] = []

        for action in self.actions:
            body = self.defs[action]
            sources: set[str] = set()
            targets: set[str] = set()

            for item in self._conjunct_spine(body):
                if item.type != "bound_infix_op":
                    continue
                lhs, rhs = item.children[0], item.children[-1]
                is_eq = any(c.type == "eq" for c in item.children)
                is_in = any(c.type == "in" for c in item.children)
                if self._membership_var(lhs) == phase_var and lhs.type != "bound_postfix_op":
                    if is_eq and rhs.type == "string":
                        sources.add(string_value(rhs))
                    elif is_in:
                        values = self._resolve_string_set(rhs)
                        if values:
                            sources.update(values)

            # Updates may sit inside IF/nested structure; scan the whole body.
            for n in iter_tree(body):
                if n.type != "bound_infix_op" or not any(c.type == "eq" for c in n.children):
                    continue
                lhs, rhs = n.children[0], n.children[-1]
                if lhs.type != "bound_postfix_op" or not any(c.type == "prime" for c in lhs.children):
                    continue
                ref = lhs.child(0)
                if ref is None or node_text(ref) != phase_var:
                    continue
                # Candidate targets: domain strings appearing in the new value.
                for s in iter_tree(rhs):
                    if s.type == "string" and string_value(s) in domain:
                        targets.add(string_value(s))

            sources &= domain
            for tgt in sorted(targets):
                if sources:
                    for src in sorted(sources):
                        edges.append((src, tgt, action))
                else:
                    notes.append((action, tgt))
        return edges, notes

    def initial_values(self, phase_var: str) -> list[str]:
        body = self.defs.get("Init")
        if body is None:
            return []
        domain = set(self.phase_domains[phase_var])
        for item in self._conjunct_spine(body):
            if item.type != "bound_infix_op" or not any(c.type == "eq" for c in item.children):
                continue
            lhs, rhs = item.children[0], item.children[-1]
            if self._membership_var(lhs) != phase_var:
                continue
            values = sorted({string_value(s) for s in iter_tree(rhs)
                             if s.type == "string" and string_value(s) in domain})
            return values
        return []


# ---- rendering ---------------------------------------------------------------


def state_id(value: str) -> str:
    ident = re.sub(r"[^A-Za-z0-9_]", "_", value)
    if not ident or not ident[0].isalpha() or ident.lower() in MERMAID_STATE_RESERVED:
        ident = "s_" + ident
    return ident


def render_state_diagram(spec: Spec, var: str) -> list[str]:
    domain = spec.phase_domains[var]
    edges, notes = spec.transitions(var)
    lines = [f"### `{var}`", ""]
    if edges:
        lines.append("```mermaid")
        lines.append("stateDiagram-v2")
        lines.append("    direction LR")
        for value in domain:
            if state_id(value) != value:
                lines.append(f'    state "{value}" as {state_id(value)}')
        for value in spec.initial_values(var):
            lines.append(f"    [*] --> {state_id(value)}")
        merged: dict[tuple[str, str], list[str]] = {}
        for src, tgt, action in edges:
            merged.setdefault((src, tgt), []).append(action)
        for (src, tgt), actions in merged.items():
            label = ", ".join(dict.fromkeys(actions))
            lines.append(f"    {state_id(src)} --> {state_id(tgt)} : {label}")
        # Phase colors are shared with trace-timeline tenure bands
        # (scripts/tla-viz/phasecolors.py): same value, same hue, both layers.
        colors = phasecolors.assign(domain)
        for value in domain:
            if value in colors:
                hex_color = colors[value][0]  # light step; GitHub themes both
                lines.append(f"    classDef c_{state_id(value)} "
                             f"fill:{hex_color}30,stroke:{hex_color}")
                lines.append(f"    class {state_id(value)} c_{state_id(value)}")
        lines.append("```")
    else:
        lines.append(f"Domain: {', '.join(f'`{v}`' for v in domain)}. "
                     "No statically extractable guard/update transitions.")
    if notes:
        lines.append("")
        lines.append("Writes whose source state is not statically determined:")
        lines.append("")
        for action, tgt in sorted(set(notes)):
            lines.append(f'- `{action}` sets `{var}` to `"{tgt}"`')
    lines.append("")
    return lines


def render_action_table(spec: Spec) -> list[str]:
    lines = [
        "| Action | Reads (incl. helper operators) | Writes |",
        "| --- | --- | --- |",
    ]
    for action in spec.actions:
        reads, writes = spec.op_usage(action)
        reads_s = ", ".join(f"`{v}`" for v in spec.variables if v in reads) or "—"
        writes_s = ", ".join(f"`{v}`" for v in spec.variables if v in writes) or "—"
        lines.append(f"| `{action}` | {reads_s} | {writes_s} |")
    lines.append("")
    return lines


def render_write_graph(spec: Spec) -> list[str]:
    lines = ["```mermaid", "flowchart LR"]
    used_vars = []
    edge_lines = []
    for i, action in enumerate(spec.actions):
        _, writes = spec.op_usage(action)
        guard_reads = spec.direct_guard_reads(action) - writes
        for v in spec.variables:
            if v in writes or v in guard_reads:
                if v not in used_vars:
                    used_vars.append(v)
        for v in spec.variables:
            if v in writes:
                edge_lines.append(f"    a{i} --> v{used_vars.index(v)}")
        for v in spec.variables:
            if v in guard_reads:
                edge_lines.append(f"    v{used_vars.index(v)} -.-> a{i}")
    lines.append('    subgraph actions["Actions"]')
    for i, action in enumerate(spec.actions):
        lines.append(f"        a{i}[{action}]")
    lines.append("    end")
    lines.append('    subgraph state["State variables"]')
    for j, v in enumerate(used_vars):
        lines.append(f"        v{j}([{v}])")
    lines.append("    end")
    lines.extend(edge_lines)
    lines.append("```")
    lines.append("")
    return lines


def render_spec(spec: Spec, out_path: Path) -> str:
    rel = Path(os.path.relpath(spec.path, out_path.parent)).as_posix()
    mutant_note = (
        f" {len(spec.mutant_actions)} expected-failure mutant action(s) "
        "(gated by `Buggy*` constants) omitted."
        if spec.mutant_actions else ""
    )
    def n_of(n, noun):
        return f"{n} {noun}{'' if n == 1 else 's'}"

    lines = [
        GENERATED_NOTE,
        "",
        f"# {spec.display_name} — structural diagrams",
        "",
        f"Generated from [`{spec.path.name}`]({rel}). "
        f"{n_of(len(spec.variables), 'state variable')}, "
        f"{n_of(len(spec.actions), 'action')} in `Next`.{mutant_note}",
        "",
    ]

    phase_vars = [v for v in spec.variables if v in spec.phase_domains]
    if phase_vars:
        lines += [
            "## Phase state machines",
            "",
            "Transitions are extracted from action guards and primed updates; "
            "edge labels are the actions that perform the transition.",
            "",
        ]
        for var in phase_vars:
            lines += render_state_diagram(spec, var)

    if spec.actions:
        lines += ["## Actions and the state they touch", ""]
        lines += render_action_table(spec)
        lines += [
            "## Write graph",
            "",
            "Solid edges: the action updates the variable. "
            "Dotted edges: the action's own definition reads the variable "
            "(reads via helper operators appear in the table above, not here).",
            "",
        ]
        lines += render_write_graph(spec)

    return "\n".join(lines).rstrip() + "\n"


def render_index(specs: list[Spec], specs_dir: Path) -> str:
    lines = [
        GENERATED_NOTE,
        "",
        "# TLA+ structural diagrams",
        "",
        "Auto-generated overviews of each protocol spec: phase state machines, "
        "action/state tables, and write graphs. Expected-failure mutant actions "
        "(`Buggy*`-gated) are omitted. See the spec headers and `INVENTORY.md` "
        "for the authoritative protocol descriptions.",
        "",
    ]
    by_group: dict[str, list[Spec]] = {}
    for spec in specs:
        group = Path(os.path.relpath(spec.path.parent, specs_dir)).as_posix()
        by_group.setdefault("vendored / legacy (root)" if group == "."
                            else group, []).append(spec)
    for group in sorted(by_group):
        lines.append(f"## {group}")
        lines.append("")
        for spec in by_group[group]:
            rel_md = Path(os.path.relpath(spec.path.parent, specs_dir),
                          f"{spec.path.stem}.md").as_posix().removeprefix("./")
            phase_vars = [v for v in spec.variables if v in spec.phase_domains]

            def n_of(n, noun):
                return f"{n} {noun}{'' if n == 1 else 's'}"

            lines.append(
                f"- [{spec.display_name}]({rel_md}) — "
                f"{n_of(len(spec.actions), 'action')}, "
                f"{n_of(len(spec.variables), 'variable')}, "
                f"{n_of(len(phase_vars), 'phase state machine')}"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--specs-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--check", action="store_true",
                    help="verify committed diagrams are up to date; write nothing")
    args = ap.parse_args()

    skip_dirs = {".generated", "traces", "diagrams"}
    spec_paths = sorted(
        p for p in args.specs_dir.rglob("*.tla")
        if not SKIP_MODULE_RE.search(p.stem)
        and not (skip_dirs & set(p.relative_to(args.specs_dir).parts))
    )
    if not spec_paths:
        print(f"no protocol specs found in {args.specs_dir}", file=sys.stderr)
        return 1

    specs = [Spec(p) for p in spec_paths]
    outputs: dict[Path, str] = {}
    for spec in specs:
        rel_dir = spec.path.parent.relative_to(args.specs_dir)
        out_path = args.out_dir / rel_dir / f"{spec.path.stem}.md"
        outputs[out_path] = render_spec(spec, out_path)
    outputs[args.out_dir / "README.md"] = render_index(specs, args.specs_dir)

    stale = []
    for path, content in outputs.items():
        if args.check:
            if not path.exists() or path.read_text() != content:
                stale.append(path)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            if not path.exists() or path.read_text() != content:
                path.write_text(content)
                print(f"wrote {path}")

    if args.check and stale:
        for path in stale:
            print(f"stale: {path}", file=sys.stderr)
        print("run `make -C zig tla-viz` to regenerate", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
