#!/usr/bin/env python3
"""Bounded fixed-score capture of unchanged pinned Fastino JointIE methods.

Package containers avoid gliner2.__init__ and its model imports. The five source
modules execute their exact pinned bytes; no optimizer methods are replaced.
This captures a control algorithm, not a trained-model or quality oracle.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import importlib.abc
import json
import logging
import math
import os
from pathlib import Path
import signal
import stat
import sys
import types


ROOT = Path(__file__).resolve().parent
SCOPE = "gliner25_pinned_joint_optimizer_fixed_scores_v1"
SOURCE_COMMIT = "3c913c7369301133d3b7699252074c4303ada50e"
FORBIDDEN = frozenset({"torch", "transformers", "numpy", "gliner2.inference", "gliner2.model"})
MAX_FILE = 131072
MAX_OUTPUT = 1048576
MODULES = (
    ("gliner2.joint_ie.candidates", "gliner2/joint_ie/candidates.py"),
    ("gliner2.joint_ie.constraints", "gliner2/joint_ie/constraints.py"),
    ("gliner2.joint_ie.optimizers.base", "gliner2/joint_ie/optimizers/base.py"),
    ("gliner2.joint_ie.optimizers.greedy", "gliner2/joint_ie/optimizers/greedy.py"),
    ("gliner2.joint_ie.optimizers.beam", "gliner2/joint_ie/optimizers/beam.py"),
)


def encode(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2,
                       allow_nan=False) + "\n").encode()


def digest(data):
    return {"size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def read_regular(path, maximum=MAX_FILE):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
            raise ValueError(f"not a bounded regular file: {path}")
        chunks = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        after = os.fstat(fd)
        if len(data) != before.st_size or len(data) > maximum or (
            before.st_size, before.st_mtime_ns, before.st_ctime_ns
        ) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError(f"file changed while reading: {path}")
        return data
    finally:
        os.close(fd)


def forbidden_loaded():
    return sorted(name for name in sys.modules
                  if any(name == root or name.startswith(root + ".") for root in FORBIDDEN))


class ImportGuard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == root or fullname.startswith(root + ".") for root in FORBIDDEN):
            raise RuntimeError(f"forbidden model/dependency import: {fullname}")
        if fullname.startswith("gliner2."):
            raise RuntimeError(f"source module outside fixed allowlist: {fullname}")
        return None


@contextmanager
def source_modules(upstream, pins):
    if forbidden_loaded() or any(name == "gliner2" or name.startswith("gliner2.") for name in sys.modules):
        raise RuntimeError("capture requires a fresh interpreter without gliner2/model dependencies")
    raw = {}
    for name, relative in MODULES:
        data = read_regular(upstream / relative)
        if digest(data) != pins[relative]:
            raise ValueError(f"pinned source mismatch: {relative}")
        raw[name] = data
    created = []
    guard = ImportGuard()
    sys.meta_path.insert(0, guard)
    try:
        for package in ("gliner2", "gliner2.joint_ie", "gliner2.joint_ie.optimizers"):
            module = types.ModuleType(package)
            module.__package__ = package
            module.__path__ = []  # No path loader can reopen unpinned source.
            sys.modules[package] = module
            created.append(package)
        for name, relative in MODULES:
            module = types.ModuleType(name)
            module.__package__ = name.rsplit(".", 1)[0]
            module.__file__ = str(upstream / relative)
            sys.modules[name] = module
            created.append(name)
            exec(compile(raw[name], module.__file__, "exec", dont_inherit=True), module.__dict__)
        yield {name.rsplit(".", 1)[1]: sys.modules[name] for name, _ in MODULES}
        if forbidden_loaded():
            raise RuntimeError("capture imported a forbidden model dependency")
    finally:
        for name in reversed(created):
            sys.modules.pop(name, None)
        sys.meta_path.remove(guard)


def node(label, start, score, end=None):
    return {"entity_type": label, "start": start, "end": start + 1 if end is None else end,
            "score": float(score), "probability": 0.5}


def edge(head, tail, score, *, relation="r", slot=None, alternative=None):
    return {"relation_type": relation, "head_index": head, "tail_index": tail,
            "score": float(score), "slot": slot, "hypothesis": relation,
            "count_alternative": alternative}


def fixed_cases():
    basic = [{"type": "EntityOverlapPolicy", "policy": "disallow"},
             {"type": "UniqueRelationPair"}, {"type": "NoSelfLoops"}]
    cases = []
    def add(identifier, nodes, edges, constraints=None, note=""):
        cases.append({"id": identifier, "beam_width": 32, "nodes": nodes, "edges": edges,
                      "constraints": basic if constraints is None else constraints, "note": note})

    add("six_negative_edges", [node("A" if i % 2 == 0 else "B", 2 * i, 10) for i in range(12)],
        [edge(2 * i, 2 * i + 1, -1, slot=i) for i in range(6)],
        note="Edge-first beam32 prunes low partial scores before free positive nodes are added; the independent no-edge assignment has utility120.")

    nodes, edges = [], []
    for i in range(6):
        head = len(nodes)
        nodes.extend([node("H", 6 * i, 0), node("A", 6 * i + 2, 10), node("B", 6 * i + 4, 0)])
        edges.extend([edge(head, head + 1, -1, slot=i), edge(head, head + 2, 9, slot=i)])
    add("greedy_baseline_wins_beam32", nodes, edges,
        note="All initial atomic gains are9. Greedy raw-edge tie rank selects B edges; source beam32's retained partial signatures prefer A endpoints. The final source beam must consider its independent greedy baseline.")

    person, organization, relation = "per's \"son\" Ω", "组织", "works'\"Ω"
    add("semantic_tie_slots_2_10", [node(organization, 2, 0), node(person, 0, 0), node(organization, 10, 0)],
        [edge(1, 0, 1, relation=relation, slot=2), edge(1, 2, 1, relation=relation, slot=10)],
        basic + [{"type": "MaxRelationsPerHead", "limit": 1, "relation": relation}],
        "Body-token candidate IDs and Python repr/str keys determine ties; source final solution max differs from initial edge rank. Source node output preserves original candidate input order.")

    add("zero_gain_edge_is_retained", [node("A", 2, 0), node("B", 10, 0)], [edge(0, 1, 0, slot=0)],
        note="The source skips gain<0, so an exactly zero atomic gain is admitted.")
    add("allowed_self_endpoint_counted_twice", [node("A", 2, 10)], [edge(0, 0, -15, slot=0)],
        [{"type": "EntityOverlapPolicy", "policy": "disallow"}, {"type": "UniqueRelationPair"}],
        "Pinned source new_ids contains an unseen self endpoint twice; reported score5 differs from the unique-node-plus-edge sum-5. This records existing source behavior, not a corrected objective.")
    add("derived_symmetric_invalid_cycle", [node("A", 2, 1), node("A", 10, 1)], [edge(0, 1, 1, slot=0)],
        basic + [{"type": "SymmetricRelation", "relation": "r"}, {"type": "AcyclicRelation", "relation": "r"}],
        "A primary edge passes incremental checks but its required reverse creates a cycle. Final constraint revalidation must reject that assignment.")
    add("count_alternatives_and_slots", [node("A", 0, 0), node("A", 2, 0), node("A", 4, 0)],
        [edge(0, 1, 5, slot=0, alternative=1), edge(0, 2, 4, slot=1, alternative=1),
         edge(1, 2, 8, slot=0, alternative=2), edge(2, 0, 8, slot=0, alternative=2)],
        note="Two compatible slots in alternative1 beat the single mutually exclusive slot in alternative2.")
    add("derived_inverse_valid", [node("A", 2, 1), node("B", 10, 1)], [edge(0, 1, 3, slot=2)],
        basic + [{"type": "InverseRelation", "relation": "r", "inverse": "reverse'Ω"}],
        "Final source companion IDs/order are returned from BaseOptimizer.solution itself.")
    add("positive_free_node_overlap_tie", [node("组织", 2, 1), node("per's \"son\" Ω", 2, 1)], [],
        note="The source free-node finish uses entity label then body-token start/end after equal score, with exact overlap constraints.")
    return cases


def make_problem(case, modules):
    candidates, constraints = modules["candidates"], modules["constraints"]
    if len(case["nodes"]) > 32 or len(case["edges"]) > 16 or case["beam_width"] != 32:
        raise ValueError("fixed-case bound exceeded")
    nodes = tuple(candidates.NodeCandidate(**row) for row in case["nodes"])
    edges = []
    for row in case["edges"]:
        values = dict(row)
        values["head"] = nodes[values.pop("head_index")].candidate_id
        values["tail"] = nodes[values.pop("tail_index")].candidate_id
        edges.append(candidates.EdgeCandidate(**values))
    rules = tuple(constraints.constraint_from_dict(row) for row in case["constraints"])
    return candidates.JointProblem(nodes, tuple(edges), rules)


def solution_row(problem, result, validator):
    node_index = {node.candidate_id: index for index, node in enumerate(problem.nodes)}
    edge_index = {edge.candidate_id: index for index, edge in enumerate(problem.edges)}
    if not math.isfinite(result.score):
        raise ValueError("nonfinite source score")
    return {
        "score": result.score, "feasible_flag": result.feasible,
        "final_constraints_satisfied": validator.validate_solution(problem, result),
        "node_indices": [node_index[node.candidate_id] for node in result.nodes],
        "node_ids": [node.candidate_id for node in result.nodes],
        "node_id_strings": [str(node.candidate_id) for node in result.nodes],
        "edges": [{"source_edge_index": edge_index.get(edge.candidate_id),
                   "candidate_id": edge.candidate_id, "candidate_id_string": str(edge.candidate_id),
                   "relation_type": edge.relation_type, "head_index": node_index[edge.head],
                   "tail_index": node_index[edge.tail], "derived": edge.derived,
                   "score": edge.score, "slot": edge.slot, "hypothesis": edge.hypothesis,
                   "count_alternative": edge.count_alternative} for edge in result.edges],
        "unique_node_plus_edge_sum": sum(node.score for node in result.nodes) + sum(edge.score for edge in result.edges),
    }


def capture_case(case, modules):
    problem = make_problem(case, modules)
    beam = modules["beam"].BeamOptimizer(beam_width=case["beam_width"])
    greedy = modules["greedy"].GreedyOptimizer()
    finishes = []
    target = beam._finish_nodes.__func__.__code__
    old_profile = sys.getprofile()
    if old_profile is not None:
        raise RuntimeError("profiling owner already active")
    def observe(frame, event, value):
        if event == "return" and frame.f_code is target:
            if len(finishes) >= case["beam_width"]:
                raise RuntimeError("source beam finish count exceeded declared width")
            finishes.append(value)
    sys.setprofile(observe)
    try:
        result = beam.optimize(problem)
    finally:
        sys.setprofile(old_profile)
    greedy_result = greedy.optimize(problem)
    finish_rows = [solution_row(problem, beam.solution(problem, state.node_ids, state.edges, state.score), beam)
                   for state in finishes]
    feasible_finish_scores = [row["score"] for row in finish_rows if row["final_constraints_satisfied"]]
    return {
        "id": case["id"], "input": case,
        "source_keys": {
            "node_ids": [node.candidate_id for node in problem.nodes],
            "node_strings": [str(node.candidate_id) for node in problem.nodes],
            "edge_ids": [edge.candidate_id for edge in problem.edges],
            "edge_strings": [str(edge.candidate_id) for edge in problem.edges],
            "hypothesis_strings": [str(edge.hypothesis) for edge in problem.edges],
            "slot_strings": [str(edge.slot) for edge in problem.edges],
        },
        "beam": solution_row(problem, result, beam),
        "greedy": solution_row(problem, greedy_result, greedy),
        "beam_finish_count": len(finish_rows),
        "best_feasible_beam_finish_score": max(feasible_finish_scores) if feasible_finish_scores else None,
        "greedy_strictly_beats_all_feasible_beam_finishes": bool(
            feasible_finish_scores and greedy_result.feasible and
            greedy.validate_solution(problem, greedy_result) and greedy_result.score > max(feasible_finish_scores)),
        "beam_finish_candidates": finish_rows,
    }


def capture(upstream, contract):
    if contract["scope"] != SCOPE or contract["source_commit"] != SOURCE_COMMIT:
        raise ValueError("invalid immutable capture contract")
    cases = fixed_cases()
    if digest(encode(cases)) != contract["fixed_cases"]:
        raise ValueError("fixed-case contract changed")
    with source_modules(upstream, contract["source_files"]) as modules:
        rows = [capture_case(case, modules) for case in cases]
    return {
        "format_version": 1, "scope": SCOPE, "qualification": False,
        "source_commit": SOURCE_COMMIT, "source_files": contract["source_files"],
        "fixed_cases": contract["fixed_cases"], "python_version": sys.version.split()[0],
        "scope_limits": {"model_execution": False, "tensor_execution": False, "quality_claim": False,
                         "maximum_cases": 9, "maximum_nodes": 32, "maximum_edges": 16,
                         "beam_width": 32, "output_bytes": MAX_OUTPUT, "wall_seconds": 15},
        "capture_seams": ["empty package containers skip model-loading package initializers",
                          "exact pinned source bytes compiled without modifications",
                          "validation-only return observer records source beam finish states"],
        "forbidden_imports_observed": forbidden_loaded(), "profile_hook_removed": sys.getprofile() is None,
        "cases": rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    def deadline(_signum, _frame):
        raise TimeoutError("fixed-score source capture exceeded 15 seconds")
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(15)
    contract_bytes = read_regular(ROOT / "contract.json")
    contract = json.loads(contract_bytes)
    report = capture(args.upstream, contract)
    report["contract"] = digest(contract_bytes)
    report["generator"] = digest(read_regular(Path(__file__)))
    data = encode(report)
    if len(data) > MAX_OUTPUT:
        raise ValueError("capture output budget exceeded")
    with args.output.open("xb") as stream:
        stream.write(data)
    signal.alarm(0)
    print(json.dumps({"event": "complete", "cases": len(report["cases"]), "output": digest(data),
                      "model_execution": False, "forbidden_imports_observed": report["forbidden_imports_observed"]}))


if __name__ == "__main__":
    main()
