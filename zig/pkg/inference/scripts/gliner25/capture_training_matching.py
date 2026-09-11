#!/usr/bin/env python3
"""Capture pinned record target bindings, costs, and detached assignments."""
from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import subprocess
import sys
from types import SimpleNamespace

PIN = "3c913c7369301133d3b7699252074c4303ada50e"


def span(start, end):
    return dict(start=start, end=end)


def field(index, query, *values):
    return dict(field=index, query=query, values=[dict(alternatives=[span(s, e) for s, e in value]) for value in values])


def record(identity, actor, tags):
    return dict(structure=0, group=2, id=identity, anchor_query=None,
                fields=[field(0, 3, actor), field(1, 7, *tags)])


def fixtures():
    base = dict(
        layout=dict(structure=0, group=2, mode="latent", word_count=5,
                    fields=[dict(query=3, cardinality="required_one"), dict(query=7, cardinality="zero_or_more")],
                    candidate_spans=[span(i, i + 1) for i in range(4)], candidate_valid=[True] * 4,
                    field_membership=[True, True, False, False, False, False, True, True],
                    instance_mask=[True] * 3),
        records=[record("first", [(0, 1)], [[(2, 3)]]), record("second", [(1, 2)], [[(3, 4)]])],
        logits=dict(objects=[-0.5, 2.0, 0.7], assignments=[((i * 7) % 17 - 8) / 3 for i in range(30)]),
    )
    cases = []
    for mode in ("latent", "anchorless"):
        for scenario in ("normal", "ties", "masked", "large_masked_cost", "sentinel"):
            case = copy.deepcopy(base)
            case["id"] = f"{mode}_{scenario}"
            case["layout"]["mode"] = mode
            if scenario == "ties":
                case["logits"] = dict(objects=[0.0] * 3, assignments=[0.0] * 30)
            elif scenario in ("masked", "large_masked_cost"):
                case["layout"]["instance_mask"] = [False, True, True]
                if scenario == "large_masked_cost":
                    case["logits"]["objects"] = [0.0, -30000.0, -40000.0]
            elif scenario == "sentinel":
                case["logits"]["assignments"] = [20000.0, -30000.0, -30000.0, 0.0, 0.0] * 6
            cases.append(case)
    for scenario in ("ordinary", "alternatives", "ambiguous"):
        case = copy.deepcopy(base)
        case["id"] = "natural_" + scenario
        case["layout"].update(mode="natural", anchor_query=3, anchor_candidates=[0, 1, None], instance_mask=[True, True, False])
        for r in case["records"]:
            r["anchor_query"] = 3
        if scenario == "alternatives":
            case["records"] = [case["records"][0]]
            case["records"][0]["fields"][0] = field(0, 3, [(1, 2), (0, 1)])
        elif scenario == "ambiguous":
            case["records"][1]["fields"][0] = field(0, 3, [(0, 1)])
            case["native_error"] = "ambiguous_anchor"
        cases.append(case)
    for scenario in ("missing_value", "capacity", "empty", "all_masked"):
        case = copy.deepcopy(base)
        case["id"] = scenario
        if scenario == "missing_value":
            case["records"][0]["fields"][1] = field(1, 7, [(2, 3)], [(4, 5)])
            case["native_error"] = "missing_candidate"
        elif scenario == "capacity":
            case["layout"]["instance_mask"] = [False, True, False]
            case["native_error"] = "capacity"
        else:
            case["records"] = []
            if scenario == "all_masked":
                case["layout"]["instance_mask"] = [False] * 3
        cases.append(case)
    mixed = copy.deepcopy(base)
    mixed["id"] = "preserve_original_record_index"
    other = copy.deepcopy(mixed["records"][0])
    other["structure"] = 4
    mixed["records"].insert(1, other)
    cases.append(mixed)
    return cases


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, default=Path(__file__).parents[2] / "testdata/gliner25/training_matching.json")
    args = parser.parse_args()
    files = ("gliner2/models/boundary/records.py", "gliner2/models/boundary/constants.py", "gliner2/training/matching.py",
             "gliner2/processing/records.py", "gliner2/processing/targets.py")
    if subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.upstream, text=True).strip() != PIN:
        raise SystemExit("wrong upstream revision")
    if subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no", "--", *files], cwd=args.upstream, text=True):
        raise SystemExit("pinned record sources have local modifications")
    sys.path.insert(0, str(args.upstream))
    import torch
    from gliner2.models.boundary.records import _dense_gold_indicator
    from gliner2.processing.records import FieldCardinality, RecordFieldSpec
    from gliner2.processing.targets import RecordFieldTarget, RecordTarget
    from gliner2.training.matching import build_dense_record_matching_cost, linear_sum_assignment

    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)

    def flat(t):
        return t.detach().reshape(-1).tolist()

    rows = []
    for case in fixtures():
        layout = case["layout"]
        field_specs = [RecordFieldSpec(query_id=f["query"], name=f"field{i}", role_index=i,
                                       cardinality=FieldCardinality(f["cardinality"])) for i, f in enumerate(layout["fields"])]
        records, record_indices = [], []
        for i, raw in enumerate(case["records"]):
            if raw["structure"] != layout["structure"]:
                continue
            records.append(RecordTarget(instance_id=raw["id"], task_index=raw["group"], anchor_query_id=raw["anchor_query"],
                                        fields=tuple(RecordFieldTarget(query_id=f["query"], values=tuple(tuple((s["start"], s["end"]) for s in v["alternatives"]) for v in f["values"])) for f in raw["fields"])))
            record_indices.append(i)
        nc, nf, ni = len(layout["candidate_spans"]), len(field_specs), len(layout["instance_mask"])
        pool_spans = torch.tensor([[s["start"], s["end"]] for s in layout["candidate_spans"]])
        membership = torch.tensor(layout["field_membership"]).reshape(nf, nc)
        group = SimpleNamespace(field_specs=field_specs, pool_spans=pool_spans, field_membership=membership)
        indicator = _dense_gold_indicator(group, records)
        scalar = torch.tensor([f.cardinality.is_scalar for f in field_specs])
        mask = torch.tensor(layout["instance_mask"])
        obj32 = torch.tensor(case["logits"]["objects"], dtype=torch.float32)
        assign32 = torch.tensor(case["logits"]["assignments"], dtype=torch.float32).reshape(ni, nf, nc + 1)
        assign32[..., 1:] = assign32[..., 1:].masked_fill(~membership[None], -10000)
        costs64 = build_dense_record_matching_cost(obj32.double(), assign32.double(), indicator, scalar, mask)
        costs32 = build_dense_record_matching_cost(obj32, assign32, indicator, scalar, mask)
        case["expected"] = dict(indicator=flat(indicator), record_indices=record_indices, costs=flat(costs64), graph_costs=flat(costs32))

        def matches(costs):
            if not records:
                pairs = []
            elif layout["mode"] == "natural":
                anchor_index = next(i for i, f in enumerate(field_specs) if f.query_id == layout["anchor_query"])
                seeds = layout["anchor_candidates"]
                pairs = []
                for g in range(len(records)):
                    candidates = [(seeds[i], i) for i in range(ni) if mask[i] and seeds[i] is not None and indicator[g, anchor_index, seeds[i]]]
                    _, i = min(candidates)
                    pairs.append(dict(instance=i, record=g, annotation=record_indices[g]))
            else:
                active = torch.nonzero(mask).flatten()
                # Strict supervision correction: masked hypotheses cannot take
                # a gold column and then disappear during post-match filtering.
                matched_rows, cols = linear_sum_assignment(costs[active])
                pairs = [dict(instance=int(active[r]), record=int(c), annotation=record_indices[int(c)]) for r, c in zip(matched_rows, cols)]
            obj_target = [0.0] * ni
            if layout["mode"] != "natural":
                for pair in pairs:
                    obj_target[pair["instance"]] = 1.0
            return dict(pairs=pairs, object_targets=obj_target, object_mask=[active and layout["mode"] != "natural" for active in layout["instance_mask"]])

        if "native_error" not in case:
            case["expected"].update(matches(costs64))
            case["expected"]["graph_pairs"] = matches(costs32)["pairs"]
        rows.append(case)
    report = dict(format_version=1, provenance=dict(upstream_commit=PIN, python=platform.python_version(), torch=importlib.metadata.version("torch"),
                                                  source_sha256={name: hashlib.sha256((args.upstream / name).read_bytes()).hexdigest() for name in files}),
                  scope="record_targets_costs_and_detached_matching", matching_domain="valid_hypotheses_only",
                  training_qualified=False, cases=rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, allow_nan=False, separators=(",", ":")) + "\n")
    print(f"wrote {len(rows)} record matching cases")


if __name__ == "__main__":
    main()
