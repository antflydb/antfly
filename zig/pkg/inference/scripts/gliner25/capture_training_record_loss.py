#!/usr/bin/env python3
"""Pinned dense-batch record objectives and live-logit gradients, no model."""
from __future__ import annotations

import argparse
import copy
from pathlib import Path
from types import SimpleNamespace

import capture_training_matching as prior
import oracle


def cases():
    source = {item["id"]: item for item in prior.fixtures()}

    def group(name):
        item = copy.deepcopy(source[name])
        item.pop("id")
        item["enabled"] = True
        return item

    result = []

    def add(name, groups, object_weight=1.0, field_weight=1.0):
        result.append(dict(id=name, groups=groups, object_weight=object_weight, field_weight=field_weight))

    add("mixed_modes", [group("latent_normal"), group("anchorless_normal"), group("natural_ordinary")])
    one = group("anchorless_normal")
    one["records"] = one["records"][:1]
    add("global_denominators_and_weights", [group("latent_normal"), one, group("empty")], 0.7, 1.3)
    absent = group("latent_normal")
    absent["layout"]["fields"][0]["cardinality"] = "optional_one"
    absent["records"][0]["fields"][0]["values"] = []
    absent["records"][0]["fields"][1]["values"] = []
    absent["records"][1]["fields"][0] = prior.field(0, 3, [(0, 1), (1, 2)])
    absent["records"][1]["fields"][1] = prior.field(1, 7, [(2, 3)], [(3, 4)])
    add("alternatives_and_absent", [absent])
    add("masked_instances_and_columns", [group("latent_masked"), group("anchorless_masked")])
    add("all_masked", [group("all_masked")])
    natural_empty = group("natural_ordinary")
    natural_empty["records"] = []
    add("natural_empty_has_no_object_loss", [natural_empty])
    add("finite_mask_dominates_gold_mass", [group("latent_sentinel")])
    one_field = group("latent_normal")
    one_field["layout"]["fields"] = one_field["layout"]["fields"][:1]
    one_field["layout"]["field_membership"] = one_field["layout"]["field_membership"][:4]
    for record in one_field["records"]:
        record["fields"] = record["fields"][:1]
    raw = one_field["logits"]["assignments"]
    one_field["logits"]["assignments"] = [value for i in range(3) for value in raw[i * 10:i * 10 + 5]]
    add("padded_fields_do_not_change_normalization", [group("latent_normal"), one_field])
    disabled = group("empty")
    disabled["enabled"] = False
    add("disabled_empty_group", [group("latent_normal"), disabled])
    zero = group("empty")
    zero["layout"]["instance_mask"] = []
    zero["logits"] = dict(objects=[], assignments=[])
    add("zero_instances_zero_gold", [zero])
    return result


def capture(source: Path):
    provenance, torch = oracle.prepare_runtime(source)
    from gliner2.models.boundary.records import _dense_gold_indicator, compute_dense_batch_loss
    from gliner2.processing.records import FieldCardinality, RecordFieldSpec
    from gliner2.processing.targets import RecordFieldTarget, RecordTarget
    from gliner2.training.matching import build_dense_record_matching_cost, linear_sum_assignment
    import torch.nn.functional as functional

    def flat(value):
        return value.detach().reshape(-1).tolist()

    output_cases = []
    for case in cases():
        groups = case["groups"]
        fields = max(len(g["layout"]["fields"]) for g in groups)
        instances = len(groups[0]["layout"]["instance_mask"])
        candidates = len(groups[0]["layout"]["candidate_spans"])
        max_gold = max(1, max(len(g["records"]) for g in groups))
        raw_objects, raw_assignments = [], []
        objects, assignments, indicators, field_masks, memberships = [], [], [], [], []
        scalar_fields, instance_masks, modes, anchors, enabled, record_masks = [], [], [], [], [], []
        for group in groups:
            layout = group["layout"]
            nf = len(layout["fields"])
            if len(layout["instance_mask"]) != instances or len(layout["candidate_spans"]) != candidates:
                raise oracle.ContractError("fixture must share dense instance/candidate widths")
            specs = [RecordFieldSpec(query_id=f["query"], name=f"field{i}", role_index=i,
                                     cardinality=FieldCardinality(f["cardinality"])) for i, f in enumerate(layout["fields"])]
            records = [RecordTarget(instance_id=r["id"], task_index=r["group"], anchor_query_id=r["anchor_query"],
                                   fields=tuple(RecordFieldTarget(query_id=f["query"], values=tuple(tuple((s["start"], s["end"]) for s in v["alternatives"]) for v in f["values"])) for f in r["fields"])) for r in group["records"]]
            membership = torch.tensor(layout["field_membership"], dtype=torch.bool).reshape(nf, candidates)
            spans = torch.tensor([[s["start"], s["end"]] for s in layout["candidate_spans"]], dtype=torch.long)
            gold = _dense_gold_indicator(SimpleNamespace(field_specs=specs, pool_spans=spans, field_membership=membership), records)
            raw_obj = torch.tensor(group["logits"]["objects"], dtype=torch.float32, requires_grad=True)
            raw_assign = torch.tensor(group["logits"]["assignments"], dtype=torch.float32).reshape(instances, nf, candidates + 1).requires_grad_()
            valid_assignment = torch.cat((torch.ones(nf, 1, dtype=torch.bool), membership), -1)
            masked = raw_assign.masked_fill(~valid_assignment[None], -10000)
            raw_objects.append(raw_obj)
            raw_assignments.append(raw_assign)
            objects.append(raw_obj)
            assignments.append(functional.pad(masked, (0, 0, 0, fields - nf), value=-10000))
            indicators.append(functional.pad(gold, (0, 0, 0, fields - nf, 0, max_gold - len(records))))
            memberships.append(functional.pad(membership, (0, 0, 0, fields - nf)))
            field_masks.append([True] * nf + [False] * (fields - nf))
            scalar_fields.append([f.cardinality.is_scalar for f in specs] + [False] * (fields - nf))
            instance_masks.append(layout["instance_mask"])
            modes.append(("natural", "latent", "anchorless").index(layout["mode"]))
            anchors.append(next((i for i, f in enumerate(specs) if f.query_id == layout.get("anchor_query")), 0))
            enabled.append(group["enabled"])
            record_masks.append([True] * len(records) + [False] * (max_gold - len(records)))
        dense = SimpleNamespace(
            object_logits=torch.stack(objects)[None], assign_logits=torch.stack(assignments)[None],
            instance_mask=torch.tensor(instance_masks, dtype=torch.bool)[None],
            field_membership=torch.stack(memberships)[None],
            pool_spans=spans[None], field_mask=torch.tensor(field_masks, dtype=torch.bool)[None],
            scalar_fields=torch.tensor(scalar_fields, dtype=torch.bool)[None],
            modes=torch.tensor(modes, dtype=torch.long)[None],
            anchor_fields=torch.tensor(anchors, dtype=torch.long)[None], group_mask=torch.tensor(enabled, dtype=torch.bool)[None],
        )
        indicator = torch.stack(indicators)[None]
        mask = torch.tensor(record_masks, dtype=torch.bool)[None]
        losses = compute_dense_batch_loss(dense, indicator, mask)
        objective = case["object_weight"] * losses["object_loss"] + case["field_weight"] * losses["field_loss"]
        derivatives = torch.autograd.grad(objective, [*raw_objects, *raw_assignments], allow_unused=True)
        cost = build_dense_record_matching_cost(dense.object_logits.detach(), dense.assign_logits.detach(), indicator,
                                               dense.scalar_fields, dense.instance_mask)
        object_count, matched_count, gradients = 0, 0, []
        for index, group in enumerate(groups):
            layout = group["layout"]
            ng = len(group["records"])
            cost_group = cost[0, index, :, :ng]
            pairs = []
            if ng:
                if layout["mode"] == "natural":
                    natural_gold = indicator[0, index, :ng, anchors[index]]
                    columns = torch.nonzero(natural_gold.any(-1)).flatten()
                    rows = natural_gold[columns].long().argmax(-1)
                else:
                    rows, columns = linear_sum_assignment(cost_group.cpu())
                for row, column in zip(rows, columns):
                    i, g = int(row), int(column)
                    if dense.instance_mask[0, index, i]:
                        pairs.append(dict(instance=i, record=g, annotation=g))
                if len(pairs) != ng:
                    raise oracle.ContractError("loss fixture encountered upstream discarded-gold behavior")
            if group["enabled"]:
                matched_count += len(pairs)
                if layout["mode"] != "natural":
                    object_count += sum(layout["instance_mask"])
            group["expected"] = dict(graph_costs=flat(cost_group), pairs=pairs)
            dobj = derivatives[index]
            dassign = derivatives[index + len(groups)]
            gradients.append(dict(objects=flat(dobj if dobj is not None else torch.zeros_like(raw_objects[index])),
                                  assignments=flat(dassign if dassign is not None else torch.zeros_like(raw_assignments[index]))))
        for value in [objective, *losses.values(), *(g for g in derivatives if g is not None)]:
            if not bool(torch.isfinite(value).all()):
                raise oracle.ContractError(f"nonfinite fixture output {case['id']}")
        case["expected"] = dict(object_loss=float(losses["object_loss"].detach()), field_loss=float(losses["field_loss"].detach()),
                                value=float(objective.detach()), object_count=object_count, matched_record_count=matched_count,
                                gradients=gradients)
        output_cases.append(case)
    files = ["gliner2/models/boundary/records.py", "gliner2/models/boundary/constants.py", "gliner2/training/matching.py"]
    return dict(format_version=1, source_commit=oracle.UPSTREAM_COMMIT, scope="synthetic_dense_batch_record_loss_and_logit_vjp",
                qualification=False, provenance=provenance, generator_sha256=oracle.sha256_file(Path(__file__)),
                oracle_sha256=oracle.sha256_file(Path(oracle.__file__)),
                source_files={name: oracle.sha256_file(source / name) for name in files},
                fixture_dependency_sha256=oracle.sha256_file(Path(prior.__file__)), cases=output_cases,
                notes=["No checkpoint, tokenizer, encoder or optimizer is loaded.",
                       "Actual compute_dense_batch_loss supplies both scalar values and raw-logit gradients.",
                       "Each native group omits padded fields; source field masks establish the equivalent normalization.",
                       "Excluded candidate columns are masked in the live source graph before the loss.",
                       "Detached assignments use exact source float32 costs; final losses are recomputed from live logits.",
                       "Cases avoid upstream inactive-hypothesis gold loss, which native matching rejects separately."])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, default=Path(__file__).parents[2] / "testdata/gliner25/training_record_loss.json")
    args = parser.parse_args()
    payload = capture(args.upstream)
    oracle.write_json(args.output, payload)
    print(f"wrote {args.output}: {len(payload['cases'])} cases")


if __name__ == "__main__":
    main()
