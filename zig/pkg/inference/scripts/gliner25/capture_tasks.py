#!/usr/bin/env python3
"""Capture independent classification, directional relation, and record heads."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import oracle


def capture(source: Path, destination: Path) -> dict[str, Any]:
    provenance, torch = oracle.prepare_runtime(source)
    from gliner2.layers import create_mlp
    from gliner2.models.boundary.records import RecordHead
    from gliner2.models.boundary.relations import RelationPairBatch, SparseRelationScorer
    from gliner2.models.outputs import CandidateTensorBatch
    from gliner2.processing.records import FieldCardinality, RecordFieldSpec, RecordSpec

    torch.manual_seed(2509)
    hidden, batch, queries, candidates = 32, 2, 3, 4
    classifier = create_mlp(input_dim=hidden, intermediate_dims=[2 * hidden], output_dim=1,
                            dropout=0.0, activation="relu", add_layer_norm=False).eval()
    relation = SparseRelationScorer(hidden, dropout=0.0, relation_query_dim=2 * hidden,
                                    use_biaffine_content=True).eval()
    record = RecordHead(hidden, record_dim=16, instance_queries=4).eval()
    tensors: dict[str, Any] = {}
    weights = {}
    for prefix, module in (("classifier", classifier), ("relation_scorer", relation), ("record_decoder", record)):
        weights.update({prefix + "." + name: value for name, value in module.state_dict().items()})

    cls_states = torch.linspace(-1, 1, batch * 4 * hidden).reshape(batch, 4, hidden)
    tensors["classification.input.states"] = cls_states
    tensors["classification.input.mask"] = torch.tensor([[True, True, True, True], [True, True, False, False]])
    relation_states = torch.linspace(-0.6, 0.8, batch * 5 * hidden).reshape(batch, 5, hidden)
    relation_queries = torch.linspace(0.9, -0.7, batch * 2 * hidden * 2).reshape(batch, 2, hidden * 2)
    pairs = RelationPairBatch(
        batch_index=torch.tensor([0, 0, 1, 1, 1]), relation_index=torch.tensor([0, 1, 0, 1, 0]),
        head_start=torch.tensor([0, 2, 0, 1, 0]), head_end=torch.tensor([1, 4, 2, 3, 1]),
        tail_start=torch.tensor([2, 0, 3, 0, 4]), tail_end=torch.tensor([4, 2, 5, 1, 5]),
        head_prob=torch.tensor([0.9, 0.8, 0.7, 0.6, 0.5]), tail_prob=torch.tensor([0.6, 0.7, 0.8, 0.9, 0.5]),
        pair_mask=torch.tensor([True, True, True, True, False]),
    )
    tensors["relation.input.token_states"] = relation_states
    tensors["relation.input.query_states"] = relation_queries
    for name in ("batch_index", "relation_index", "head_start", "head_end", "tail_start", "tail_end", "head_prob", "tail_prob", "pair_mask"):
        tensors["relation.input." + name] = getattr(pairs, name)

    query_states = torch.linspace(-0.8, 0.7, batch * queries * hidden).reshape(batch, queries, hidden)
    candidate_states = torch.linspace(0.7, -0.9, batch * queries * candidates * hidden).reshape(batch, queries, candidates, hidden)
    indices = torch.tensor([[0, 1], [1, 3], [3, 5], [0, 5]])[None, None].expand(batch, queries, -1, -1)
    mask = torch.tensor([[[True, True, False, True], [True, False, True, False], [False] * 4],
                         [[True, False, False, False], [False, True, False, True], [False] * 4]])
    pair_logits = torch.linspace(-1.0, 1.3, batch * queries * candidates).reshape(batch, queries, candidates)
    candidate_batch = CandidateTensorBatch(indices=indices, proposal_logits=None, pair_logits=pair_logits,
                                            valid_mask=mask, query_mask=torch.ones(batch, queries, dtype=torch.bool),
                                            candidate_states=candidate_states)
    tensors.update({"record.input.query_states": query_states, "record.input.candidate_states": candidate_states,
                    "record.input.indices": indices, "record.input.valid_mask": mask, "record.input.pair_logits": pair_logits})

    # Module hooks expose numerical intermediates produced by the upstream
    # forward itself. They do not replace or reimplement scorer mathematics.
    hooks = []
    for family, module in (("classification", classifier), ("relation", relation)):
        for name, child in module.named_modules():
            if not name:
                continue
            key = family + ".intermediate." + name
            hooks.append(child.register_forward_hook(lambda _m, _inputs, output, key=key: tensors.__setitem__(key, output.detach().clone())))
    with torch.inference_mode():
        tensors["classification.expected.logits"] = classifier(cls_states).squeeze(-1)
        tensors["relation.expected.logits"] = relation(relation_states, relation_queries, candidate_batch, pairs)
    for hook in hooks:
        hook.remove()

    rows = []
    with torch.inference_mode():
        for sample in range(batch):
            for mode in ("natural", "latent", "anchorless"):
                name = f"{mode}_{sample}"
                prefix = "record." + name
                fields = (
                    RecordFieldSpec(0, "person", 0, FieldCardinality.REQUIRED_ONE, is_anchor=mode == "natural"),
                    RecordFieldSpec(1, "organization", 1, FieldCardinality.ZERO_OR_MORE),
                    RecordFieldSpec(2, "location", 2, FieldCardinality.OPTIONAL_ONE),
                )
                spec = RecordSpec(task_index=0, task_name="employment", task_type="json_structures", mode=mode,
                                  fields=fields, anchor_query_id=0 if mode == "natural" else None)
                handle = record.inst_proj.register_forward_pre_hook(
                    lambda _module, inputs, prefix=prefix: tensors.__setitem__(prefix + ".expected.instance_states", inputs[0].detach().clone()))
                try:
                    group = record.forward_group(spec, query_states[sample], candidate_batch, sample)
                finally:
                    handle.remove()
                tensors[prefix + ".expected.object_logits"] = group.object_logits
                field_rows = []
                for field, qid in enumerate(group.field_query_ids):
                    key = prefix + f".field.{field}"
                    tensors[key + ".input.query_state"] = query_states[sample, qid]
                    tensors[key + ".input.candidate_states"] = candidate_states[sample, qid][mask[sample, qid]]
                    tensors[key + ".input.spans"] = group.field_spans[field]
                    tensors[key + ".input.logits"] = group.field_cand_logits[field]
                    tensors[key + ".expected.assignment"] = group.assign_logits[field]
                    field_rows.append({"query_id": qid, "name": fields[field].name,
                                       "cardinality": fields[field].cardinality.value,
                                       "is_anchor": fields[field].is_anchor, "tensor_prefix": key})
                rows.append({"id": name, "mode": mode, "sample": sample, "fields": field_rows,
                             "instances": group.num_instances, "instance_seed": group.instance_seed,
                             "instance_spans": group.instance_spans, "tensor_prefix": prefix})

    oracle.verify_upstream_checkout(source)
    with oracle.atomic_output_directory(destination) as output:
        report = {"format_version": 1, "scope": "task_head_reference", "provenance": provenance,
                  "generator_sha256": oracle.sha256_file(Path(__file__)), "oracle_sha256": oracle.sha256_file(Path(oracle.__file__)),
                  "seed": 2509, "hidden": hidden, "record_dim": 16, "instance_queries": 4,
                  "relation_query_dim": hidden * 2, "relation_biaffine_content": True,
                  "real_model_qualified": False, "native_runtime_qualified": False, "training_qualified": False,
                  "records": rows, "weights": oracle.save_tensors(output / "weights.safetensors", weights, torch),
                  "tensors": oracle.save_tensors(output / "tensors.safetensors", tensors, torch)}
        oracle.write_json(output / "capture.json", report)
    return {"status": "captured", "output": str(destination.resolve()), "records": len(rows)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(capture(args.upstream, args.output), sort_keys=True))
        return 0
    except (oracle.ContractError, OSError, ImportError, RuntimeError, ValueError, TypeError) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
