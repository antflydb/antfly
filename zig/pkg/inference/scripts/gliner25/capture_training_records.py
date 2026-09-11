#!/usr/bin/env python3
"""Pinned fully batched dense-record forward/VJP reference, with no encoder."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import oracle


def capture(source: Path):
    provenance, torch = oracle.prepare_runtime(source)
    from gliner2.models.boundary.records import RecordHead
    from gliner2.models.boundary.validation import safe_query_ids
    from gliner2.models.outputs import CandidateTensorBatch

    torch.manual_seed(253927)
    owner = torch.nn.Module()
    owner.record_decoder = RecordHead(hidden_size=4, record_dim=4, instance_queries=2)
    model = owner.record_decoder
    owner.train()
    states = torch.sin(torch.arange(24, dtype=torch.float32).reshape(2, 3, 4) * .23)
    states[1].zero_()
    inputs = {"pool_states": states,
              "query_states": torch.cos(torch.arange(24, dtype=torch.float32).reshape(2, 3, 4) * .37),
              "pair_logits": (torch.arange(18, dtype=torch.float32).reshape(2, 3, 3) - 7) / 5}
    pool_spans = torch.tensor([[[0, 1], [1, 2], [2, 3]], [[0, 0], [0, 0], [0, 0]]], dtype=torch.long)
    valid_mask = torch.tensor([[[1, 1, 1], [1, 0, 1], [0, 1, 1]], [[0, 0, 0], [0, 0, 0], [0, 0, 0]]], dtype=torch.bool)
    query_mask = torch.tensor([[1, 1, 1], [1, 0, 1]], dtype=torch.bool)
    field_query_ids = torch.tensor([[[0, 1], [1, 2], [2, 0]], [[0, 2], [1, -1], [2, 0]]], dtype=torch.long)
    field_mask = torch.tensor([[[1, 1], [1, 1], [1, 0]], [[1, 1], [1, 1], [1, 1]]], dtype=torch.bool)
    scalar_fields = torch.tensor([[[1, 0], [1, 1], [0, 1]], [[1, 0], [1, 1], [0, 1]]], dtype=torch.bool)
    modes = torch.tensor([[0, 1, 2], [0, 1, 2]], dtype=torch.long)
    anchor_fields = torch.tensor([[0, -1, -1], [0, 0, 0]], dtype=torch.long)
    observed = {}

    def instance_hook(_module, values):
        observed["instance_states"] = values[0]

    hook = model.inst_proj.register_forward_pre_hook(instance_hook)

    def pack(value):
        value = value.detach().cpu().contiguous()
        if value.is_floating_point() and not bool(torch.isfinite(value).all()):
            raise oracle.ContractError("nonfinite dense-record forward/VJP capture")
        return {"shape": list(value.shape), "values": value.reshape(-1).tolist()}

    def evaluate(name, group_mask):
        observed.clear()
        leaves = {key: value.detach().clone().requires_grad_() for key, value in inputs.items()}
        candidates = CandidateTensorBatch(indices=pool_spans.unsqueeze(1).expand(2, 3, 3, 2), proposal_logits=None,
            pair_logits=leaves["pair_logits"], valid_mask=valid_mask, query_mask=query_mask,
            candidate_states=leaves["pool_states"].unsqueeze(1).expand(2, 3, 3, 4))
        routing = (field_query_ids, field_mask, scalar_fields, modes, anchor_fields, group_mask)
        # This is the fully batched training entry, not the differently
        # padded per-group helper used by ordinary inference.
        result = model.forward_groups_dense(leaves["query_states"], candidates, routing)
        if set(observed) != {"instance_states"}:
            raise oracle.ContractError("dense record intermediate route differs")
        outputs = {"instance_states": observed["instance_states"], "object_logits": result.object_logits,
                   "assignment_logits": result.assign_logits}
        cotangents = {}
        objective = torch.tensor(0.0)
        for output_name, value in outputs.items():
            seed = int.from_bytes(hashlib.sha256(output_name.encode()).digest()[:2], "little")
            cotangent = (((torch.arange(value.numel()) + seed) % 17).float() - 8).reshape(value.shape) / 13
            cotangents[output_name] = cotangent
            objective = objective + (value * cotangent).sum()
        parameters = dict(owner.named_parameters())
        gradients = torch.autograd.grad(objective, [*leaves.values(), *parameters.values()], allow_unused=False)
        safe_fields, _ = safe_query_ids(field_query_ids, 3, 3, 3, 3)
        groups = []
        for batch in range(2):
            for group in range(3):
                mode = ("natural", "latent", "anchorless")[int(modes[batch, group])]
                anchor = int(anchor_fields[batch, group].clamp(0, 1))
                anchor_query = int(safe_fields[batch, group, anchor])
                groups.append({"batch_index": batch, "group_index": group, "mode": mode,
                    "pool_indices": pack(torch.arange(3) + batch * 3),
                    "field_query_indices": pack(safe_fields[batch, group] + batch * 3),
                    "natural_logit_indices": pack(torch.arange(3) + (batch * 3 + anchor_query) * 3) if mode == "natural" else None,
                    "candidate_mask": pack(valid_mask[batch, 0]), "field_membership": pack(result.field_membership[batch, group]),
                    "instance_mask": pack(result.instance_mask[batch, group])})
        return {"id": name, "training": True, "group_mask": pack(group_mask), "native_groups": groups,
                "outputs": {key: pack(value) for key, value in outputs.items()},
                "cotangents": {key: pack(value) for key, value in cotangents.items()},
                "input_gradients": {key: pack(value) for key, value in zip(leaves, gradients[:len(leaves)])},
                "parameter_gradients": {key: pack(value) for key, value in zip(parameters, gradients[len(leaves):])},
                "instance_mask": pack(result.instance_mask), "field_membership": pack(result.field_membership)}

    all_groups = torch.ones(2, 3, dtype=torch.bool)
    masked_groups = torch.tensor([[1, 0, 1], [1, 1, 0]], dtype=torch.bool)
    try:
        cases = [evaluate("all_groups", all_groups), evaluate("masked_groups", masked_groups)]
        if evaluate("all_groups", all_groups) != cases[0]:
            raise oracle.ContractError("dense-record replay differs")
    finally:
        hook.remove()
    return {"format_version": 1, "scope": "synthetic_boundary_dense_record_forward_vjp", "qualification": False,
            "source_commit": oracle.UPSTREAM_COMMIT, "provenance": provenance,
            "generator_sha256": oracle.sha256_file(Path(__file__)), "oracle_sha256": oracle.sha256_file(Path(oracle.__file__)),
            "source_files": {name: oracle.sha256_file(source / name) for name in (
                "gliner2/models/boundary/records.py", "gliner2/models/boundary/validation.py", "gliner2/models/outputs.py")},
            "config": {"batch": 2, "queries": 3, "candidates": 3, "groups": 3, "fields": 2, "hidden_size": 4,
                       "record_dim": 4, "record_instance_queries": 2, "padded_instances": 3},
            "inputs": {name: pack(value) for name, value in inputs.items()},
            "parameters": {name: pack(value) for name, value in owner.named_parameters()},
            "routing": {"field_query_ids": pack(field_query_ids), "field_mask": pack(field_mask),
                        "scalar_fields": pack(scalar_fields), "modes": pack(modes), "anchor_fields": pack(anchor_fields)},
            "candidate_routing": {"pool_spans": pack(pool_spans), "valid_mask": pack(valid_mask), "query_mask": pack(query_mask)},
            "native_geometry_index_dtype": "i32", "dropout_masks": {}, "cases": cases,
            "notes": ["Only the actual RecordHead.forward_groups_dense training entry executes; no checkpoint, encoder or boundary head is loaded.",
                      "All six sample/group calls share the same live weights and inputs; native parameter VJPs must accumulate across them.",
                      "Instances pad to max(candidate_count, learned_instance_queries), so the third anchorless row is computed before object masking.",
                      "Sample1 has an entirely masked pool with zero candidate values; the source dense path still computes uniform attention over masked columns.",
                      "Field-query routing includes a negative ID and an inactive query; field projection and ABSENT columns remain live where upstream leaves them unmasked.",
                      "Group masking affects instance/object masks but does not erase field membership or assignment rows.",
                      "Cotangents include masked objects, masked candidate columns and unmasked padded assignment rows to distinguish their VJPs.",
                      "RecordHead contains no dropout modules; train mode does not require a synthetic RNG substitute."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, default=oracle.FIXTURES / "training_records.json")
    args = parser.parse_args()
    value = capture(args.source)
    oracle.write_json(args.output, value)
    print(f"{args.output}: {args.output.stat().st_size} bytes; {len(value['parameters'])} parameter VJPs; two cases")


if __name__ == "__main__":
    main()
