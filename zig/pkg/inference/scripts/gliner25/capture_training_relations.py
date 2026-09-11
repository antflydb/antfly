#!/usr/bin/env python3
"""Pinned tiny sparse-relation forward/VJP reference; no checkpoint is loaded."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import oracle


def capture(source: Path):
    provenance, torch = oracle.prepare_runtime(source)
    from gliner2.models.boundary.relations import RelationPairBatch, SparseRelationScorer
    from gliner2.models.boundary.validation import safe_relation_indices

    torch.manual_seed(253919)
    owner = torch.nn.Module()
    owner.relation_scorer = SparseRelationScorer(4, dropout=0.1, relation_query_dim=8, use_biaffine_content=True)
    model = owner.relation_scorer
    inputs = {"text_states": (torch.arange(24, dtype=torch.float32).reshape(2, 3, 4) - 9) / 7,
              "relation_query_states": torch.cos(torch.arange(32, dtype=torch.float32).reshape(2, 2, 8) * 0.41)}
    route = {"batch_index": [0, 0, 1, 1, -1, 2], "relation_index": [0, 1, 0, 1, 0, 5],
             "head_start": [0, 1, 0, -2, 1, 4], "head_end": [1, 3, 2, 1, 3, 6],
             "tail_start": [1, 0, 2, 0, 0, -1], "tail_end": [3, 1, 3, 2, 1, 1]}
    tensors = {name: torch.tensor(values, dtype=torch.long) for name, values in route.items()}
    pairs = RelationPairBatch(**tensors, head_prob=torch.ones(6), tail_prob=torch.ones(6),
                              pair_mask=torch.tensor([True, True, True, False, True, True]))
    mask = torch.tensor([0.0 if (index * 7 + 13) % 10 == 0 else 1 / 0.9 for index in range(24)], dtype=torch.float32).reshape(6, 4)
    observed = {}
    handles = []

    def pre_features(_module, values):
        observed["features"] = values[0]

    def observe(name, squeeze=False):
        def hook(_module, _inputs, output):
            observed[name] = output.squeeze(-1) if squeeze else output
        return hook

    handles.append(model.mlp[0].register_forward_pre_hook(pre_features))
    for module, name, squeeze in ((model.mlp[2], "hidden", False), (model.mlp, "mlp_logits", True),
                                  (model.head_content_projection, "head_content", False),
                                  (model.tail_content_projection, "tail_content", False)):
        handles.append(module.register_forward_hook(observe(name, squeeze)))
    original_dropout = model.mlp[2].forward
    dropout_calls = []

    def dropout(value):
        if model.training:
            if value.shape != mask.shape:
                raise oracle.ContractError("relation dropout route shape differs")
            dropout_calls.append(1)
            return value * mask
        return value

    model.mlp[2].forward = dropout

    def pack(value):
        value = value.detach().cpu().contiguous()
        if value.is_floating_point() and not bool(torch.isfinite(value).all()):
            raise oracle.ContractError("nonfinite relation forward/VJP capture")
        return {"shape": list(value.shape), "values": value.reshape(-1).tolist()}

    def evaluate(training, final_only=False):
        owner.train(training)
        observed.clear()
        dropout_calls.clear()
        leaves = {name: value.detach().clone().requires_grad_() for name, value in inputs.items()}
        logits = model(*leaves.values(), None, pairs)
        if len(dropout_calls) != int(training):
            raise oracle.ContractError("relation dropout mask consumption differs")
        outputs = {"logits": logits, **observed}
        if set(outputs) != {"logits", "features", "hidden", "mlp_logits", "head_content", "tail_content"}:
            raise oracle.ContractError("relation intermediate capture differs")
        cotangents = {}
        objective = torch.tensor(0.0)
        for name, value in outputs.items():
            seed = int.from_bytes(hashlib.sha256(name.encode()).digest()[:2], "little")
            cotangent = (((torch.arange(value.numel()) + seed) % 17).float() - 8).reshape(value.shape) / 13
            if final_only and name != "logits":
                cotangent = torch.zeros_like(cotangent)
            cotangents[name] = cotangent
            objective = objective + (value * cotangent).sum()
        parameters = dict(owner.named_parameters())
        gradients = torch.autograd.grad(objective, [*leaves.values(), *parameters.values()], allow_unused=False)
        return {"id": ("train_external_mask" if training else "eval") + ("_final_logits_only" if final_only else "_all_outputs"),
                "training": training, "final_logits_only": final_only,
                "outputs": {name: pack(value) for name, value in outputs.items()},
                "cotangents": {name: pack(value) for name, value in cotangents.items()},
                "input_gradients": {name: pack(value) for name, value in zip(leaves, gradients[:len(leaves)])},
                "parameter_gradients": {name: pack(value) for name, value in zip(parameters, gradients[len(leaves):])}}

    try:
        cases = [evaluate(False), evaluate(True), evaluate(False, final_only=True)]
        if evaluate(True) != cases[1]:
            raise oracle.ContractError("relation external-mask replay differs")
    finally:
        model.mlp[2].forward = original_dropout
        for handle in handles:
            handle.remove()
    b = tensors["batch_index"].clamp(0, 1)
    relation, relation_valid = safe_relation_indices(tensors["relation_index"], 2)
    valid = (tensors["batch_index"] >= 0) & (tensors["batch_index"] < 2) & relation_valid & pairs.pair_mask
    hs, he, ts, te = (tensors[name] for name in ("head_start", "head_end", "tail_start", "tail_end"))
    delta = (ts - hs).float()
    geometry = {"query_indices": pack(b * 2 + relation),
                "text_indices": [pack(b * 3 + value.clamp(0, 2)) for value in (hs, he - 1, ts, te - 1)],
                "head_prefix_start": pack(b * 4 + hs.clamp(0, 3)), "head_prefix_end": pack(b * 4 + he.clamp(0, 3)),
                "tail_prefix_start": pack(b * 4 + ts.clamp(0, 3)), "tail_prefix_end": pack(b * 4 + te.clamp(0, 3)),
                "head_length": pack((he - hs).clamp_min(1).float().unsqueeze(-1)),
                "tail_length": pack((te - ts).clamp_min(1).float().unsqueeze(-1)),
                "geometry": pack(torch.stack((torch.sign(delta), delta.abs() / 3), -1)), "valid": pack(valid)}
    return {"format_version": 1, "scope": "synthetic_boundary_relation_forward_vjp", "qualification": False,
            "source_commit": oracle.UPSTREAM_COMMIT, "provenance": provenance,
            "generator_sha256": oracle.sha256_file(Path(__file__)), "oracle_sha256": oracle.sha256_file(Path(oracle.__file__)),
            "source_files": {name: oracle.sha256_file(source / name) for name in (
                "gliner2/models/boundary/relations.py", "gliner2/models/boundary/validation.py")},
            "config": {"batch": 2, "text_length": 3, "hidden_size": 4, "relations": 2, "relation_query_dim": 8,
                       "pairs": 6, "dropout": 0.1, "directional_relation_states": True, "relation_biaffine_content": True},
            "inputs": {name: pack(value) for name, value in inputs.items()},
            "parameters": {name: pack(value) for name, value in owner.named_parameters()},
            "original_pairs": {**route, "pair_mask": pairs.pair_mask.tolist()}, "geometry": geometry,
            "native_geometry_index_dtype": "i32",
            "dropout_masks": {"__gliner25.dropout.relations.hidden": pack(mask)}, "cases": cases,
            "notes": ["Only synthetic task weights/states are loaded; no checkpoint or encoder is involved.",
                      "SparseRelationScorer receives original text states; its boundary_states argument name is historical.",
                      "Absolute text/prefix/query indices implement upstream clamping, while widths and positional delta use original endpoints.",
                      "Invalid pair slots return exactly zero; unmasked intermediate outputs remain observable.",
                      "The final-logits-only case proves invalid pair routes contribute no loss gradient, independently of intermediate cotangents.",
                      "Training dropout uses one caller-supplied inverted mask and claims no RNG-stream equivalence."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, default=oracle.FIXTURES / "training_relations.json")
    args = parser.parse_args()
    value = capture(args.source)
    oracle.write_json(args.output, value)
    print(f"{args.output}: {args.output.stat().st_size} bytes; {len(value['parameters'])} parameter VJPs; {len(value['cases'])} cases")


if __name__ == "__main__":
    main()
