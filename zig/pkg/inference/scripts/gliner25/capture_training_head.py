#!/usr/bin/env python3
"""Tiny pinned boundary-stage forward/VJP oracle with explicit dropout masks.

No checkpoint, tokenizer, encoder or corpus is loaded. Training SDPA is expanded
only to replace its implicit random dropout with externally supplied masks.
The zero-dropout expansion is checked against genuine SDPA forward and VJP.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
from pathlib import Path
from typing import Any

import oracle


def capture(source: Path) -> dict[str, Any]:
    provenance, torch = oracle.prepare_runtime(source)
    from gliner2.layers import create_mlp
    from gliner2.models.boundary.encoding import BoundaryEncoder
    from gliner2.models.boundary.heads import BoundaryQueryHead
    from gliner2.models.boundary.pool import DocumentCandidatePool
    import torch.nn.functional as functional

    torch.manual_seed(253901)

    class StageOne(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.boundary_head = torch.nn.Module()
            h = self.boundary_head
            h.boundary_encoder = BoundaryEncoder(4, 4, dropout=0.1, refinement_layers=1,
                                                  ffn_multiplier=2, attention_layers=1,
                                                  attention_heads=2, attention_window=1)
            h.boundary_query_head = BoundaryQueryHead(4, 4, dropout=0.1)
            h.shared_pool_builder = DocumentCandidatePool(4, pool_boundary_top_k=4, pool_size=8, min_pool_per_query=2)
            # Nonzero synthetic parameters exercise both input and weight
            # derivatives; these are not pretrained checkpoint values.
            h.null_projection = torch.nn.Linear(4, 1)
            h.count_head = torch.nn.Linear(4, 1)
            self.classifier = create_mlp(4, [8], 1, dropout=0.1, activation="relu", add_layer_norm=False)

        def forward(self, text, query, choices):
            h = self.boundary_head
            encoded = h.boundary_encoder(text, text_mask)
            margins = h.boundary_query_head(encoded.states, encoded.mask, text, text_mask, query, query_mask)
            return {"boundary_states": encoded.states, "start_logits": margins.start_logits,
                    "end_logits": margins.end_logits, "inside_logits": margins.inside_logits,
                    "pool_start": h.shared_pool_builder.start_projection(encoded.states),
                    "pool_end": h.shared_pool_builder.end_projection(encoded.states),
                    "null_logits": h.null_projection(query).squeeze(-1),
                    "count_logits": h.count_head(query).squeeze(-1),
                    "classification_logits": self.classifier(choices).squeeze(-1)}

    model = StageOne()
    text_mask = torch.tensor([[1, 1, 1], [1, 0, 0]], dtype=torch.bool)
    query_mask = torch.tensor([[1, 1], [1, 0]], dtype=torch.bool)
    inputs = {"text_states": (torch.arange(24, dtype=torch.float32).reshape(2, 3, 4) - 11) / 11,
              "query_states": torch.cos(torch.arange(16, dtype=torch.float32).reshape(2, 2, 4) * 0.37),
              "classification_states": torch.sin(torch.arange(12, dtype=torch.float32).reshape(3, 4) * 0.23)}

    def pack(tensor):
        tensor = tensor.detach().cpu().contiguous()
        if tensor.is_floating_point() and not bool(torch.isfinite(tensor).all()):
            raise oracle.ContractError("nonfinite differentiable head reference")
        return {"shape": list(tensor.shape), "values": tensor.reshape(-1).tolist()}

    # Every mask shape is declared before model execution and depends only on
    # the fixed graph, never on tensor values, labels or output predictions.
    shapes = {
        "boundary_head.boundary_encoder.dropout": [[2, 4, 4]],
        "boundary_head.boundary_encoder.attention_blocks.0.dropout": [[2, 4, 4]],
        "boundary_head.boundary_encoder.refinement_blocks.0.dropout": [[2, 4, 8], [2, 4, 4]],
        "boundary_head.boundary_query_head.dropout": [[2, 4, 4], [2, 4, 4], [2, 3, 4]],
        "classifier.2": [[3, 8]],
        "boundary_head.boundary_encoder.attention_blocks.0.sdpa_probabilities": [[2, 2, 4, 4]],
    }
    masks = {}
    for name, calls in shapes.items():
        for index, shape in enumerate(calls):
            site = f"{name}/call_{index}"
            seed = int.from_bytes(hashlib.sha256(site.encode()).digest()[:4], "little")
            count = 1
            for width in shape:
                count *= width
            mask = torch.tensor([0.0 if (element * 7 + seed) % 10 == 0 else 1 / 0.9 for element in range(count)], dtype=torch.float32)
            masks[site] = mask.reshape(shape)

    @contextmanager
    def explicit_dropout(training):
        originals = {}
        calls = {}
        original_sdpa = functional.scaled_dot_product_attention

        def apply(name, value):
            index = calls.get(name, 0)
            calls[name] = index + 1
            site = f"{name}/call_{index}"
            mask = masks.get(site)
            if mask is None or mask.shape != value.shape:
                raise oracle.ContractError(f"unexpected dropout route {site}: {tuple(value.shape)}")
            return value * mask

        def sdpa(query, key, value, attn_mask=None, dropout_p=0.0, is_causal=False, scale=None, enable_gqa=False):
            if is_causal or enable_gqa or attn_mask is None or attn_mask.dtype != torch.bool or dropout_p not in (0.0, 0.1):
                raise oracle.ContractError("unexpected boundary SDPA contract")
            if scale is None:
                scale = query.shape[-1] ** -0.5
            probabilities = torch.softmax((query @ key.transpose(-2, -1) * scale).masked_fill(~attn_mask, -torch.inf), dim=-1)
            if dropout_p:
                probabilities = apply("boundary_head.boundary_encoder.attention_blocks.0.sdpa_probabilities", probabilities)
            return probabilities @ value

        try:
            for name, module in model.named_modules():
                if isinstance(module, torch.nn.Dropout):
                    if name not in shapes or module.p != 0.1 or module.inplace:
                        raise oracle.ContractError(f"unrecognized dropout site: {name}")
                    originals[module] = module.forward
                    module.forward = lambda value, name=name: apply(name, value) if training else value
            functional.scaled_dot_product_attention = sdpa
            yield
            expected = {name: len(values) for name, values in shapes.items()} if training else {}
            if calls != expected:
                raise oracle.ContractError(f"dropout mask consumption differs: {calls}")
        finally:
            functional.scaled_dot_product_attention = original_sdpa
            for module, forward in originals.items():
                module.forward = forward

    def evaluate():
        leaves = {name: values.detach().clone().requires_grad_() for name, values in inputs.items()}
        outputs = model(*leaves.values())
        cotangents = {}
        objective = torch.tensor(0.0)
        for name, value in outputs.items():
            seed = int.from_bytes(hashlib.sha256(name.encode()).digest()[:2], "little")
            cotangent = (((torch.arange(value.numel()) + seed) % 17).float() - 8).reshape(value.shape) / 13
            cotangents[name] = cotangent
            objective = objective + (value * cotangent).sum()
        parameters = dict(model.named_parameters())
        gradients = torch.autograd.grad(objective, [*leaves.values(), *parameters.values()], allow_unused=False)
        return {"outputs": {name: pack(value) for name, value in outputs.items()},
                "cotangents": {name: pack(value) for name, value in cotangents.items()},
                "input_gradients": {name: pack(value) for name, value in zip(leaves, gradients[:len(leaves)])},
                "parameter_gradients": {name: pack(value) for name, value in zip(parameters, gradients[len(leaves):])},
                "objective": objective.detach().item()}

    model.eval()
    genuine = evaluate()
    with explicit_dropout(False):
        expanded = evaluate()
    discrepancies = {}
    for group in ("outputs", "input_gradients", "parameter_gradients"):
        maximum = 0.0
        for name in genuine[group]:
            left = torch.tensor(genuine[group][name]["values"])
            right = torch.tensor(expanded[group][name]["values"])
            maximum = max(maximum, float((left - right).abs().max()))
        discrepancies[group] = maximum
        if maximum > 3e-6:
            raise oracle.ContractError(f"explicit zero-dropout SDPA differs from genuine SDPA {group}: {maximum}")
    model.train()
    with explicit_dropout(True):
        trained = evaluate()
    with explicit_dropout(True):
        repeated = evaluate()
    if trained != repeated:
        raise oracle.ContractError("external dropout replay is not deterministic")
    source_files = ["gliner2/models/boundary/encoding.py", "gliner2/models/boundary/heads.py",
                    "gliner2/models/boundary/pool.py", "gliner2/layers.py"]
    native_sites = {
        "boundary_encoder.output": ("boundary_head.boundary_encoder.dropout/call_0", [8, 4]),
        "boundary_encoder.attention.0.probabilities": ("boundary_head.boundary_encoder.attention_blocks.0.sdpa_probabilities/call_0", [4, 4, 4]),
        "boundary_encoder.attention.0.output": ("boundary_head.boundary_encoder.attention_blocks.0.dropout/call_0", [8, 4]),
        "boundary_encoder.refinement.0.hidden": ("boundary_head.boundary_encoder.refinement_blocks.0.dropout/call_0", [8, 8]),
        "boundary_encoder.refinement.0.output": ("boundary_head.boundary_encoder.refinement_blocks.0.dropout/call_1", [8, 4]),
        "marginals.start": ("boundary_head.boundary_query_head.dropout/call_0", [8, 4]),
        "marginals.end": ("boundary_head.boundary_query_head.dropout/call_1", [8, 4]),
        "marginals.inside": ("boundary_head.boundary_query_head.dropout/call_2", [6, 4]),
        "classifier": ("classifier.2/call_0", [3, 8]),
    }
    return {"format_version": 1, "scope": "synthetic_boundary_stage_one_forward_vjp", "qualification": False,
            "source_commit": oracle.UPSTREAM_COMMIT, "provenance": provenance,
            "generator_sha256": oracle.sha256_file(Path(__file__)), "oracle_sha256": oracle.sha256_file(Path(oracle.__file__)),
            "source_files": {name: oracle.sha256_file(source / name) for name in source_files},
            "config": {"batch": 2, "text_length": 3, "queries": 2, "classifications": 3, "hidden_size": 4,
                       "boundary_dim": 4, "dropout": 0.1, "attention_heads": 2, "attention_layers": 1,
                       "attention_window": 1, "refinement_layers": 1, "ffn_multiplier": 2},
            "inputs": {name: pack(value) for name, value in inputs.items()},
            "text_mask": pack(text_mask), "query_mask": pack(query_mask), "text_lengths": [3, 1],
            "parameters": {name: pack(value) for name, value in model.named_parameters()},
            "dropout_masks": {name: pack(value) for name, value in masks.items()},
            "native_dropout_routes": {"__gliner25.dropout." + name: {"source_mask": source_mask, "shape": shape} for name, (source_mask, shape) in native_sites.items()},
            "zero_dropout_sdpa_max_absolute_differences": discrepancies,
            "cases": [{"id": "eval", "training": False, "sdpa": "genuine_upstream_cpu", **genuine},
                      {"id": "train_external_masks", "training": True, "sdpa": "expanded_explicit_dropout", **trained}],
            "notes": ["No checkpoint or encoder is loaded; every parameter is synthetic.",
                      "Null/count synthetic parameters are nonzero to exercise input and weight VJPs.",
                      "Cotangents are independent of masks/gold and include inactive output positions; masked operations decide their VJP.",
                      "Training SDPA dropout is replaced by explicitly supplied probability masks; no training RNG equivalence is claimed.",
                      "Discrete candidate selection and sparse reranking/record/relation heads are outside this stage-one fixture."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, default=oracle.FIXTURES / "training_head.json")
    args = parser.parse_args()
    value = capture(args.source)
    oracle.write_json(args.output, value)
    print(f"{args.output}: {args.output.stat().st_size} bytes; {len(value['parameters'])} parameter VJPs; two cases")


if __name__ == "__main__":
    main()
