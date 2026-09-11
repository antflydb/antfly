#!/usr/bin/env python3
"""Tiny live shared-pool forward/VJP oracle at fixed retained candidates.

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
    from gliner2.models.boundary.pool import DocumentCandidatePool, PooledCandidates, SharedPoolScorer
    from gliner2.models.boundary.indexing import gather_rows
    import torch.nn.functional as functional

    torch.manual_seed(253901)

    class StagesOneAndTwo(torch.nn.Module):
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
            h.shared_pool_scorer = SharedPoolScorer(4, 4, 4, dropout=0.1,
                candidate_attention_layers=0, candidate_attention_heads=2, query_attention_layers=0,
                enable_span_content=True, content_dim=2, content_soft_max_pool=False, text_hidden_size=4)
            h.candidate_encoder = torch.nn.Linear(8, 4)

        def forward(self, text, query, choices):
            h = self.boundary_head
            encoded = h.boundary_encoder(text, text_mask)
            margins = h.boundary_query_head(encoded.states, encoded.mask, text, text_mask, query, query_mask)
            pool_start = h.shared_pool_builder.start_projection(encoded.states)
            pool_end = h.shared_pool_builder.end_projection(encoded.states)
            starts, ends = retained_spans[..., 0], retained_spans[..., 1]
            # Exactly the live recomputation in DocumentCandidatePool.forward
            # after no-grad selection. Indices are fixed, not gold injected.
            compatibility = (gather_rows(pool_start, starts) * gather_rows(pool_end, ends)).sum(-1) / 2
            union_start = margins.start_logits.amax(1)
            union_end = margins.end_logits.amax(1)
            proposal_logits = (compatibility + union_start.gather(1, starts) + union_end.gather(1, ends)).masked_fill(~retained_valid, -10000)
            compatibility = torch.where(retained_valid, compatibility, torch.zeros_like(compatibility))
            pool = PooledCandidates(retained_spans, retained_valid, proposal_logits, None, compatibility)
            shared_logits, candidate_features = h.shared_pool_scorer(encoded.states, query, query_mask,
                pool, margins.start_logits, margins.end_logits, margins.inside_prefix, text_mask.sum(1),
                text, text_mask, inside_prefix_mean=margins.inside_prefix_mean)
            endpoints = torch.cat((gather_rows(encoded.states, starts), gather_rows(encoded.states, ends)), -1)
            candidate_states = h.candidate_encoder(endpoints) * retained_valid.unsqueeze(-1)
            self.inside_mean = margins.inside_prefix_mean.detach().clone()
            return {"boundary_states": encoded.states, "start_logits": margins.start_logits,
                    "end_logits": margins.end_logits, "inside_logits": margins.inside_logits,
                    "pool_start": pool_start, "pool_end": pool_end,
                    "null_logits": h.null_projection(query).squeeze(-1),
                    "count_logits": h.count_head(query).squeeze(-1),
                    "classification_logits": self.classifier(choices).squeeze(-1),
                    "shared_logits": shared_logits, "proposal_logits": proposal_logits,
                    "proposal_compat": compatibility, "candidate_features": candidate_features,
                    "candidate_states": candidate_states}

    model = StagesOneAndTwo()
    text_mask = torch.tensor([[1, 1, 1], [1, 0, 0]], dtype=torch.bool)
    query_mask = torch.tensor([[1, 1], [1, 0]], dtype=torch.bool)
    retained_spans = torch.tensor([[[0, 1], [0, 2], [1, 3]], [[0, 1], [0, 0], [0, 0]]], dtype=torch.long)
    retained_valid = torch.tensor([[1, 1, 1], [1, 0, 0]], dtype=torch.bool)
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
        "boundary_head.shared_pool_scorer.content_pooler.dropout": [[2, 3, 2]],
        "boundary_head.shared_pool_scorer.film_output.2": [[2, 3, 2, 64]],
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
        return {"inside_prefix_mean": pack(model.inside_mean),
                "outputs": {name: pack(value) for name, value in outputs.items()},
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
                    "gliner2/models/boundary/pool.py", "gliner2/models/boundary/content.py",
                    "gliner2/models/boundary/indexing.py", "gliner2/layers.py"]
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
        "boundary_head.shared_pool_scorer.content_pooler": ("boundary_head.shared_pool_scorer.content_pooler.dropout/call_0", [6, 2]),
        "shared_pool.film_hidden": ("boundary_head.shared_pool_scorer.film_output.2/call_0", [12, 64]),
    }
    lengths = (retained_spans[..., 1] - retained_spans[..., 0]).clamp_min(1).float()
    length_features = torch.stack((torch.log1p(lengths), lengths / text_mask.sum(1).view(2, 1), torch.rsqrt(lengths)), -1)
    offsets = torch.arange(2).reshape(2, 1) * 4
    geometry = {"spans": pack(retained_spans), "valid": pack(retained_valid),
                "starts": pack((retained_spans[..., 0] + offsets).reshape(-1)),
                "ends": pack((retained_spans[..., 1] + offsets).reshape(-1)),
                "lengths": pack(lengths.reshape(6, 1)), "length_features": pack(length_features.reshape(6, 3))}
    return {"format_version": 1, "scope": "synthetic_boundary_shared_pool_forward_vjp", "qualification": False,
            "source_commit": oracle.UPSTREAM_COMMIT, "provenance": provenance,
            "generator_sha256": oracle.sha256_file(Path(__file__)), "oracle_sha256": oracle.sha256_file(Path(oracle.__file__)),
            "source_files": {name: oracle.sha256_file(source / name) for name in source_files},
            "config": {"batch": 2, "text_length": 3, "queries": 2, "classifications": 3, "hidden_size": 4,
                       "boundary_dim": 4, "dropout": 0.1, "attention_heads": 2, "attention_layers": 1,
                       "attention_window": 1, "refinement_layers": 1, "ffn_multiplier": 2,
                       "candidates": 3, "pair_dim": 4, "content_dim": 2, "enable_span_content": True,
                       "content_soft_max_pool": False, "candidate_attention_layers": 0, "query_attention_layers": 0,
                       "use_inside_evidence": True, "enable_records": True},
            "inputs": {name: pack(value) for name, value in inputs.items()},
            "text_mask": pack(text_mask), "query_mask": pack(query_mask), "text_lengths": [3, 1],
            "geometry": geometry,
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
                      "Retained spans are fixed input decisions; no top-k, gold injection or discrete selection is differentiated.",
                      "Proposal compatibility and query-union marginals are recomputed live, as in DocumentCandidatePool.forward.",
                      "Inside centering mean is detached by pinned upstream code; each case records its own value as external geometry.",
                      "Shared-pool scoring uses the genuine upstream SharedPoolScorer with span content and FiLM dropout.",
                      "Candidate states are the live masked endpoint encoder used by record training; record and relation scorers are outside this fixture."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, default=oracle.FIXTURES / "training_candidates.json")
    args = parser.parse_args()
    value = capture(args.source)
    oracle.write_json(args.output, value)
    print(f"{args.output}: {args.output.stat().st_size} bytes; {len(value['parameters'])} parameter VJPs; two cases")


if __name__ == "__main__":
    main()
