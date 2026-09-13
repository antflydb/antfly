#!/usr/bin/env python3
"""Capture synthetic GLiNER2.5 head forwards and VJPs from pinned Torch.

Choose one head per process; no pretrained checkpoint or encoder is loaded.
Each profile retains its original weights, masks, cases and numerical checks.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
from typing import Any

import oracle


def share_cotangents(report):
    """Store identical per-case derivative seeds once without changing values."""
    report["cotangents"] = report["cases"][0]["cotangents"]
    common = json.dumps(report["cotangents"], sort_keys=True, allow_nan=False)
    for case in report["cases"]:
        if json.dumps(case["cotangents"], sort_keys=True, allow_nan=False) == common:
            del case["cotangents"]
    return report


def capture_boundary(source: Path) -> dict[str, Any]:
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
    report = {"format_version": 1, "scope": "synthetic_boundary_stage_one_forward_vjp", "qualification": False,
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
    return share_cotangents(report)


def capture_explicit(source: Path):
    provenance, torch = oracle.prepare_runtime(source)
    from gliner2.models.boundary.proposal import BoundaryProposals, ProposalSettings, SparseBoundaryProposer
    from gliner2.models.boundary.scoring import SparseBoundaryPairScorer, continuous_length_features

    torch.manual_seed(253923)
    owner = torch.nn.Module()
    owner.boundary_head = torch.nn.Module()
    head = owner.boundary_head
    settings = ProposalSettings(4, 4, 4, 4, 8, 8, 4, 4, enable_rotary_endpoints=True)
    head.boundary_proposer = SparseBoundaryProposer(4, 4, settings)
    head.pair_scorer = SparseBoundaryPairScorer(4, 4, 4, use_inside_evidence=True, dropout=.1,
        enable_span_content=True, content_dim=2, content_soft_max_pool=False, enable_rotary_endpoints=True,
        rotary_base=10000, query_conditioned_inside_weight=True, endpoint_difference_features=True,
        reranker_endpoint_compat=True, multihead_pair_compat_heads=2, content_hidden_size=4)
    text_mask = torch.tensor([[1, 1, 1], [1, 0, 0]], dtype=torch.bool)
    query_mask = torch.tensor([[1, 1], [1, 0]], dtype=torch.bool)
    lengths = text_mask.sum(1)
    keep = text_mask.unsqueeze(1) & query_mask.unsqueeze(-1)
    boundary_keep = (torch.arange(4).reshape(1, 4) <= lengths.unsqueeze(1)).unsqueeze(1) & query_mask.unsqueeze(-1)
    spans = torch.tensor([[[[0, 1], [0, 2], [1, 3]], [[1, 2], [0, 3], [0, 0]]],
                          [[[0, 1], [0, 0], [0, 0]], [[0, 1], [0, 0], [0, 0]]]], dtype=torch.long)
    starts, ends = spans[..., 0], spans[..., 1]
    valid = (ends > starts) & (ends <= lengths.view(2, 1, 1)) & query_mask.unsqueeze(-1)
    inputs = {"boundary_states": torch.sin(torch.arange(32, dtype=torch.float32).reshape(2, 4, 4) * .21),
              "text_states": (torch.arange(24, dtype=torch.float32).reshape(2, 3, 4) - 9) / 7,
              "query_states": torch.cos(torch.arange(16, dtype=torch.float32).reshape(2, 2, 4) * .43),
              "start_logits": torch.sin(torch.arange(16, dtype=torch.float32).reshape(2, 2, 4) * .37).masked_fill(~boundary_keep, -10000),
              "end_logits": torch.cos(torch.arange(16, dtype=torch.float32).reshape(2, 2, 4) * .27).masked_fill(~boundary_keep, -10000),
              "inside_logits": torch.sin(torch.arange(12, dtype=torch.float32).reshape(2, 2, 3) * .47).masked_fill(~keep, -10000)}
    shapes = {"boundary_head.pair_scorer.dropout": [[2, 2, 3, 4], [2, 2, 3, 4]],
              "boundary_head.pair_scorer.content_pooler.dropout": [[2, 2, 3, 2]]}
    masks = {}
    for name, calls in shapes.items():
        for index, shape in enumerate(calls):
            site = f"{name}/call_{index}"
            seed = int.from_bytes(hashlib.sha256(site.encode()).digest()[:4], "little")
            count = 1
            for width in shape:
                count *= width
            masks[site] = torch.tensor([0.0 if (i * 7 + seed) % 10 == 0 else 1 / .9 for i in range(count)], dtype=torch.float32).reshape(shape)
    originals = {}
    counts = {}

    def apply(name, value):
        if not owner.training:
            return value
        count = counts.get(name, 0)
        counts[name] = count + 1
        mask = masks.get(f"{name}/call_{count}")
        if mask is None or value.shape != mask.shape:
            raise oracle.ContractError("unexpected explicit-scorer dropout route")
        return value * mask

    for name, module in owner.named_modules():
        if isinstance(module, torch.nn.Dropout):
            if name not in shapes or module.p != .1:
                raise oracle.ContractError("unrecognized explicit-scorer dropout")
            originals[module] = module.forward
            module.forward = lambda value, name=name: apply(name, value)

    def pack(value):
        value = value.detach().cpu().contiguous()
        if value.is_floating_point() and not bool(torch.isfinite(value).all()):
            raise oracle.ContractError("nonfinite explicit-scorer capture")
        return {"shape": list(value.shape), "values": value.reshape(-1).tolist()}

    def evaluate(training):
        owner.train(training)
        counts.clear()
        leaves = {name: value.detach().clone().requires_grad_() for name, value in inputs.items()}
        inside = leaves["inside_logits"].masked_fill(~keep, 0.0).float()
        mean = (inside.sum(-1, keepdim=True) / keep.sum(-1, keepdim=True).clamp_min(1)).detach()
        centered = (inside - mean) * keep
        prefix = torch.cat((torch.zeros(2, 2, 1), centered.cumsum(-1)), -1)
        prior = head.boundary_proposer.score_explicit_pairs(leaves["boundary_states"], leaves["query_states"], spans, valid)
        proposals = BoundaryProposals(indices=spans, logits=None, valid_mask=valid, compat_logits=prior)
        logits = head.pair_scorer(leaves["boundary_states"], leaves["query_states"], proposals,
            leaves["start_logits"], leaves["end_logits"], prefix, lengths, leaves["text_states"], text_mask,
            inside_prefix_mean=mean)
        if counts != ({name: len(calls) for name, calls in shapes.items()} if training else {}):
            raise oracle.ContractError("explicit-scorer dropout mask consumption differs")
        outputs = {"logits": logits, "proposal_compat": prior}
        cotangents = {}
        objective = torch.tensor(0.0)
        for name, value in outputs.items():
            seed = int.from_bytes(hashlib.sha256(name.encode()).digest()[:2], "little")
            cotangent = (((torch.arange(value.numel()) + seed) % 17).float() - 8).reshape(value.shape) / 13
            cotangents[name] = cotangent
            objective = objective + (value * cotangent).sum()
        parameters = dict(owner.named_parameters())
        gradients = torch.autograd.grad(objective, [*leaves.values(), *parameters.values()], allow_unused=False)
        return {"id": "train_external_masks" if training else "eval", "training": training,
                "inside_prefix_mean": pack(mean), "outputs": {name: pack(value) for name, value in outputs.items()},
                "cotangents": {name: pack(value) for name, value in cotangents.items()},
                "input_gradients": {name: pack(value) for name, value in zip(leaves, gradients[:len(leaves)])},
                "parameter_gradients": {name: pack(value) for name, value in zip(parameters, gradients[len(leaves):])}}

    try:
        cases = [evaluate(False), evaluate(True)]
        if evaluate(True) != cases[1]:
            raise oracle.ContractError("explicit dropout replay differs")
    finally:
        for module, forward in originals.items():
            module.forward = forward
    boundary_offset = torch.arange(2).reshape(2, 1, 1) * 4
    marginal_offset = torch.arange(4).reshape(2, 2, 1) * 4
    geometry = {"spans": pack(spans), "valid": pack(valid), "starts": pack((starts + boundary_offset).reshape(-1)),
                "ends": pack((ends + boundary_offset).reshape(-1)),
                "marginal_starts": pack((starts + marginal_offset).reshape(-1)),
                "marginal_ends": pack((ends + marginal_offset).reshape(-1)),
                "lengths": pack((ends - starts).clamp_min(1).float().reshape(12, 1)),
                "length_features": pack(continuous_length_features(starts, ends, lengths).reshape(12, 3))}
    native_sites = {"explicit.start": ("boundary_head.pair_scorer.dropout/call_0", [12, 4]),
                    "explicit.end": ("boundary_head.pair_scorer.dropout/call_1", [12, 4]),
                    "boundary_head.pair_scorer.content_pooler": ("boundary_head.pair_scorer.content_pooler.dropout/call_0", [12, 2])}
    report = {"format_version": 1, "scope": "synthetic_boundary_explicit_forward_vjp", "qualification": False,
            "source_commit": oracle.UPSTREAM_COMMIT, "provenance": provenance,
            "generator_sha256": oracle.sha256_file(Path(__file__)), "oracle_sha256": oracle.sha256_file(Path(oracle.__file__)),
            "source_files": {name: oracle.sha256_file(source / name) for name in (
                "gliner2/models/boundary/proposal.py", "gliner2/models/boundary/scoring.py",
                "gliner2/models/boundary/content.py", "gliner2/models/boundary/rotary.py", "gliner2/models/boundary/indexing.py")},
            "config": {"batch": 2, "text_length": 3, "hidden_size": 4, "boundary_dim": 4, "pair_dim": 4, "queries": 2,
                "capacity": 3, "content_dim": 2, "dropout": .1, "enable_span_content": True, "content_soft_max_pool": False,
                "enable_rotary_endpoints": True, "rotary_base": 10000, "query_conditioned_inside_weight": True,
                "endpoint_difference_features": True, "reranker_endpoint_compat": True, "multihead_pair_compat_heads": 2},
            "inputs": {name: pack(value) for name, value in inputs.items()},
            "parameters": {name: pack(value) for name, value in owner.named_parameters()},
            "text_mask": pack(text_mask), "query_mask": pack(query_mask), "text_lengths": lengths.tolist(),
            "geometry": geometry, "native_geometry_index_dtype": "i32",
            "dropout_masks": {name: pack(value) for name, value in masks.items()},
            "native_dropout_routes": {"__gliner25.dropout." + name: {"source_mask": source_mask, "shape": shape}
                                       for name, (source_mask, shape) in native_sites.items()}, "cases": cases,
            "notes": ["The actual upstream explicit proposer and pair scorer consume six independent live state/marginal inputs.",
                      "All routes are fixed; no discrete candidate selection or encoder execution occurs.",
                      "Adjacent-pair rotary gates, multihead compatibility, endpoint differences, content and query-conditioned inside weights are enabled.",
                      "The pinned inside-centering mean is detached; each case retains it for native graph binding.",
                      "Inactive query and padded candidate outputs receive cotangents but their masks determine zero VJPs.",
                      "Training uses explicit inverted masks without claiming RNG-stream equivalence."]}
    return share_cotangents(report)


def capture_records(source: Path):
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
    report = {"format_version": 1, "scope": "synthetic_boundary_dense_record_forward_vjp", "qualification": False,
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
    return share_cotangents(report)


def capture_relations(source: Path):
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
    report = {"format_version": 1, "scope": "synthetic_boundary_relation_forward_vjp", "qualification": False,
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
    return share_cotangents(report)


CAPTURES = {
    "boundary": (capture_boundary, "training_head.json"),
    "explicit": (capture_explicit, "training_explicit.json"),
    "records": (capture_records, "training_records.json"),
    "relations": (capture_relations, "training_relations.json"),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("head", choices=CAPTURES)
    parser.add_argument("--source", "--upstream", dest="source", type=Path,
                        default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    capture, filename = CAPTURES[args.head]
    output = args.output or oracle.FIXTURES / filename
    value = capture(args.source)
    output.parent.mkdir(parents=True, exist_ok=True)
    oracle.write_json(output, value)
    print(f"{output}: {output.stat().st_size} bytes; {len(value['parameters'])} parameter VJPs; {len(value['cases'])} cases")


if __name__ == "__main__":
    main()
