#!/usr/bin/env python3
"""Pinned explicit-span scorer VJPs at fixed typed routes; no encoder load."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import oracle


def capture(source: Path):
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
    return {"format_version": 1, "scope": "synthetic_boundary_explicit_forward_vjp", "qualification": False,
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, default=oracle.FIXTURES / "training_explicit.json")
    args = parser.parse_args()
    value = capture(args.source)
    oracle.write_json(args.output, value)
    print(f"{args.output}: {args.output.stat().st_size} bytes; {len(value['parameters'])} parameter VJPs; two cases")


if __name__ == "__main__":
    main()
