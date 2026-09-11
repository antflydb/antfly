#!/usr/bin/env python3
"""Generate small JSON references by executing pinned upstream primitives."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import oracle


def flat(tensor: Any) -> list[Any]:
    return tensor.detach().cpu().reshape(-1).tolist()


def pool_cases(torch: Any) -> list[dict[str, Any]]:
    from gliner2.models.boundary.pool import DocumentCandidatePool
    from gliner2.models.boundary.proposal import select_top_boundaries

    class PreparedProjection(torch.nn.Module):
        """Inject projected states at the native primitive's input boundary."""

        def __init__(self, value: Any):
            super().__init__()
            self.register_buffer("value", value)

        def forward(self, states: Any) -> Any:
            if states.shape != self.value.shape:
                raise oracle.ContractError("prepared projection shape mismatch")
            return self.value

    batch, boundaries, queries, dim = 2, 6, 3, 4
    lengths = torch.tensor([5, 2])
    boundary_mask = torch.arange(boundaries)[None, :] <= lengths[:, None]
    query_mask = torch.tensor([[True, True, True], [True, False, True]])
    starts = torch.tensor([
        [[1, 5, 5, 0, -1, 4], [4, 1, 5, 2, 0, 3], [3, 2, 1, 5, 0, 4]],
        [[2, 2, 0, 10000, 10000, 10000], [10000] * 6, [-1, 3, 0, 10000, 10000, 10000]],
    ], dtype=torch.float32)
    ends = starts.flip(-1).clone()
    ends[1, 0, :3] = torch.tensor([0, 2, 4])
    ends[1, 2, :3] = torch.tensor([1, 3, 4])
    projected_starts = (torch.arange(batch * boundaries * dim, dtype=torch.float32) % 11 - 5).reshape(batch, boundaries, dim) / 8
    projected_ends = projected_starts.flip(1).clone() / 2
    cases = []
    for name in ("ragged_queries", "all_ties", "below_mask_sentinel", "no_active_queries"):
        start = starts.clone()
        end = ends.clone()
        qm = query_mask.clone()
        ps, pe = projected_starts.clone(), projected_ends.clone()
        if name == "all_ties":
            start.zero_()
            end.zero_()
            ps.zero_()
            pe.zero_()
        elif name == "below_mask_sentinel":
            # Query 0 has no masked competitors; row 1 does. The finite
            # sentinel can outrank real logits or enter a masked query's union.
            start.fill_(-20000)
            end.fill_(-20001)
            qm[1] = torch.tensor([True, True, True])
            ps.zero_()
            pe.zero_()
        elif name == "no_active_queries":
            qm[1].zero_()
        config = {"boundary_top_k": 4, "capacity": 8, "min_per_query": 2}
        pool = DocumentCandidatePool(dim, pool_boundary_top_k=config["boundary_top_k"],
                                     pool_size=config["capacity"], min_pool_per_query=config["min_per_query"])
        pool.start_projection = PreparedProjection(ps)
        pool.end_projection = PreparedProjection(pe)
        with torch.inference_mode():
            output = pool(ps, boundary_mask, qm, start, end)
            keep = boundary_mask[:, None, :] & qm[:, :, None]
            union_start = start.masked_fill(~keep, -10000).amax(1)
            union_end = end.masked_fill(~keep, -10000).amax(1)
            union_valid = boundary_mask & qm.any(-1, keepdim=True)
            _, si, sv = select_top_boundaries(union_start[:, None], union_valid[:, None], 4)
            _, ei, ev = select_top_boundaries(union_end[:, None], union_valid[:, None], 4)
        cases.append({
            "id": name, "batch": batch, "boundaries": boundaries, "queries": queries, "dim": dim,
            "lengths": flat(lengths), "query_mask": flat(qm), "boundary_mask": flat(boundary_mask),
            "start_logits": flat(start), "end_logits": flat(end),
            "projected_starts": flat(ps), "projected_ends": flat(pe), "config": config,
            "intermediates": {"union_start": flat(union_start), "union_end": flat(union_end),
                              "start_indices": flat(si), "start_valid": flat(sv),
                              "end_indices": flat(ei), "end_valid": flat(ev)},
            "expected": {"indices": flat(output.indices), "valid": flat(output.mask),
                         "proposal_logits": flat(output.proposal_logits),
                         "compat_logits": flat(output.compat_logits)},
        })
    return cases


def inside_cases(torch: Any) -> list[dict[str, Any]]:
    from gliner2.models.boundary.heads import BoundaryQueryHead

    batch, queries, length, dim = 2, 3, 5, 4
    lengths = torch.tensor([5, 2])
    mask = torch.arange(length)[None, :] < lengths[:, None]
    query_mask = torch.tensor([[True, True, True], [True, False, True]])
    logits = torch.tensor([
        [[1, 2, 4, 8, 16], [-20001, -20002, -20004, -20008, -20016], [0, -1, 0, 1, 0]],
        [[3, 9, 7000, -7000, 10000], [11, 12, 13, 14, 15], [-3, 5, 19, 20, 21]],
    ], dtype=torch.float32)
    states = torch.nn.functional.pad(logits.transpose(1, 2), (0, 1)).contiguous().requires_grad_(True)
    # D=4 makes sqrt(D)=2 exact; this projection yields the supplied logits
    # while executing the upstream mask, detached mean, and fp32 cumsum.
    query = (torch.eye(dim)[:queries] * 2)[None, :].expand(batch, -1, -1)
    head = BoundaryQueryHead(dim, dim, dropout=0).eval()
    head.inside_text_projection = torch.nn.Identity()
    head.inside_query_projection = torch.nn.Identity()
    boundary_mask = torch.arange(length + 1)[None, :] <= lengths[:, None]
    out = head(torch.zeros(batch, length + 1, dim), boundary_mask, states, mask, query, query_mask)
    grad_prefix = (torch.arange(batch * queries * (length + 1), dtype=torch.float32).reshape(batch, queries, length + 1) % 7 - 3) / 4
    # A nonzero cotangent for the detached mean must contribute no gradient.
    mean_cotangent = torch.full((batch, queries, 1), 11.0)
    loss = (out.inside_prefix * grad_prefix).sum() + (out.inside_prefix_mean * mean_cotangent).sum()
    gradient, = torch.autograd.grad(loss, states)
    return [{
        "id": "ragged_detached_mean", "batch": batch, "queries": queries, "length": length,
        "lengths": flat(lengths), "query_mask": flat(query_mask), "text_mask": flat(mask),
        "inside_logits": flat(logits), "grad_prefix": flat(grad_prefix), "grad_mean": flat(mean_cotangent),
        "expected": {"masked_logits": flat(out.inside_logits), "prefix": flat(out.inside_prefix),
                     "mean": flat(out.inside_prefix_mean), "input_gradient": flat(gradient.transpose(1, 2)[:, :queries])},
    }]


def overlap_cases() -> list[dict[str, Any]]:
    from gliner2.inference.overlap import normalize_overlap_policy, resolve_overlaps

    specifications = {
        "crossing_containment": [(0, 8, 0.75), (1, 3, 0.9), (2, 5, 0.8), (8, 9, 0.4)],
        "global_weighted": [(0, 6, 0.9), (0, 3, 0.6), (3, 6, 0.6)],
        "exact_duplicates": [(0, 2, 0.5), (0, 2, 0.9), (0, 2, 0.9), (2, 3, 0.1)],
        "zero_score_count_tie": [(0, 6, 0.0), (0, 3, 0.0), (3, 6, 0.0)],
        "equal_score_count_lex_tie": [(1, 4, 0.5), (0, 3, 0.5), (4, 6, 0.5)],
        "negative_chunk_scores": [(0, 2, -0.5), (2, 3, 0.0), (3, 5, 0.25), (0, 5, -0.1)],
    }
    rows = []
    for name, triples in specifications.items():
        items = [{"id": index, "start": start, "end": end, "score": score}
                 for index, (start, end, score) in enumerate(triples)]
        for policy in ("allow", "nested", "longest", "flat"):
            output = resolve_overlaps(items, policy, score=lambda item: item["score"],
                                      start=lambda item: item["start"], end=lambda item: item["end"])
            rows.append({"id": name + "_" + policy, "policy": policy,
                         "canonical_policy": normalize_overlap_policy(policy), "items": items,
                         "expected_ids": [item["id"] for item in output]})
    return rows


def capture(source: Path, destination: Path) -> dict[str, Any]:
    provenance, torch = oracle.prepare_runtime(source)
    report = {"format_version": 1, "scope": "boundary_primitive_reference", "provenance": provenance,
              "generator_sha256": oracle.sha256_file(Path(__file__)),
              "oracle_sha256": oracle.sha256_file(Path(oracle.__file__)),
              "manifest_sha256": oracle.sha256_file(oracle.HERE / "oracle_manifest.json"),
              "real_model_qualified": False, "native_runtime_qualified": False, "training_qualified": False,
              "layouts": {"logits": "[B,Q,N]", "projected_states": "[B,N,D]", "indices": "[B,C,2]",
                          "inside": "[B,Q,L]", "prefix": "[B,Q,L+1]", "mean": "[B,Q,1]"},
              "pool_cases": pool_cases(torch), "inside_cases": inside_cases(torch), "overlap_cases": overlap_cases()}
    oracle.verify_upstream_checkout(source)
    with oracle.atomic_output_directory(destination) as output:
        oracle.write_json(output / "primitives.json", report)
    return {"status": "captured", "file": str(destination / "primitives.json"),
            "sha256": oracle.sha256_file(destination / "primitives.json")}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new output directory")
    args = parser.parse_args()
    try:
        print(json.dumps(capture(args.upstream, args.output), sort_keys=True))
        return 0
    except (oracle.ContractError, OSError, ImportError, RuntimeError, ValueError, TypeError) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
