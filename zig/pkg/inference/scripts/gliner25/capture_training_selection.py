#!/usr/bin/env python3
"""Capture detached proposal and shared-pool selection from pinned Torch."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
from pathlib import Path
import platform
import subprocess
import sys

PIN = "3c913c7369301133d3b7699252074c4303ada50e"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, default=Path(__file__).parents[2] / "testdata/gliner25/training_selection.json")
    args = parser.parse_args()
    files = ("gliner2/models/boundary/proposal.py", "gliner2/models/boundary/pool.py", "gliner2/models/boundary/constants.py",
             "gliner2/models/boundary/indexing.py", "gliner2/models/boundary/losses.py")
    if subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.upstream, text=True).strip() != PIN:
        raise SystemExit("wrong upstream revision")
    if subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no", "--", *files], cwd=args.upstream, text=True):
        raise SystemExit("pinned selection sources have local modifications")
    sys.path.insert(0, str(args.upstream))
    import torch
    from gliner2.models.boundary.proposal import assemble_candidates, select_top_boundaries
    from gliner2.models.boundary.pool import DocumentCandidatePool
    from gliner2.models.boundary.losses import build_candidate_labels

    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)

    def flat(t):
        return t.detach().reshape(-1).tolist()

    def spans(t):
        return [dict(start=s, end=e) for s, e in t.detach().reshape(-1, 2).tolist()]

    def gold_record(pairs, mask):
        return dict(batch=pairs.shape[0], queries=pairs.shape[1], capacity=pairs.shape[2], spans=spans(pairs), valid=flat(mask))

    def requested(phase, probability, mask, seed):
        draws = torch.rand(mask.shape, generator=torch.Generator().manual_seed(seed))
        keep = mask & (draws < probability) if phase == "training" else torch.zeros_like(mask)
        return keep, flat(draws) if 0 < probability < 1 else None

    def missing(indices, valid, gold, required, pooled):
        equal = ((gold.unsqueeze(-2) == (indices[:, None, None] if pooled else indices.unsqueeze(-3))).all(-1)
                 & (valid[:, None, None] if pooled else valid.unsqueeze(-2)))
        return bool((required & ~equal.any(-1)).any())

    rows = []
    pairs = torch.tensor([
        [[[0, 1], [1, 2], [0, 1], [2, 4], [-1, -1]], [[0, 3], [1, 3], [2, 4], [0, 1], [0, 3]], [[0, 1]] * 5],
        [[[0, 1], [1, 2], [0, 2], [0, 1], [-1, -1]], [[0, 1], [1, 2], [2, 3], [0, 3], [0, 1]], [[0, 1]] * 5],
    ])
    pair_mask = torch.ones((2, 3, 5), dtype=torch.bool)
    pair_mask[:, 0, -1] = False
    query_mask = torch.tensor([[True, True, False], [True, True, False]])
    gold_pairs = torch.tensor([[[[0, 1], [3, 4]], [[1, 3], [0, 4]], [[0, 0], [0, 0]]],
                               [[[0, 3], [1, 2]], [[0, 1], [1, 3]], [[0, 0], [0, 0]]]])
    gold_mask = torch.tensor([[[True, True], [True, True], [False, False]]] * 2)
    for scenario in ("normal", "ties", "floor", "above_gold"):
        values = [((i * 13) % 17 - 8) / 2 for i in range(30)]
        if scenario == "ties":
            values = [2.0] * 30
        elif scenario == "floor":
            values = [-20000, -10000, -30000, 1, 0] * 6
        elif scenario == "above_gold":
            values = [20000, 10000, 15000, 30000, 0] * 6
        scores = torch.tensor(values, dtype=torch.float32).reshape(2, 3, 5)
        for phase, probability, capacity in (("training", 1.0, 4), ("training", 0.5, 4), ("training", 0.0, 4),
                                             ("evaluation", 1.0, 4), ("training", 1.0, 1), ("training", 1.0, 8)):
            required, draws = requested(phase, probability, gold_mask, 31)
            kwargs = dict(capacity=capacity, n_boundaries=5)
            if phase == "training":
                kwargs.update(gold_pairs=gold_pairs, gold_mask=gold_mask, gold_injection_prob=probability,
                              generator=torch.Generator().manual_seed(31))
            selected, valid, injected, pre_keys, pre_valid = assemble_candidates(pairs[..., 0], pairs[..., 1], scores, pair_mask, query_mask, **kwargs)
            labels = build_candidate_labels(selected, valid, gold_pairs, gold_mask)
            pre_hit = ((gold_pairs[..., 0] * 5 + gold_pairs[..., 1]).unsqueeze(-1) == pre_keys.unsqueeze(-2)) & pre_valid.unsqueeze(-2)
            rows.append(dict(id=f"query_{scenario}_{phase}_{probability}_{capacity}",
                             query=dict(batch=2, queries=3, proposals=5, spans=spans(pairs), scores=flat(scores), valid=flat(pair_mask),
                                        query_mask=flat(query_mask), word_counts=[4, 3]),
                             gold=gold_record(gold_pairs, gold_mask),
                             options=dict(phase=phase, capacity=capacity, gold_injection_probability=probability, injection_draws=draws),
                             native_capacity_error=missing(selected, valid, gold_pairs, required, False),
                             expected=dict(spans=spans(selected), valid=flat(valid), injected=flat(injected), gold_labels=flat(labels.bool()),
                                           gold_total=int(gold_mask.sum()), gold_hits_before_injection=int((pre_hit.any(-1) & gold_mask).sum()))))

    b, q, n, d = 2, 3, 5, 2
    states = torch.arange(b * n * d, dtype=torch.float32).reshape(b, n, d) / 10
    start_logits = torch.tensor([((i * 7) % 13 - 6) / 4 for i in range(b * q * n)]).reshape(b, q, n)
    end_logits = torch.tensor([((i * 11) % 17 - 8) / 3 for i in range(b * q * n)]).reshape(b, q, n)
    boundary_mask = torch.tensor([[True] * 5, [True] * 4 + [False]])
    union_start = start_logits.masked_fill(~(boundary_mask[:, None] & query_mask[:, :, None]), -10000).amax(1)
    union_end = end_logits.masked_fill(~(boundary_mask[:, None] & query_mask[:, :, None]), -10000).amax(1)
    union_valid = boundary_mask & query_mask.any(-1, keepdim=True)
    _, starts, starts_valid = select_top_boundaries(union_start[:, None], union_valid[:, None], 4)
    _, ends, ends_valid = select_top_boundaries(union_end[:, None], union_valid[:, None], 4)
    starts, ends, starts_valid, ends_valid = starts[:, 0], ends[:, 0], starts_valid[:, 0], ends_valid[:, 0]
    ps = starts[:, :, None].expand(b, 4, 4).reshape(b, -1)
    pe = ends[:, None, :].expand(b, 4, 4).reshape(b, -1)
    pair_valid = (starts_valid[:, :, None] & ends_valid[:, None, :] & (ends[:, None, :] > starts[:, :, None])).reshape(b, -1)
    batch_index = torch.arange(b)[:, None]
    compat = (states[batch_index, ps] * states[batch_index, pe]).sum(-1) / math.sqrt(d)
    global_scores = compat + union_start.gather(1, ps) + union_end.gather(1, pe)
    qps, qpe = ps[:, None].expand(b, q, -1), pe[:, None].expand(b, q, -1)
    query_scores = start_logits.gather(2, qps) + end_logits.gather(2, qpe) + compat[:, None]
    for quota in (0, 1, 3):
        for phase, probability, capacity in (("training", 1.0, 6), ("training", 0.5, 6), ("training", 0.0, 6),
                                             ("evaluation", 1.0, 6), ("training", 1.0, 2), ("training", 1.0, 20)):
            model = DocumentCandidatePool(d, pool_boundary_top_k=4, pool_size=capacity, min_pool_per_query=quota)
            with torch.no_grad():
                for projection in (model.start_projection, model.end_projection):
                    projection.weight.copy_(torch.eye(d))
                    projection.bias.zero_()
            kwargs = dict(return_stats=True)
            if phase == "training":
                kwargs.update(gold_pairs=gold_pairs, gold_mask=gold_mask, gold_injection_prob=probability,
                              generator=torch.Generator().manual_seed(41))
            with torch.no_grad():
                selected = model(states, boundary_mask, query_mask, start_logits, end_logits, **kwargs)
                baseline = model(states, boundary_mask, query_mask, start_logits, end_logits)
            labels = build_candidate_labels(selected.indices, selected.mask, gold_pairs, gold_mask)
            required, draws = requested(phase, probability, gold_mask, 41)
            hit = ((gold_pairs.unsqueeze(-2) == baseline.indices[:, None, None]).all(-1) & baseline.mask[:, None, None]).any(-1) & gold_mask
            rows.append(dict(id=f"document_quota{quota}_{phase}_{probability}_{capacity}",
                             document=dict(batch=b, queries=q, proposals=ps.shape[-1], spans=spans(torch.stack((ps, pe), -1)),
                                           global_scores=flat(global_scores), valid=flat(pair_valid), query_scores=flat(query_scores),
                                           query_mask=flat(query_mask), word_counts=[4, 3], min_pool_per_query=quota),
                             gold=gold_record(gold_pairs, gold_mask),
                             options=dict(phase=phase, capacity=capacity, gold_injection_probability=probability, injection_draws=draws),
                             native_capacity_error=missing(selected.indices, selected.mask, gold_pairs, required, True),
                             expected=dict(spans=spans(selected.indices), valid=flat(selected.mask), gold_labels=flat(labels.bool()),
                                           gold_total=int(gold_mask.sum()), gold_hits_before_injection=int(hit.sum()))))
    result = dict(format_version=1, provenance=dict(upstream_commit=PIN, python=platform.python_version(), torch=importlib.metadata.version("torch"),
                                                  source_sha256={name: hashlib.sha256((args.upstream / name).read_bytes()).hexdigest() for name in files}),
                  scope="detached_candidate_selection", training_qualified=False, cases=rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, allow_nan=False, separators=(",", ":")) + "\n")
    print(f"wrote {len(rows)} candidate selection cases")


if __name__ == "__main__":
    main()
