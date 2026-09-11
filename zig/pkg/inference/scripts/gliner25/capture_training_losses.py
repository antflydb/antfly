#!/usr/bin/env python3
"""Capture GLiNER2.5 loss values and logit gradients without loading a model."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import subprocess
import sys


PIN = "3c913c7369301133d3b7699252074c4303ada50e"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, default=Path(__file__).parents[2] / "testdata/gliner25/training_losses.json")
    args = parser.parse_args()
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.upstream, text=True).strip()
    if actual != PIN:
        raise SystemExit(f"wrong upstream revision: {actual}")
    source_files = ("gliner2/models/boundary/losses.py", "gliner2/models/boundary/targets_device.py", "gliner2/models/boundary/constants.py")
    dirty = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no", "--", *source_files], cwd=args.upstream, text=True)
    if dirty:
        raise SystemExit("pinned loss sources have local modifications")
    sys.path.insert(0, str(args.upstream))
    import torch
    from gliner2.models.boundary import losses
    from gliner2.models.boundary.targets_device import dense_targets_from_pairs

    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)

    def vector(t):
        return t.detach().reshape(-1).tolist()

    def scalar(t):
        return t.detach().item()

    def trainable(values, shape):
        # Public native tensors use f32. Widen the exact same f32 inputs to
        # f64 for an independent high-precision derivative oracle.
        return torch.tensor(values, dtype=torch.float32).double().reshape(shape).requires_grad_()

    def ordered(t, layout):
        return t if layout == "query_candidate" else t.transpose(1, 2).contiguous()

    def shape_record(shape, layout):
        return dict(batch=shape[0], queries=shape[1], candidates=shape[2], layout=layout)

    loss_cases = []
    dimensions = (2, 3, 5)
    raw = [-3, 0, 0.1, 1.2, 4, -1, 2, 2, 0, -4, 3, -2, 0.5, 1, -1] * 2
    gold = [0, 1, 0, 0.2, 0.8, 1, 0, 0, 0, 1, 0, 0, 1, 1, 0] * 2
    keep = [True, True, False, True, True] * 5 + [False] * 5
    query_mask = torch.tensor([[True, False, True], [True, True, False]])
    canonical_targets = torch.tensor(gold, dtype=torch.float32).double().reshape(dimensions)
    canonical_valid = torch.tensor(keep).reshape(dimensions)
    canonical_hard = torch.tensor([True, False, True, False, False] * 6).reshape(dimensions)
    for layout in ("query_candidate", "candidate_query"):
        targets = ordered(canonical_targets, layout)
        valid = ordered(canonical_valid, layout)
        hard = ordered(canonical_hard, layout)
        axis = (1, 2) if layout == "query_candidate" else (2, 1)
        for reduction in ("global", "per_query", "sum"):
            for weight in (0.0, 0.2, 1.0):
                for kind in ("bce", "focal", "pair"):
                    if kind == "pair" and weight != 1:
                        continue
                    settings = ((0.0, 2.0, 0.05), (1.0, 1.0, 0.0), (0.5, 2.5, 0.1)) if kind == "focal" else ((0.0, 2.0, 0.05),)
                    for gp, gn, clip in settings:
                        x = ordered(trainable(raw, dimensions), layout).detach().requires_grad_()
                        if kind == "pair":
                            value = losses.candidate_pair_loss(x, targets, valid, hard, query_mask=query_mask, reduction=reduction, query_axis=axis[0], candidate_axis=axis[1])
                        else:
                            # The marginal loss API always consumes canonical B,Q,N.
                            logical_x = x if layout == "query_candidate" else x.transpose(1, 2)
                            if kind == "bce":
                                value = losses.balanced_multilabel_bce(logical_x, canonical_targets, canonical_valid, query_mask=query_mask, reduction=reduction, negative_weight=weight)
                            else:
                                value = losses.asymmetric_focal_loss(logical_x, canonical_targets, canonical_valid, query_mask=query_mask, reduction=reduction, negative_weight=weight, gamma_positive=gp, gamma_negative=gn, clip=clip)
                        gradient, = torch.autograd.grad(value, x)
                        loss_cases.append(dict(kind=kind, shape=shape_record(dimensions, layout), logits=vector(x), targets=vector(targets), valid=vector(valid), hard=vector(hard) if kind == "pair" else None, query_mask=vector(query_mask), reduction=reduction, negative_weight=weight, gamma_positive=gp, gamma_negative=gn, clip=clip, expected=scalar(value), gradient=vector(gradient)))
        for scenario in ("normal", "no_gold", "all_masked", "sentinel"):
            x = ordered(trainable(raw if scenario != "sentinel" else [-20000] * 30, dimensions), layout).detach().requires_grad_()
            valid = ordered(canonical_valid if scenario != "all_masked" else torch.zeros(dimensions, dtype=torch.bool), layout)
            gold_mask = ordered((canonical_targets > 0.5) & canonical_valid if scenario not in ("no_gold", "all_masked") else torch.zeros(dimensions, dtype=torch.bool), layout)
            value = losses.proposal_listwise_loss(x, gold_mask, valid, query_mask, query_axis=axis[0], candidate_axis=axis[1])
            gradient, = torch.autograd.grad(value, x)
            loss_cases.append(dict(kind="listwise", shape=shape_record(dimensions, layout), logits=vector(x), valid=vector(valid), gold=vector(gold_mask), query_mask=vector(query_mask), expected=scalar(value), gradient=vector(gradient)))
    for kind in ("abstention", "poisson_count"):
        for empty in (False, True):
            x = trainable([-2, 0, 1, 3, -0.5, 0.25], (2, 3))
            mentions = torch.tensor([True, False, False, True, True, False, False, False, False] * 2).reshape(2, 3, 3)
            qm = query_mask if not empty else torch.zeros_like(query_mask)
            function = losses.abstention_loss if kind == "abstention" else losses.count_log_rate_loss
            value = function(x, mentions, qm)
            gradient, = torch.autograd.grad(value, x)
            loss_cases.append(dict(kind=kind, shape=shape_record((2, 3, 1), "query_candidate"), logits=vector(x), query_mask=vector(qm), mentions=vector(mentions), mention_capacity=3, expected=scalar(value), gradient=vector(gradient)))
    for kind in ("bce", "listwise"):
        x = trainable([], (2, 2, 0))
        valid = torch.zeros_like(x, dtype=torch.bool)
        qm = torch.ones((2, 2), dtype=torch.bool)
        value = losses.balanced_multilabel_bce(x, x.detach(), valid, query_mask=qm) if kind == "bce" else losses.proposal_listwise_loss(x, valid, valid, qm)
        gradient, = torch.autograd.grad(value, x)
        loss_cases.append(dict(kind=kind, shape=shape_record((2, 2, 0), "query_candidate"), logits=[], targets=[], valid=[], gold=[], query_mask=vector(qm), expected=scalar(value), gradient=vector(gradient)))

    hard_cases = []
    for layout in ("query_candidate", "candidate_query"):
        axis = (1, 2) if layout == "query_candidate" else (2, 1)
        for ratio in (0, 1, 3):
            for minimum in (0, 1, 4):
                for keep_absent in (False, True):
                    logits = ordered(trainable(raw, dimensions), layout)
                    labels = ordered(canonical_targets, layout)
                    valid = ordered(canonical_valid, layout)
                    result = losses.select_hard_negative_candidates(logits, labels, valid, negatives_per_positive=ratio, minimum_negatives=minimum, keep_all_when_no_positive=keep_absent, query_axis=axis[0], candidate_axis=axis[1])
                    hard_cases.append(dict(shape=shape_record(dimensions, layout), logits=vector(logits), labels=vector(labels), valid=vector(valid), ratio=ratio, minimum=minimum, keep_absent=keep_absent, expected=vector(result)))
        # Ties in a fully absent query preserve logical candidate order.
        logits = ordered(trainable([2, 2, 2, -1], (1, 1, 4)), layout)
        labels = torch.zeros_like(logits)
        valid = torch.ones_like(logits, dtype=torch.bool)
        result = losses.select_hard_negative_candidates(logits, labels, valid, negatives_per_positive=0, minimum_negatives=2, query_axis=axis[0], candidate_axis=axis[1])
        hard_cases.append(dict(shape=shape_record((1, 1, 4), layout), logits=vector(logits), labels=vector(labels), valid=vector(valid), ratio=0, minimum=2, keep_absent=False, expected=vector(result)))

    def span_records(tensor):
        return [dict(start=start, end=end) for start, end in tensor.reshape(-1, 2).tolist()]

    label_cases = []
    gold_pairs = torch.tensor([0, 2, 1, 3, -1, -1, 2, 4, 2, 4, -1, -1]).reshape(1, 2, 3, 2)
    gold_valid = torch.tensor([True, True, False, True, True, False]).reshape(1, 2, 3)
    candidate_pairs = torch.tensor([0, 2, 0, 1, 2, 4, -1, -1]).reshape(1, 4, 2)
    candidate_valid = torch.tensor([True, True, True, False]).reshape(1, 4)
    for pooled in (False, True):
        for layout in ("query_candidate", "candidate_query"):
            axis = (1, 2) if layout == "query_candidate" else (2, 1)
            indices = candidate_pairs if pooled else ordered(candidate_pairs.unsqueeze(1).expand(1, 2, 4, 2), layout)
            valid = candidate_valid if pooled else ordered(candidate_valid.unsqueeze(1).expand(1, 2, 4), layout)
            labels, soft = losses.build_candidate_labels(indices, valid, gold_pairs, gold_valid, return_iou=True, query_axis=axis[0], candidate_axis=axis[1])
            if pooled and layout == "query_candidate":
                # Pooled upstream output is always B,C,Q; native caller can
                # choose either explicit axis order without changing values.
                labels, soft = labels.transpose(1, 2), soft.transpose(1, 2)
            label_cases.append(dict(shape=shape_record((1, 2, 4), layout), pooled=pooled, spans=span_records(indices), valid=vector(valid), gold=span_records(gold_pairs), gold_valid=vector(gold_valid), gold_capacity=3, expected=vector(labels), soft=vector(soft)))

    consistency_cases = []
    for layout in ("query_candidate", "candidate_query"):
        for saturation in (False, True):
            pair_values = [0, 1, -2, 0.5, -1, 2] if not saturation else [20, -30, 0, 1, -2, 4]
            canonical = trainable(pair_values, (1, 2, 3))
            pair = ordered(canonical, layout).detach().requires_grad_()
            spans = torch.tensor([0, 2, 0, 1, -1, -1, 1, 3, 1, 2, 2, 3]).reshape(1, 2, 3, 2)
            valid = torch.tensor([True, True, False, True, True, True]).reshape(1, 2, 3)
            starts = trainable([0, 1, -2, 0.5, -1, 2, 0, 3], (1, 2, 4))
            ends = trainable([0.2, 0, 1, -2, 0.5, 1, 2, -1], (1, 2, 4))
            keep = torch.tensor([True, True, True, True, True, True, False, True]).reshape(1, 2, 4)
            logical_pair = pair if layout == "query_candidate" else pair.transpose(1, 2)
            value = losses.marginal_pair_consistency_loss(logical_pair, spans, valid, starts, ends, keep)
            grads = torch.autograd.grad(value, (pair, starts, ends))
            consistency_cases.append(dict(shape=shape_record((1, 2, 3), layout), logits=vector(pair), spans=span_records(ordered(spans, layout)), valid=vector(ordered(valid, layout)), starts=vector(starts), ends=vector(ends), boundary_width=4, boundary_keep=vector(keep), expected=scalar(value), pair_gradient=vector(grads[0]), start_gradient=vector(grads[1]), end_gradient=vector(grads[2])))

    dense_cases = []
    for width, spans, valid in ((5, [0, 3, 1, 2, 0, 3, -1, -1], [True, True, True, False]), (0, [-1, -1], [False]), (3, [], [])):
        pair = torch.tensor(spans, dtype=torch.long).reshape(1, 1, len(valid), 2)
        mask = torch.tensor(valid, dtype=torch.bool).reshape(1, 1, len(valid))
        starts, ends, inside = dense_targets_from_pairs(pair, mask, width)
        dense_cases.append(dict(batch=1, queries=1, capacity=len(valid), text_length=width, gold=span_records(pair), valid=vector(mask), starts=vector(starts), ends=vector(ends), inside=vector(inside)))

    result = dict(format_version=1, provenance=dict(upstream_commit=PIN, python=platform.python_version(), torch=importlib.metadata.version("torch"), dtype="float64_from_exact_float32_inputs", source_sha256={name: hashlib.sha256((args.upstream / name).read_bytes()).hexdigest() for name in source_files}), loss_cases=loss_cases, hard_cases=hard_cases, label_cases=label_cases, consistency_cases=consistency_cases, dense_cases=dense_cases)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, allow_nan=False, separators=(",", ":")) + "\n")
    print("wrote", {key: len(result[key]) for key in ("loss_cases", "hard_cases", "label_cases", "consistency_cases", "dense_cases")})


if __name__ == "__main__":
    main()
