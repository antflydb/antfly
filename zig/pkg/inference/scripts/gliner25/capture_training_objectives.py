#!/usr/bin/env python3
"""Capture GLiNER2.5 losses, candidate selection and record matching.

Choose one profile per process. Fixtures retain their independent pinned Torch
values, gradients, assignment decisions and source identities without a model.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.metadata
import json
import math
from pathlib import Path
import platform
import subprocess
import sys
from types import SimpleNamespace

import oracle

PIN = oracle.UPSTREAM_COMMIT


def prepare_source(source: Path, files):
    """Check the original profile's source set before importing pinned Torch."""
    if subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip() != PIN:
        raise SystemExit("wrong upstream revision")
    if subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no", "--", *files], cwd=source, text=True):
        raise SystemExit("pinned training sources have local modifications")
    sys.path.insert(0, str(source))
    import torch
    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)
    return torch


def capture_losses(source: Path):
    source_files = ("gliner2/models/boundary/losses.py", "gliner2/models/boundary/targets_device.py", "gliner2/models/boundary/constants.py")
    torch = prepare_source(source, source_files)
    from gliner2.models.boundary import losses
    from gliner2.models.boundary.targets_device import dense_targets_from_pairs


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

    result = dict(format_version=1, provenance=dict(upstream_commit=PIN, python=platform.python_version(), torch=importlib.metadata.version("torch"), dtype="float64_from_exact_float32_inputs", source_sha256={name: hashlib.sha256((source / name).read_bytes()).hexdigest() for name in source_files}), loss_cases=loss_cases, hard_cases=hard_cases, label_cases=label_cases, consistency_cases=consistency_cases, dense_cases=dense_cases)
    return result


def capture_selection(source: Path):
    files = ("gliner2/models/boundary/proposal.py", "gliner2/models/boundary/pool.py", "gliner2/models/boundary/constants.py",
             "gliner2/models/boundary/indexing.py", "gliner2/models/boundary/losses.py")
    torch = prepare_source(source, files)
    from gliner2.models.boundary.proposal import assemble_candidates, select_top_boundaries
    from gliner2.models.boundary.pool import DocumentCandidatePool
    from gliner2.models.boundary.losses import build_candidate_labels


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
                                                  source_sha256={name: hashlib.sha256((source / name).read_bytes()).hexdigest() for name in files}),
                  scope="detached_candidate_selection", training_qualified=False, cases=rows)
    return result


def capture_matching(source: Path):
    files = ("gliner2/models/boundary/records.py", "gliner2/models/boundary/constants.py", "gliner2/training/matching.py",
             "gliner2/processing/records.py", "gliner2/processing/targets.py")
    torch = prepare_source(source, files)
    from gliner2.models.boundary.records import _dense_gold_indicator
    from gliner2.processing.records import FieldCardinality, RecordFieldSpec
    from gliner2.processing.targets import RecordFieldTarget, RecordTarget
    from gliner2.training.matching import build_dense_record_matching_cost, linear_sum_assignment


    def flat(t):
        return t.detach().reshape(-1).tolist()

    rows = []
    for case in fixtures():
        layout = case["layout"]
        field_specs = [RecordFieldSpec(query_id=f["query"], name=f"field{i}", role_index=i,
                                       cardinality=FieldCardinality(f["cardinality"])) for i, f in enumerate(layout["fields"])]
        records, record_indices = [], []
        for i, raw in enumerate(case["records"]):
            if raw["structure"] != layout["structure"]:
                continue
            records.append(RecordTarget(instance_id=raw["id"], task_index=raw["group"], anchor_query_id=raw["anchor_query"],
                                        fields=tuple(RecordFieldTarget(query_id=f["query"], values=tuple(tuple((s["start"], s["end"]) for s in v["alternatives"]) for v in f["values"])) for f in raw["fields"])))
            record_indices.append(i)
        nc, nf, ni = len(layout["candidate_spans"]), len(field_specs), len(layout["instance_mask"])
        pool_spans = torch.tensor([[s["start"], s["end"]] for s in layout["candidate_spans"]])
        membership = torch.tensor(layout["field_membership"]).reshape(nf, nc)
        group = SimpleNamespace(field_specs=field_specs, pool_spans=pool_spans, field_membership=membership)
        indicator = _dense_gold_indicator(group, records)
        scalar = torch.tensor([f.cardinality.is_scalar for f in field_specs])
        mask = torch.tensor(layout["instance_mask"])
        obj32 = torch.tensor(case["logits"]["objects"], dtype=torch.float32)
        assign32 = torch.tensor(case["logits"]["assignments"], dtype=torch.float32).reshape(ni, nf, nc + 1)
        assign32[..., 1:] = assign32[..., 1:].masked_fill(~membership[None], -10000)
        costs64 = build_dense_record_matching_cost(obj32.double(), assign32.double(), indicator, scalar, mask)
        costs32 = build_dense_record_matching_cost(obj32, assign32, indicator, scalar, mask)
        case["expected"] = dict(indicator=flat(indicator), record_indices=record_indices, costs=flat(costs64), graph_costs=flat(costs32))

        def matches(costs):
            if not records:
                pairs = []
            elif layout["mode"] == "natural":
                anchor_index = next(i for i, f in enumerate(field_specs) if f.query_id == layout["anchor_query"])
                seeds = layout["anchor_candidates"]
                pairs = []
                for g in range(len(records)):
                    candidates = [(seeds[i], i) for i in range(ni) if mask[i] and seeds[i] is not None and indicator[g, anchor_index, seeds[i]]]
                    _, i = min(candidates)
                    pairs.append(dict(instance=i, record=g, annotation=record_indices[g]))
            else:
                active = torch.nonzero(mask).flatten()
                # Strict supervision correction: masked hypotheses cannot take
                # a gold column and then disappear during post-match filtering.
                matched_rows, cols = linear_sum_assignment(costs[active])
                pairs = [dict(instance=int(active[r]), record=int(c), annotation=record_indices[int(c)]) for r, c in zip(matched_rows, cols)]
            obj_target = [0.0] * ni
            if layout["mode"] != "natural":
                for pair in pairs:
                    obj_target[pair["instance"]] = 1.0
            return dict(pairs=pairs, object_targets=obj_target, object_mask=[active and layout["mode"] != "natural" for active in layout["instance_mask"]])

        if "native_error" not in case:
            case["expected"].update(matches(costs64))
            case["expected"]["graph_pairs"] = matches(costs32)["pairs"]
        rows.append(case)
    report = dict(format_version=1, provenance=dict(upstream_commit=PIN, python=platform.python_version(), torch=importlib.metadata.version("torch"),
                                                  source_sha256={name: hashlib.sha256((source / name).read_bytes()).hexdigest() for name in files}),
                  scope="record_targets_costs_and_detached_matching", matching_domain="valid_hypotheses_only",
                  training_qualified=False, cases=rows)
    return report


def span(start, end):
    return dict(start=start, end=end)


def field(index, query, *values):
    return dict(field=index, query=query, values=[dict(alternatives=[span(s, e) for s, e in value]) for value in values])


def record(identity, actor, tags):
    return dict(structure=0, group=2, id=identity, anchor_query=None,
                fields=[field(0, 3, actor), field(1, 7, *tags)])


def fixtures():
    base = dict(
        layout=dict(structure=0, group=2, mode="latent", word_count=5,
                    fields=[dict(query=3, cardinality="required_one"), dict(query=7, cardinality="zero_or_more")],
                    candidate_spans=[span(i, i + 1) for i in range(4)], candidate_valid=[True] * 4,
                    field_membership=[True, True, False, False, False, False, True, True],
                    instance_mask=[True] * 3),
        records=[record("first", [(0, 1)], [[(2, 3)]]), record("second", [(1, 2)], [[(3, 4)]])],
        logits=dict(objects=[-0.5, 2.0, 0.7], assignments=[((i * 7) % 17 - 8) / 3 for i in range(30)]),
    )
    cases = []
    for mode in ("latent", "anchorless"):
        for scenario in ("normal", "ties", "masked", "large_masked_cost", "sentinel"):
            case = copy.deepcopy(base)
            case["id"] = f"{mode}_{scenario}"
            case["layout"]["mode"] = mode
            if scenario == "ties":
                case["logits"] = dict(objects=[0.0] * 3, assignments=[0.0] * 30)
            elif scenario in ("masked", "large_masked_cost"):
                case["layout"]["instance_mask"] = [False, True, True]
                if scenario == "large_masked_cost":
                    case["logits"]["objects"] = [0.0, -30000.0, -40000.0]
            elif scenario == "sentinel":
                case["logits"]["assignments"] = [20000.0, -30000.0, -30000.0, 0.0, 0.0] * 6
            cases.append(case)
    for scenario in ("ordinary", "alternatives", "ambiguous"):
        case = copy.deepcopy(base)
        case["id"] = "natural_" + scenario
        case["layout"].update(mode="natural", anchor_query=3, anchor_candidates=[0, 1, None], instance_mask=[True, True, False])
        for r in case["records"]:
            r["anchor_query"] = 3
        if scenario == "alternatives":
            case["records"] = [case["records"][0]]
            case["records"][0]["fields"][0] = field(0, 3, [(1, 2), (0, 1)])
        elif scenario == "ambiguous":
            case["records"][1]["fields"][0] = field(0, 3, [(0, 1)])
            case["native_error"] = "ambiguous_anchor"
        cases.append(case)
    for scenario in ("missing_value", "capacity", "empty", "all_masked"):
        case = copy.deepcopy(base)
        case["id"] = scenario
        if scenario == "missing_value":
            case["records"][0]["fields"][1] = field(1, 7, [(2, 3)], [(4, 5)])
            case["native_error"] = "missing_candidate"
        elif scenario == "capacity":
            case["layout"]["instance_mask"] = [False, True, False]
            case["native_error"] = "capacity"
        else:
            case["records"] = []
            if scenario == "all_masked":
                case["layout"]["instance_mask"] = [False] * 3
        cases.append(case)
    mixed = copy.deepcopy(base)
    mixed["id"] = "preserve_original_record_index"
    other = copy.deepcopy(mixed["records"][0])
    other["structure"] = 4
    mixed["records"].insert(1, other)
    cases.append(mixed)
    return cases


def record_loss_cases():
    source = {item["id"]: item for item in fixtures()}

    def group(name):
        item = copy.deepcopy(source[name])
        item.pop("id")
        item["enabled"] = True
        return item

    result = []

    def add(name, groups, object_weight=1.0, field_weight=1.0):
        result.append(dict(id=name, groups=groups, object_weight=object_weight, field_weight=field_weight))

    add("mixed_modes", [group("latent_normal"), group("anchorless_normal"), group("natural_ordinary")])
    one = group("anchorless_normal")
    one["records"] = one["records"][:1]
    add("global_denominators_and_weights", [group("latent_normal"), one, group("empty")], 0.7, 1.3)
    absent = group("latent_normal")
    absent["layout"]["fields"][0]["cardinality"] = "optional_one"
    absent["records"][0]["fields"][0]["values"] = []
    absent["records"][0]["fields"][1]["values"] = []
    absent["records"][1]["fields"][0] = field(0, 3, [(0, 1), (1, 2)])
    absent["records"][1]["fields"][1] = field(1, 7, [(2, 3)], [(3, 4)])
    add("alternatives_and_absent", [absent])
    add("masked_instances_and_columns", [group("latent_masked"), group("anchorless_masked")])
    add("all_masked", [group("all_masked")])
    natural_empty = group("natural_ordinary")
    natural_empty["records"] = []
    add("natural_empty_has_no_object_loss", [natural_empty])
    add("finite_mask_dominates_gold_mass", [group("latent_sentinel")])
    one_field = group("latent_normal")
    one_field["layout"]["fields"] = one_field["layout"]["fields"][:1]
    one_field["layout"]["field_membership"] = one_field["layout"]["field_membership"][:4]
    for record in one_field["records"]:
        record["fields"] = record["fields"][:1]
    raw = one_field["logits"]["assignments"]
    one_field["logits"]["assignments"] = [value for i in range(3) for value in raw[i * 10:i * 10 + 5]]
    add("padded_fields_do_not_change_normalization", [group("latent_normal"), one_field])
    disabled = group("empty")
    disabled["enabled"] = False
    add("disabled_empty_group", [group("latent_normal"), disabled])
    zero = group("empty")
    zero["layout"]["instance_mask"] = []
    zero["logits"] = dict(objects=[], assignments=[])
    add("zero_instances_zero_gold", [zero])
    return result


def capture_record_loss(source: Path):
    provenance, torch = oracle.prepare_runtime(source)
    from gliner2.models.boundary.records import _dense_gold_indicator, compute_dense_batch_loss
    from gliner2.processing.records import FieldCardinality, RecordFieldSpec
    from gliner2.processing.targets import RecordFieldTarget, RecordTarget
    from gliner2.training.matching import build_dense_record_matching_cost, linear_sum_assignment
    import torch.nn.functional as functional

    def flat(value):
        return value.detach().reshape(-1).tolist()

    output_cases = []
    for case in record_loss_cases():
        groups = case["groups"]
        fields = max(len(g["layout"]["fields"]) for g in groups)
        instances = len(groups[0]["layout"]["instance_mask"])
        candidates = len(groups[0]["layout"]["candidate_spans"])
        max_gold = max(1, max(len(g["records"]) for g in groups))
        raw_objects, raw_assignments = [], []
        objects, assignments, indicators, field_masks, memberships = [], [], [], [], []
        scalar_fields, instance_masks, modes, anchors, enabled, record_masks = [], [], [], [], [], []
        for group in groups:
            layout = group["layout"]
            nf = len(layout["fields"])
            if len(layout["instance_mask"]) != instances or len(layout["candidate_spans"]) != candidates:
                raise oracle.ContractError("fixture must share dense instance/candidate widths")
            specs = [RecordFieldSpec(query_id=f["query"], name=f"field{i}", role_index=i,
                                     cardinality=FieldCardinality(f["cardinality"])) for i, f in enumerate(layout["fields"])]
            records = [RecordTarget(instance_id=r["id"], task_index=r["group"], anchor_query_id=r["anchor_query"],
                                   fields=tuple(RecordFieldTarget(query_id=f["query"], values=tuple(tuple((s["start"], s["end"]) for s in v["alternatives"]) for v in f["values"])) for f in r["fields"])) for r in group["records"]]
            membership = torch.tensor(layout["field_membership"], dtype=torch.bool).reshape(nf, candidates)
            spans = torch.tensor([[s["start"], s["end"]] for s in layout["candidate_spans"]], dtype=torch.long)
            gold = _dense_gold_indicator(SimpleNamespace(field_specs=specs, pool_spans=spans, field_membership=membership), records)
            raw_obj = torch.tensor(group["logits"]["objects"], dtype=torch.float32, requires_grad=True)
            raw_assign = torch.tensor(group["logits"]["assignments"], dtype=torch.float32).reshape(instances, nf, candidates + 1).requires_grad_()
            valid_assignment = torch.cat((torch.ones(nf, 1, dtype=torch.bool), membership), -1)
            masked = raw_assign.masked_fill(~valid_assignment[None], -10000)
            raw_objects.append(raw_obj)
            raw_assignments.append(raw_assign)
            objects.append(raw_obj)
            assignments.append(functional.pad(masked, (0, 0, 0, fields - nf), value=-10000))
            indicators.append(functional.pad(gold, (0, 0, 0, fields - nf, 0, max_gold - len(records))))
            memberships.append(functional.pad(membership, (0, 0, 0, fields - nf)))
            field_masks.append([True] * nf + [False] * (fields - nf))
            scalar_fields.append([f.cardinality.is_scalar for f in specs] + [False] * (fields - nf))
            instance_masks.append(layout["instance_mask"])
            modes.append(("natural", "latent", "anchorless").index(layout["mode"]))
            anchors.append(next((i for i, f in enumerate(specs) if f.query_id == layout.get("anchor_query")), 0))
            enabled.append(group["enabled"])
            record_masks.append([True] * len(records) + [False] * (max_gold - len(records)))
        dense = SimpleNamespace(
            object_logits=torch.stack(objects)[None], assign_logits=torch.stack(assignments)[None],
            instance_mask=torch.tensor(instance_masks, dtype=torch.bool)[None],
            field_membership=torch.stack(memberships)[None],
            pool_spans=spans[None], field_mask=torch.tensor(field_masks, dtype=torch.bool)[None],
            scalar_fields=torch.tensor(scalar_fields, dtype=torch.bool)[None],
            modes=torch.tensor(modes, dtype=torch.long)[None],
            anchor_fields=torch.tensor(anchors, dtype=torch.long)[None], group_mask=torch.tensor(enabled, dtype=torch.bool)[None],
        )
        indicator = torch.stack(indicators)[None]
        mask = torch.tensor(record_masks, dtype=torch.bool)[None]
        losses = compute_dense_batch_loss(dense, indicator, mask)
        objective = case["object_weight"] * losses["object_loss"] + case["field_weight"] * losses["field_loss"]
        derivatives = torch.autograd.grad(objective, [*raw_objects, *raw_assignments], allow_unused=True)
        cost = build_dense_record_matching_cost(dense.object_logits.detach(), dense.assign_logits.detach(), indicator,
                                               dense.scalar_fields, dense.instance_mask)
        object_count, matched_count, gradients = 0, 0, []
        for index, group in enumerate(groups):
            layout = group["layout"]
            ng = len(group["records"])
            cost_group = cost[0, index, :, :ng]
            pairs = []
            if ng:
                if layout["mode"] == "natural":
                    natural_gold = indicator[0, index, :ng, anchors[index]]
                    columns = torch.nonzero(natural_gold.any(-1)).flatten()
                    rows = natural_gold[columns].long().argmax(-1)
                else:
                    rows, columns = linear_sum_assignment(cost_group.cpu())
                for row, column in zip(rows, columns):
                    i, g = int(row), int(column)
                    if dense.instance_mask[0, index, i]:
                        pairs.append(dict(instance=i, record=g, annotation=g))
                if len(pairs) != ng:
                    raise oracle.ContractError("loss fixture encountered upstream discarded-gold behavior")
            if group["enabled"]:
                matched_count += len(pairs)
                if layout["mode"] != "natural":
                    object_count += sum(layout["instance_mask"])
            group["expected"] = dict(graph_costs=flat(cost_group), pairs=pairs)
            dobj = derivatives[index]
            dassign = derivatives[index + len(groups)]
            gradients.append(dict(objects=flat(dobj if dobj is not None else torch.zeros_like(raw_objects[index])),
                                  assignments=flat(dassign if dassign is not None else torch.zeros_like(raw_assignments[index]))))
        for value in [objective, *losses.values(), *(g for g in derivatives if g is not None)]:
            if not bool(torch.isfinite(value).all()):
                raise oracle.ContractError(f"nonfinite fixture output {case['id']}")
        case["expected"] = dict(object_loss=float(losses["object_loss"].detach()), field_loss=float(losses["field_loss"].detach()),
                                value=float(objective.detach()), object_count=object_count, matched_record_count=matched_count,
                                gradients=gradients)
        output_cases.append(case)
    files = ["gliner2/models/boundary/records.py", "gliner2/models/boundary/constants.py", "gliner2/training/matching.py"]
    return dict(format_version=1, source_commit=oracle.UPSTREAM_COMMIT, scope="synthetic_dense_batch_record_loss_and_logit_vjp",
                qualification=False, provenance=provenance, generator_sha256=oracle.sha256_file(Path(__file__)),
                oracle_sha256=oracle.sha256_file(Path(oracle.__file__)),
                source_files={name: oracle.sha256_file(source / name) for name in files},
                fixture_dependency_sha256=oracle.sha256_file(Path(__file__)), cases=output_cases,
                notes=["No checkpoint, tokenizer, encoder or optimizer is loaded.",
                       "Actual compute_dense_batch_loss supplies both scalar values and raw-logit gradients.",
                       "Each native group omits padded fields; source field masks establish the equivalent normalization.",
                       "Excluded candidate columns are masked in the live source graph before the loss.",
                       "Detached assignments use exact source float32 costs; final losses are recomputed from live logits.",
                       "Cases avoid upstream inactive-hypothesis gold loss, which native matching rejects separately."])


CAPTURES = {
    "losses": (capture_losses, "training_losses.json"),
    "selection": (capture_selection, "training_selection.json"),
    "matching": (capture_matching, "training_matching.json"),
    "record-loss": (capture_record_loss, "training_record_loss.json"),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=CAPTURES)
    parser.add_argument("--upstream", "--source", dest="source", type=Path,
                        default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    capture, filename = CAPTURES[args.profile]
    output = args.output or oracle.FIXTURES / filename
    value = capture(args.source)
    output.parent.mkdir(parents=True, exist_ok=True)
    if args.profile == "record-loss":
        oracle.write_json(output, value)
    else:
        output.write_text(json.dumps(value, allow_nan=False, separators=(",", ":")) + "\n")
    print(f"wrote {output}: SHA256 {oracle.sha256_file(output)}")


if __name__ == "__main__":
    main()
