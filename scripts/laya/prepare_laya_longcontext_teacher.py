#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "mlx-lm>=0.28,<0.29",
#     "torch>=2.6,<3",
#     "transformers>=4.51,<5",
#     "safetensors>=0.5",
#     "numpy>=2",
# ]
# ///
"""Roadmap step 2a: label states with a long-context open instruct teacher.

The released Laya checkpoint sees at most upstream's 512-token sequence
(question, options and state sharing that budget); `prepare_laya_finetune.py`
drops any typed-decisions case whose state exceeds 316 tokens so every
question fits, and `prepare_laya_packed_distillation.py` leaves those states
at their gold target because its teacher (the same unpacked Laya checkpoint)
cannot see them either. This script extends that pipeline: it scores exactly
the records Laya cannot see with a long-context instruct model (Qwen3-14B,
>=32k native context, run locally via MLX), fits a per-question-type
temperature on a calibration split, and blends the calibrated distribution
with gold the same way the original script does. See
zig/pkg/inference/models/laya/LAYA.md, "Long-context teacher (step 2a)".

    uv run --script prepare_laya_longcontext_teacher.py records.jsonl \\
        --teacher-model .tmp/laya/qwen3-14b-4bit \\
        --laya-model .tmp/laya/laya-released --common .tmp/laya/common.py \\
        --calibration td/calibration.jsonl \\
        --output distilled-long.jsonl --metrics-output long-metrics.json

Pass `--score-all` to score every record regardless of whether Laya could see
it (used to compare the two teachers on states both can see) and
`--compare-laya` to also score records with the actual unpacked Laya
checkpoint (upstream's own code, which truncates long states exactly as
serving does) so the two teachers' agreement with gold can be read side by
side. `--limit`/`--seed` take a bounded, shuffled sample; label log-likelihood
scoring is O(records) forward passes (one per record, prompt shared across
its labels via a branched KV cache) rather than O(decisions x labels).
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import inspect
import json
import math
import os
import random
import tempfile
import time
from pathlib import Path

QTYPES = {"choice": 0, "score": 1, "noul": 2}
KIND_NAME = {"choice": "a single choice", "score": "an ordinal level", "noul": "a yes/no question"}


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_records(path: Path) -> list[dict]:
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not records:
        raise ValueError(f"Empty dataset: {path}")
    return records


def sample(records: list[dict], limit: int | None, seed: int) -> list[dict]:
    if limit is None or limit >= len(records):
        return records
    rng = random.Random(seed)
    indices = list(range(len(records)))
    rng.shuffle(indices)
    chosen = sorted(indices[:limit])
    return [records[i] for i in chosen]


# --- Eligibility (states Laya can/cannot see) -------------------------------------------------


def laya_decision_config(laya_model: Path) -> dict:
    raw = json.loads((laya_model / "config.json").read_text())
    decision = raw["laya"]
    if decision.get("packing", {}).get("mode", "none") != "none":
        raise ValueError("--laya-model must be an unpacked Laya checkpoint")
    return decision


def upstream_question(record: dict) -> dict:
    """Map a native record to common.build_sequence's question dictionary (mirrors
    prepare_laya_packed_distillation.upstream_question; duplicated to avoid a torch
    import when only eligibility, not Laya scoring, is needed)."""
    kind, labels = record["kind"], record["labels"]
    descriptions = record.get("descriptions") or [""] * len(labels)
    if kind == "choice":
        crit = {label: (desc or None) for label, desc in zip(labels, descriptions)}
    elif kind == "score":
        crit = [desc or label for label, desc in zip(labels, descriptions)]
    elif kind == "noul":
        if labels != ["false", "true"]:
            raise ValueError(f"Boolean labels must be false, true: {record['id']}")
        crit = {"false": descriptions[0], "true": descriptions[1]}
    else:
        raise ValueError(f"Unknown question kind: {record['id']}")
    return {"t": kind, "ins": record["instruction"], "crit": crit}


def state_fits(laya_tok, record: dict, decision: dict, build_sequence) -> bool:
    """True when the unpacked Laya checkpoint keeps every state token for this
    question, i.e. Laya can see the whole state. Mirrors
    prepare_laya_packed_distillation.state_fits exactly, so the two scripts
    partition a dataset without overlap or gaps."""
    ids, _ = build_sequence(
        laya_tok, record["text"], upstream_question(record), decision["max_len"], decision["head_max_len"]
    )
    state = laya_tok(record["text"].replace(laya_tok.mask_token, " "), add_special_tokens=False)["input_ids"]
    head_and_options = len(ids) - len(state) - 1
    return head_and_options >= 0 and ids[head_and_options : head_and_options + len(state)] == state


# --- Long-context teacher (MLX) -----------------------------------------------------------------


def build_prompt(record: dict) -> str:
    labels = record["labels"]
    descriptions = record.get("descriptions") or [""] * len(labels)
    lines = [
        "You are given a state (context) and a question about it.",
        "Read the state carefully, then answer the question by choosing exactly one of the given labels.",
        "",
        "STATE:",
        record["text"],
        "",
        f"QUESTION ({KIND_NAME[record['kind']]}): {record['instruction']}",
        "LABELS:",
    ]
    for label, desc in zip(labels, descriptions):
        lines.append(f"- {label}: {desc}" if desc else f"- {label}")
    lines.append("")
    lines.append("Answer with exactly one label from the list above, verbatim, and nothing else.")
    return "\n".join(lines)


def score_record(model, tok, mx, cache_mod, record: dict) -> tuple[list[float], int]:
    """Average per-token log-likelihood of each label, via one prompt forward pass
    and one short branched continuation per label (KV cache reused from the
    prompt; branching does not mutate the shared cache, verified against a
    from-scratch full-sequence forward on a released Laya fixture prompt)."""
    prompt_ids = tok.apply_chat_template(
        [{"role": "user", "content": build_prompt(record)}], add_generation_prompt=True, enable_thinking=False
    )
    cache = cache_mod.make_prompt_cache(model)
    logits = model(mx.array([prompt_ids]), cache=cache)
    mx.eval(logits)
    last_logprobs = logits[0, -1].astype(mx.float32)
    last_logprobs = last_logprobs - mx.logsumexp(last_logprobs)
    saved_state = [c.state for c in cache]

    scores = []
    total_tokens = len(prompt_ids)
    for label in record["labels"]:
        cont_ids = tok.encode(str(label), add_special_tokens=False)
        if not cont_ids:
            scores.append(float("-inf"))
            continue
        total_tokens += len(cont_ids)
        logprob = float(last_logprobs[cont_ids[0]])
        if len(cont_ids) > 1:
            fresh = []
            for k, v in saved_state:
                c = cache_mod.KVCache()
                c.state = (k, v)
                fresh.append(c)
            cont_logits = model(mx.array([cont_ids[:-1]]), cache=fresh)
            mx.eval(cont_logits)
            step_logprobs = cont_logits[0].astype(mx.float32)
            step_logprobs = step_logprobs - mx.logsumexp(step_logprobs, axis=-1, keepdims=True)
            for i, next_id in enumerate(cont_ids[1:]):
                logprob += float(step_logprobs[i, next_id])
        scores.append(logprob / len(cont_ids))  # length-normalized average log-likelihood
    return scores, total_tokens


# --- Calibration and blending (shared shape with prepare_laya_packed_distillation) --------------


def softmax_with_temperature(scores: list[float], temp: float) -> list[float]:
    scaled = [s / temp for s in scores]
    m = max(scaled)
    exps = [math.exp(s - m) for s in scaled]
    total = sum(exps)
    return [e / total for e in exps]


def cross_entropy(probs: list[float], target: list[float]) -> float:
    return -sum(t * math.log(max(p, 1e-12)) for t, p in zip(target, probs))


def fit_temperature(scored: list[tuple[list[float], list[float]]]) -> float:
    """Grid-search the scalar temperature minimizing mean cross-entropy against
    gold on one question type: coarse log-spaced pass, then a refinement pass
    around the best point."""
    if not scored:
        return 1.0

    def mean_ce(temp: float) -> float:
        return sum(cross_entropy(softmax_with_temperature(s, temp), t) for s, t in scored) / len(scored)

    def search(lo: float, hi: float, steps: int) -> float:
        best_t, best_ce = 1.0, math.inf
        for i in range(steps):
            t = lo * (hi / lo) ** (i / (steps - 1))
            ce = mean_ce(t)
            if ce < best_ce:
                best_t, best_ce = t, ce
        return best_t

    coarse = search(0.02, 20.0, 60)
    return search(max(0.005, coarse / 3), coarse * 3, 40)


def blend(gold: list[float], teacher: list[float], gold_weight: float) -> list[float]:
    if len(gold) != len(teacher) or not 0 <= gold_weight <= 1:
        raise ValueError("Cannot blend mismatched targets")
    mixed = [gold_weight * g + (1 - gold_weight) * t for g, t in zip(gold, teacher)]
    total = sum(mixed)
    if not math.isfinite(total) or total <= 0:
        raise ValueError("Blended target is not a distribution")
    return [p / total for p in mixed]


# --- Metrics (mirrors finetune/laya/evaluate.zig: 15 equal-width confidence bins) ----------------


def metrics_for(items: list[tuple[str, list[float], list[float]]]) -> dict:
    """items: (kind, probabilities, target) triples. Returns overall plus
    per-kind accuracy, soft CE and ECE, matching Metrics/metrics() in
    finetune/laya/evaluate.zig exactly."""

    def compute(subset):
        n = len(subset)
        if n == 0:
            return None
        bins = [[0.0, 0.0, 0.0] for _ in range(15)]  # count, confidence, correct
        accuracy = soft_ce = 0.0
        for _, probs, target in subset:
            winner = max(range(len(probs)), key=lambda k: probs[k])
            gold = max(range(len(target)), key=lambda k: target[k])
            soft_ce += cross_entropy(probs, target)
            correct = 1.0 if winner == gold else 0.0
            accuracy += correct
            confidence = probs[winner]
            b = min(14, int(confidence * 15))
            bins[b][0] += 1
            bins[b][1] += confidence
            bins[b][2] += correct
        ece = sum(c / n * abs(conf / c - corr / c) for c, conf, corr in bins if c > 0)
        return {"decisions": n, "accuracy": accuracy / n, "soft_ce": soft_ce / n, "ece": ece}

    out = {"overall": compute(items)}
    for kind in QTYPES:
        subset = [it for it in items if it[0] == kind]
        if subset:
            out[kind] = compute(subset)
    return out


# --- Laya baseline (upstream code, for --compare-laya) ------------------------------------------


def load_laya_temperature(decision: dict):
    def temperature(kind: str, count: int) -> float:
        bucket = "2" if count <= 2 else "3-5" if count <= 5 else "6-10" if count <= 10 else "11+"
        buckets = decision.get("temperature_by_options", {})
        value = buckets.get(f"{kind}:{bucket}")
        if value is None:
            value = decision.get("temperature", [1, 1, 1])[QTYPES[kind]]
        return max(0.001, float(value))

    return temperature


def score_laya_baseline(laya_model: Path, common, decision: dict, records: list[dict]) -> list[list[float]]:
    """Score records with the actual unpacked Laya checkpoint via upstream's own
    code, exactly as laya_upstream_baseline.py does. `common.build_sequence`
    truncates a long state to fit the fixed budget, so this reproduces the
    accuracy Laya would serve in production if asked about these states today."""
    import torch
    from safetensors.torch import load_file
    from transformers import ModernBertConfig, ModernBertModel, PreTrainedTokenizerFast

    raw = json.loads((laya_model / "config.json").read_text())
    raw.pop("laya")
    cfg = ModernBertConfig.from_dict(raw)
    cfg._attn_implementation = "eager"
    model = common.DecisionModel(
        ModernBertModel(cfg), head_layers=decision["head_layers"], n_act=len(decision.get("act_costs", {})) + 1
    ).eval()
    model.load_state_dict(load_file(laya_model / "model.safetensors"), strict=True)
    tok = PreTrainedTokenizerFast.from_pretrained(laya_model)
    temperature = load_laya_temperature(decision)

    results = []
    for r in records:
        ids, markers = common.build_sequence(
            tok, r["text"], upstream_question(r), decision["max_len"], decision["head_max_len"]
        )
        batch = common.collate_items([[{"ids": ids, "markers": markers, "qtype": QTYPES[r["kind"]]}]], tok.pad_token_id)
        with torch.no_grad():
            logits, _ = model(*(batch[k] for k in ("input_ids", "attention_mask", "marker_pos", "marker_mask", "qtype")))
        n = len(r["labels"])
        probs = torch.softmax(logits[0, :n].double() / temperature(r["kind"], n), -1).tolist()
        results.append(probs)
    return results


# --- Main -----------------------------------------------------------------------------------


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("records", type=Path)
    parser.add_argument("--teacher-model", type=Path, required=True, help="Local MLX long-context teacher directory")
    parser.add_argument("--laya-model", type=Path, required=True, help="Prepared unpacked Laya directory")
    parser.add_argument("--common", type=Path, required=True, help="Upstream laya/common.py")
    parser.add_argument("--calibration", type=Path, help="Native records with gold targets to fit per-type temperature")
    parser.add_argument("--temperatures", type=Path, help="Reuse temperatures from a prior run's provenance JSON")
    parser.add_argument("--gold-weight", type=float, default=0.5)
    parser.add_argument("--score-all", action="store_true", help="Score every record, ignoring Laya eligibility")
    parser.add_argument("--compare-laya", action="store_true", help="Also score with the unpacked Laya checkpoint")
    parser.add_argument("--limit", type=int, help="Bounded, shuffled sample size")
    parser.add_argument("--seed", type=int, default=20260925)
    parser.add_argument("--output", type=Path, help="Distilled records output (optional if only measuring)")
    parser.add_argument("--metrics-output", type=Path, help="Write an accuracy/soft-CE/ECE comparison report here")
    args = parser.parse_args()
    if args.output and args.output.exists():
        parser.error(f"Output already exists: {args.output}")
    if not 0 <= args.gold_weight <= 1:
        parser.error("--gold-weight must be in [0, 1]")
    if not args.output and not args.metrics_output:
        parser.error("Nothing to do: pass --output, --metrics-output, or both")

    from mlx_lm.models import cache as cache_mod
    from mlx_lm.utils import load as mlx_load
    from transformers import PreTrainedTokenizerFast
    import mlx.core as mx

    common = load_module(args.common, "laya_common")
    decision = laya_decision_config(args.laya_model)
    laya_tok = PreTrainedTokenizerFast.from_pretrained(args.laya_model)

    records = load_records(args.records)
    eligible_mask = [state_fits(laya_tok, r, decision, common.build_sequence) for r in records]
    if args.score_all:
        targets = list(range(len(records)))
    else:
        targets = [i for i, ok in enumerate(eligible_mask) if not ok]
    chosen = sample(targets, args.limit, args.seed)

    t0 = time.time()
    model, teacher_tok = mlx_load(args.teacher_model)
    load_seconds = time.time() - t0

    raw_scores: dict[int, list[float]] = {}
    total_tokens = 0
    t0 = time.time()
    for i in chosen:
        scores, tokens = score_record(model, teacher_tok, mx, cache_mod, records[i])
        raw_scores[i] = scores
        total_tokens += tokens
    score_seconds = time.time() - t0

    temperatures: dict[str, float] = {}
    if args.temperatures:
        temperatures = json.loads(args.temperatures.read_text())["temperature_by_kind"]
    elif args.calibration:
        cal_records = load_records(args.calibration)
        cal_targets = [
            r for r in cal_records if not state_fits(laya_tok, r, decision, common.build_sequence)
        ]
        for kind in QTYPES:
            subset = [r for r in cal_targets if r["kind"] == kind]
            if not subset:
                temperatures[kind] = 1.0
                continue
            scored = []
            for r in subset:
                scores, _ = score_record(model, teacher_tok, mx, cache_mod, r)
                scored.append((scores, r["target"]))
            temperatures[kind] = fit_temperature(scored)
    else:
        temperatures = {kind: 1.0 for kind in QTYPES}

    calibrated: dict[int, list[float]] = {
        i: softmax_with_temperature(scores, temperatures.get(records[i]["kind"], 1.0))
        for i, scores in raw_scores.items()
    }

    laya_probs: dict[int, list[float]] = {}
    if args.compare_laya:
        laya_records = [records[i] for i in chosen]
        for i, probs in zip(chosen, score_laya_baseline(args.laya_model, common, decision, laya_records)):
            laya_probs[i] = probs

    if args.output:
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=args.output.parent, delete=False) as out:
                temporary = Path(out.name)
                for i, record in enumerate(records):
                    if i in calibrated:
                        record = {**record, "target": blend(record["target"], calibrated[i], args.gold_weight)}
                    out.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")
                out.flush()
                os.fsync(out.fileno())
            os.link(temporary, args.output)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    if args.metrics_output:
        report = {
            "format": "antfly-laya-longcontext-teacher/v1",
            "teacher_model": str(args.teacher_model),
            "records_scored": len(chosen),
            "records_total": len(records),
            "score_all": args.score_all,
            "temperature_by_kind": temperatures,
            "seconds_per_decision": score_seconds / max(1, len(chosen)),
            "prompt_tokens": total_tokens,
            "load_seconds": load_seconds,
        }
        raw_items = [(records[i]["kind"], softmax_with_temperature(raw_scores[i], 1.0), records[i]["target"]) for i in chosen]
        calibrated_items = [(records[i]["kind"], calibrated[i], records[i]["target"]) for i in chosen]
        report["teacher_uncalibrated"] = metrics_for(raw_items)
        report["teacher_calibrated"] = metrics_for(calibrated_items)
        if laya_probs:
            laya_items = [(records[i]["kind"], laya_probs[i], records[i]["target"]) for i in chosen]
            report["laya_teacher"] = metrics_for(laya_items)
        args.metrics_output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))

    if args.output:
        provenance = {
            "format": "antfly-laya-longcontext-distillation/v1",
            "records": len(records),
            "distilled": len(calibrated),
            "score_all": args.score_all,
            "gold_weight": args.gold_weight,
            "temperature_by_kind": temperatures,
            "source_sha256": digest(args.records),
            "teacher_model": str(args.teacher_model),
            "teacher_config_sha256": digest(args.teacher_model / "config.json"),
            "laya_model": str(args.laya_model),
            "common_sha256": digest(args.common),
            "prompt_template_sha256": hashlib.sha256(inspect.getsource(build_prompt).encode()).hexdigest(),
            "output_sha256": digest(args.output),
        }
        Path(f"{args.output}.json").write_text(json.dumps(provenance, indent=2) + "\n")
        print(json.dumps(provenance, indent=2))


if __name__ == "__main__":
    main()
