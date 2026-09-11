#!/usr/bin/env python3
"""Reduce standalone Flash-prefill v2 evidence for the locked Gemma4 fixture.

This tool is intentionally not a production/release gate. It accepts one
identity-layout/random-pattern harness run covering the exact 2051-token
prefill (512+512+512+512+3), validates strict candidate/Flash-v1 bitwise
qualification, and projects the measured attention time across Gemma4's
28 HD256 local-attention and 7 HD512 global-attention layers.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


INPUT_SCHEMA = "antfly.cuda_flash_prefill_prototype.v4"
OUTPUT_SCHEMA = "antfly.cuda_flash_prefill_v2_projection.v1"
Q512_PREFIXES = (0, 512, 1024, 1536)
TAIL_PREFIX = 2048
LAYERS_BY_HEAD_DIM = {256: 28, 512: 7}
DEFAULT_MATERIAL_SPEEDUP = 1.20


class EvidenceError(ValueError):
    pass


def _positive_finite(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvidenceError(f"{field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result <= 0.0:
        raise EvidenceError(f"{field} must be positive and finite")
    return result


def _speedup(baseline_us: float, candidate_us: float) -> float:
    return baseline_us / candidate_us


def summarize(payload: dict[str, Any], material_speedup: float) -> dict[str, Any]:
    if payload.get("schema") != INPUT_SCHEMA:
        raise EvidenceError(f"expected schema {INPUT_SCHEMA!r}")
    if payload.get("runtime_integrated") is not False:
        raise EvidenceError("input must be standalone evidence")
    if payload.get("pass") is not True:
        raise EvidenceError("input qualification did not pass")
    gates = payload.get("gates")
    if (
        not isinstance(gates, dict)
        or gates.get("require_bitwise_candidate") is not True
    ):
        raise EvidenceError("strict candidate/canonical bitwise gate was not enabled")
    if not math.isfinite(material_speedup) or material_speedup <= 1.0:
        raise EvidenceError(
            "material speedup threshold must be finite and greater than 1"
        )

    raw_results = payload.get("results")
    if not isinstance(raw_results, list):
        raise EvidenceError("results must be an array")
    indexed: dict[tuple[int, int, int], dict[str, Any]] = {}
    for result in raw_results:
        if not isinstance(result, dict):
            raise EvidenceError("each result must be an object")
        head_dim = result.get("head_dim")
        q_len = result.get("q_len")
        prefix = result.get("prefix")
        key = (head_dim, q_len, prefix)
        if head_dim not in LAYERS_BY_HEAD_DIM:
            raise EvidenceError(f"unexpected head dimension: {head_dim!r}")
        if result.get("layout") != "identity-null" or result.get("pattern") != "random":
            raise EvidenceError(
                "projection input must contain only identity-null/random cases"
            )
        if key in indexed:
            raise EvidenceError(f"duplicate result for {key!r}")
        if result.get("pass") is not True:
            raise EvidenceError(f"case {key!r} did not pass")
        differential = result.get("candidate_vs_canonical")
        if (
            not isinstance(differential, dict)
            or differential.get("bitwise_mismatches") != 0
        ):
            raise EvidenceError(f"case {key!r} is not bitwise-identical to Flash-v1")
        indexed[key] = result

    required = {
        (head_dim, 512, prefix)
        for head_dim in LAYERS_BY_HEAD_DIM
        for prefix in Q512_PREFIXES
    }
    required.update((head_dim, 3, TAIL_PREFIX) for head_dim in LAYERS_BY_HEAD_DIM)
    if set(indexed) != required:
        missing = sorted(required - set(indexed))
        extra = sorted(set(indexed) - required)
        raise EvidenceError(
            f"locked fixture mismatch; missing={missing!r} extra={extra!r}"
        )

    per_head_dim: list[dict[str, Any]] = []
    projected_baseline_us = 0.0
    projected_candidate_us = 0.0
    for head_dim, layers in LAYERS_BY_HEAD_DIM.items():
        q512_baseline_us = 0.0
        q512_candidate_us = 0.0
        prefixes: list[dict[str, Any]] = []
        for prefix in Q512_PREFIXES:
            timing = indexed[(head_dim, 512, prefix)].get("timing")
            if not isinstance(timing, dict):
                raise EvidenceError(f"missing timing for {(head_dim, 512, prefix)!r}")
            baseline_us = _positive_finite(
                timing.get("baseline_mean_us"), "timing.baseline_mean_us"
            )
            candidate_us = _positive_finite(
                timing.get("candidate_mean_us"), "timing.candidate_mean_us"
            )
            q512_baseline_us += baseline_us
            q512_candidate_us += candidate_us
            prefixes.append(
                {
                    "prefix": prefix,
                    "baseline_us": baseline_us,
                    "candidate_us": candidate_us,
                    "speedup": _speedup(baseline_us, candidate_us),
                }
            )

        tail_timing = indexed[(head_dim, 3, TAIL_PREFIX)].get("timing")
        if not isinstance(tail_timing, dict):
            raise EvidenceError(f"missing tail timing for head_dim={head_dim}")
        tail_baseline_us = _positive_finite(
            tail_timing.get("baseline_mean_us"), "tail.baseline_mean_us"
        )
        tail_candidate_us = _positive_finite(
            tail_timing.get("candidate_mean_us"), "tail.candidate_mean_us"
        )
        all_baseline_us = q512_baseline_us + tail_baseline_us
        all_candidate_us = q512_candidate_us + tail_candidate_us
        projected_baseline_us += layers * all_baseline_us
        projected_candidate_us += layers * all_candidate_us
        per_head_dim.append(
            {
                "head_dim": head_dim,
                "policy": "swa512" if head_dim == 256 else "global",
                "layers": layers,
                "q512_prefixes": prefixes,
                "q512_sum_per_layer": {
                    "baseline_us": q512_baseline_us,
                    "candidate_us": q512_candidate_us,
                    "speedup": _speedup(q512_baseline_us, q512_candidate_us),
                },
                "q3_tail_per_layer": {
                    "baseline_us": tail_baseline_us,
                    "candidate_us": tail_candidate_us,
                    "speedup": _speedup(tail_baseline_us, tail_candidate_us),
                },
                "all_chunks_per_layer": {
                    "baseline_us": all_baseline_us,
                    "candidate_us": all_candidate_us,
                    "speedup": _speedup(all_baseline_us, all_candidate_us),
                },
            }
        )

    projected_speedup = _speedup(projected_baseline_us, projected_candidate_us)
    artifacts = payload.get("artifacts")
    return {
        "schema": OUTPUT_SCHEMA,
        "source_schema": INPUT_SCHEMA,
        "source_artifacts": artifacts if isinstance(artifacts, dict) else {},
        "locked_fixture": {
            "prompt_tokens": 2051,
            "chunks": [512, 512, 512, 512, 3],
            "query_heads": 8,
            "kv_heads": 1,
            "kv_format": "f16-paged",
            "page_size_tokens": 16,
            "local_layers_hd256_swa512": 28,
            "global_layers_hd512": 7,
        },
        "candidate": {
            "query_tile": payload.get("candidate_query_tile"),
            "key_tile": payload.get("candidate_key_tile"),
            "head_group": payload.get("candidate_head_group"),
        },
        "per_head_dimension": per_head_dim,
        "projected_35_layer_attention": {
            "baseline_us": projected_baseline_us,
            "candidate_us": projected_candidate_us,
            "saved_us": projected_baseline_us - projected_candidate_us,
            "speedup": projected_speedup,
            "material_speedup_threshold": material_speedup,
            "material_pass": projected_speedup >= material_speedup,
        },
    }


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument(
        "--material-speedup",
        type=float,
        default=DEFAULT_MATERIAL_SPEEDUP,
        help="minimum projected 35-layer attention speedup (default: 1.20)",
    )
    parser.add_argument(
        "--require-material",
        action="store_true",
        help="return nonzero if the material threshold is not met",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        with args.evidence.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            raise EvidenceError("top-level evidence must be an object")
        summary = summarize(payload, args.material_speedup)
    except (EvidenceError, OSError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    rendered = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    sys.stdout.write(rendered)
    if args.json_out is not None:
        args.json_out.write_text(rendered, encoding="utf-8")
    if (
        args.require_material
        and not summary["projected_35_layer_attention"]["material_pass"]
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
