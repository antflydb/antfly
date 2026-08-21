#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Contract tests for the Gemma4 A4B Metal release benchmark."""

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from benchmark_metal_gemma4_a4b import (  # noqa: E402
    BenchmarkContractError,
    METADATA_SCHEMA,
    build_summary,
    parse_sample,
)


PROMPT_IDS = "11 12"
OUTPUT_IDS = "21 22 23"


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def payload(total: float, prefill: float, decode: float, *, fallback: int = 0) -> dict:
    return {
        "backend": "metal",
        "tokens": 3,
        "finish_reason": "length",
        "token_ids": [21, 22, 23],
        "timing_ms": {
            "generate": total,
            "prefill_inner": prefill,
            "decode_inner": decode,
        },
        "metal": {
            "device": "Apple M4",
            "residency": {
                "runtime_mapped_fallbacks": 0,
                "runtime_mapped_failures": 0,
                "a4b_linear_attempts": 90,
                "a4b_linear_successes": 90 - fallback,
                "a4b_linear_fallbacks": fallback,
                "a4b_pair_attempts": 30,
                "a4b_pair_successes": 30 - fallback,
                "a4b_pair_fallbacks": fallback,
                "a4b_scatter_attempts": 30,
                "a4b_scatter_successes": 30 - fallback,
                "a4b_scatter_fallbacks": fallback,
            },
        },
    }


def log(mode: str, budget_mb: int, rss_mb: int = 1900) -> str:
    slots = 8 if budget_mb == 2048 else 128
    return (
        f"prompt_token_ids: {PROMPT_IDS}\n"
        f"token_ids: {OUTPUT_IDS}\n"
        f"gemma4_a4b_runtime: mode={mode} budget_mb={budget_mb} "
        f"kv_mb=512 safety_mb=64 expert_slots={slots} model=test\n"
        f"{rss_mb * 1024 * 1024}  maximum resident set size\n"
    )


class ParseSampleTests(unittest.TestCase):
    def test_accepts_exact_bounded_route(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            json_path = root / "sample.json"
            log_path = root / "sample.log"
            json_path.write_text(json.dumps(payload(100, 40, 60)))
            log_path.write_text(log("streamed", 2048))
            sample = parse_sample(
                json_path,
                log_path,
                expected_mode="streamed",
                expected_budget_mb=2048,
                expected_device="Apple M4",
                expected_prompt_tokens=2,
                expected_prompt_sha256=digest(PROMPT_IDS),
                expected_output_tokens=3,
                expected_output_sha256=digest(OUTPUT_IDS),
                max_rss_mb=2304,
            )
            self.assertEqual(sample["expert_slots"], 8)
            self.assertEqual(sample["max_rss_mb"], 1900)

    def test_rejects_any_a4b_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            json_path = root / "sample.json"
            log_path = root / "sample.log"
            json_path.write_text(json.dumps(payload(100, 40, 60, fallback=1)))
            log_path.write_text(log("streamed", 2048))
            with self.assertRaises(BenchmarkContractError):
                parse_sample(
                    json_path,
                    log_path,
                    expected_mode="streamed",
                    expected_budget_mb=2048,
                    expected_device="Apple M4",
                    expected_prompt_tokens=2,
                    expected_prompt_sha256=digest(PROMPT_IDS),
                    expected_output_tokens=3,
                    expected_output_sha256=digest(OUTPUT_IDS),
                    max_rss_mb=2304,
                )


class SummaryTests(unittest.TestCase):
    def test_paired_gate_passes_repeatable_faster_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            invocations = []
            timings = {
                ("baseline", 0): (100.0, 40.0, 60.0),
                ("candidate", 0): (90.0, 36.0, 54.0),
                ("baseline", 1): (101.0, 40.0, 61.0),
                ("candidate", 1): (90.9, 36.0, 54.9),
            }
            serial = 0
            for pair in range(2):
                for variant in ("baseline", "candidate"):
                    label = f"{serial:03d}-{pair}-{variant}"
                    serial += 1
                    invocations.append(
                        {
                            "label": label,
                            "variant": variant,
                            "pair": pair,
                            "warmup": False,
                        }
                    )
                    total, prefill, decode = timings[(variant, pair)]
                    (root / f"{label}.json").write_text(
                        json.dumps(payload(total, prefill, decode))
                    )
                    (root / f"{label}.log").write_text(log("streamed", 2048))
            metadata = {
                "schema": METADATA_SCHEMA,
                "experiment_id": "unit",
                "expected_metal_device": "Apple M4",
                "expected_prompt_tokens": 2,
                "expected_prompt_token_ids_sha256": digest(PROMPT_IDS),
                "output_tokens": 3,
                "expected_output_token_ids_sha256": digest(OUTPUT_IDS),
                "baseline_mode": "streamed",
                "baseline_budget_mb": 2048,
                "baseline_max_rss_mb": 2304,
                "candidate_mode": "streamed",
                "candidate_budget_mb": 2048,
                "candidate_max_rss_mb": 2304,
                "max_total_latency_ratio": 0.95,
                "max_decode_latency_ratio": 0.95,
                "min_decode_throughput_ratio": 1.05,
                "min_target_wins": 2,
                "max_cv": 0.05,
                "invocations": invocations,
            }
            (root / "metadata.json").write_text(json.dumps(metadata))
            summary = build_summary(root)
            self.assertTrue(summary["passed"], summary["failures"])
            self.assertAlmostEqual(
                summary["paired_ratios"]["candidate_total_latency_ratio"]["median"],
                0.9,
            )


if __name__ == "__main__":
    unittest.main()
