#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

from __future__ import annotations

import array
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import benchmark_metal_gemma4_lm_head_repack_quality as quality  # noqa: E402


SUITE_PATH = SCRIPT_DIR / "fixtures/gemma4_lm_head_repack_quality_v2.json"


def metric_step(
    *,
    top1_match: bool = True,
    candidate_nll: float = 1.0,
    candidate_logits_distinct: bool = True,
    refined_top1_match: bool = True,
) -> dict:
    return {
        "baseline_expected_nll": 1.0,
        "candidate_expected_nll": candidate_nll,
        "top1_match": top1_match,
        "top10_overlap": 10,
        "kl_base_to_candidate": 0.0,
        "js_divergence": 0.0,
        "max_abs": 0.0,
        "_sum_abs": 0.0,
        "_sum_sq": 0.0,
        "element_count": 16,
        "candidate_logits_distinct": candidate_logits_distinct,
        "refined_top1_match": refined_top1_match,
    }


class SuiteContractTests(unittest.TestCase):
    def test_reviewed_fixture_digest_and_coverage_are_pinned(self) -> None:
        self.assertEqual(
            quality.REVIEWED_SUITE_SHA256,
            hashlib.sha256(SUITE_PATH.read_bytes()).hexdigest(),
        )
        suite = quality.load_suite(SUITE_PATH, quality.REVIEWED_SUITE_SHA256)["raw"]
        self.assertEqual(quality.SUITE_SCHEMA, suite["schema"])
        self.assertGreaterEqual(len(suite["cases"]), 8)
        self.assertGreaterEqual(len({case["category"] for case in suite["cases"]}), 8)
        chat = [case for case in suite["cases"] if case.get("prompt_mode") == "chat"]
        self.assertTrue(chat)
        self.assertTrue(all(case["continuation_token_ids"] for case in chat))

    def test_tampered_or_unreviewed_suite_fails_closed(self) -> None:
        raw = json.loads(SUITE_PATH.read_text(encoding="utf-8"))
        raw["cases"][1]["id"] = raw["cases"][0]["id"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "suite.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaisesRegex(quality.ContractError, "duplicate case id"):
                quality.load_suite(path, digest)
            with self.assertRaisesRegex(quality.ContractError, "SHA-256 mismatch"):
                quality.load_suite(path, quality.REVIEWED_SUITE_SHA256)

    def test_suite_digest_must_be_lowercase_sha256(self) -> None:
        for digest in ("", "a" * 63, "A" * 64, "not-a-digest"):
            with self.subTest(digest=digest):
                with self.assertRaisesRegex(quality.ContractError, "lowercase digest"):
                    quality.load_suite(SUITE_PATH, digest)


class ThresholdContractTests(unittest.TestCase):
    def test_reviewed_and_stricter_thresholds_are_allowed(self) -> None:
        self.assertEqual(
            quality.DEFAULT_THRESHOLDS,
            quality.validate_thresholds(dict(quality.DEFAULT_THRESHOLDS)),
        )
        stricter = dict(quality.DEFAULT_THRESHOLDS)
        stricter["max_perplexity_ratio"] -= 0.001
        stricter["min_top1_agreement"] += 0.001
        self.assertEqual(stricter, quality.validate_thresholds(stricter))

    def test_looser_nonfinite_and_incomplete_thresholds_are_rejected(self) -> None:
        for name, value in (
            ("max_perplexity_ratio", 1.02),
            ("max_mean_kl_base_to_candidate", 0.02),
            ("min_top1_agreement", 0.98),
            ("min_mean_top10_overlap_fraction", 0.89),
        ):
            thresholds = dict(quality.DEFAULT_THRESHOLDS)
            thresholds[name] = value
            with self.subTest(name=name):
                with self.assertRaisesRegex(quality.ContractError, "cannot be looser"):
                    quality.validate_thresholds(thresholds)
        nonfinite = dict(quality.DEFAULT_THRESHOLDS)
        nonfinite["max_step_kl_base_to_candidate"] = math.nan
        with self.assertRaisesRegex(quality.ContractError, "finite number"):
            quality.validate_thresholds(nonfinite)
        incomplete = dict(quality.DEFAULT_THRESHOLDS)
        incomplete.pop("min_step_top10_overlap_fraction")
        with self.assertRaisesRegex(quality.ContractError, "reviewed contract"):
            quality.validate_thresholds(incomplete)

    def test_aggregate_gate_passes_identity_and_rejects_argmax_drift(self) -> None:
        aggregate, failures = quality.aggregate_metrics(
            [metric_step(), metric_step()],
            quality.DEFAULT_THRESHOLDS,
        )
        self.assertFalse(failures)
        self.assertEqual(1.0, aggregate["perplexity_ratio"])
        aggregate, failures = quality.aggregate_metrics(
            [metric_step(), metric_step(top1_match=False)],
            quality.DEFAULT_THRESHOLDS,
        )
        self.assertEqual(0.5, aggregate["top1_agreement"])
        self.assertIn("top-1 agreement gate failed", failures)

    def test_aggregate_gate_rejects_vacuous_dump_and_refine_divergence(self) -> None:
        aggregate, failures = quality.aggregate_metrics(
            [metric_step(candidate_logits_distinct=False), metric_step()],
            quality.DEFAULT_THRESHOLDS,
        )
        self.assertEqual(1, aggregate["distinct_candidate_logit_steps"])
        self.assertIn("candidate transformed-logit attestation gate failed", failures)

        aggregate, failures = quality.aggregate_metrics(
            [metric_step(refined_top1_match=False), metric_step()],
            quality.DEFAULT_THRESHOLDS,
        )
        self.assertEqual(0.5, aggregate["refined_argmax_agreement"])
        self.assertIn("top-8 Q4 nomination plus Q6 refinement gate failed", failures)

    def test_campaign_dimensions_require_repeatability_and_reviewed_vocab(self) -> None:
        quality.validate_campaign_dimensions(2, quality.REVIEWED_VOCAB_SIZE, 120.0)
        for repetitions in (1, 5):
            with self.subTest(repetitions=repetitions):
                with self.assertRaisesRegex(quality.ContractError, "determinism is observable"):
                    quality.validate_campaign_dimensions(
                        repetitions,
                        quality.REVIEWED_VOCAB_SIZE,
                        120.0,
                    )
        with self.assertRaisesRegex(quality.ContractError, "reviewed Gemma 4 value"):
            quality.validate_campaign_dimensions(2, 256000, 120.0)
        for timeout in (0.0, math.inf, math.nan, 3601.0):
            with self.subTest(timeout=timeout):
                with self.assertRaisesRegex(quality.ContractError, "timeout-seconds"):
                    quality.validate_campaign_dimensions(
                        2,
                        quality.REVIEWED_VOCAB_SIZE,
                        timeout,
                    )


class EvidenceContractTests(unittest.TestCase):
    def test_output_directory_must_be_new_and_outside_the_repository(self) -> None:
        repo = SCRIPT_DIR.parents[3]
        with self.assertRaisesRegex(quality.ContractError, "outside the source repository"):
            quality.validate_output_directory(repo / "new-evidence", repo)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(quality.ContractError, "already exists"):
                quality.validate_output_directory(root, repo)
            expected = (root / "new-evidence").resolve()
            self.assertEqual(expected, quality.validate_output_directory(expected, repo))

    def test_clean_environment_drops_unreviewed_policy_variables(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "HOME": "/safe-home",
                "TERMITE_METAL_ENABLE_LM_HEAD_Q4_REPACK": "q4_k",
                "UNRELATED_SECRET": "secret",
            },
            clear=True,
        ):
            env = quality.clean_environment({"TERMITE_METAL_DUMP_GENERATE_LOGITS_F32": "/tmp/out"})
        self.assertEqual("/safe-home", env["HOME"])
        self.assertEqual("/tmp/out", env["TERMITE_METAL_DUMP_GENERATE_LOGITS_F32"])
        self.assertNotIn("TERMITE_METAL_ENABLE_LM_HEAD_Q4_REPACK", env)
        self.assertNotIn("UNRELATED_SECRET", env)

    def test_file_provenance_detects_rewrites(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "artifact.bin"
            path.write_bytes(b"first")
            before = quality.stable_file_provenance(path)
            path.write_bytes(b"second")
            after = quality.stable_file_provenance(path)
            self.assertFalse(quality.same_file_provenance(before, after))
            self.assertTrue(quality.same_file_provenance(after, dict(after)))

    def test_logit_reader_rejects_wrong_size_and_nonfinite_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wrong_size = root / "wrong.f32"
            wrong_size.write_bytes(b"short")
            with self.assertRaisesRegex(quality.ContractError, "invalid logit dump size"):
                quality.read_logits(wrong_size, 2)

            nonfinite = root / "nonfinite.f32"
            values = array.array("f", [0.0, math.inf])
            with nonfinite.open("wb") as destination:
                values.tofile(destination)
            with self.assertRaisesRegex(quality.ContractError, "non-finite"):
                quality.read_logits(nonfinite, 2)

    def test_logit_comparison_attests_raw_q4_and_exact_refine_contract(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline_path = root / "baseline.f32"
            candidate_path = root / "candidate.f32"
            baseline = array.array("f", (float(value) for value in range(12)))
            candidate = array.array("f", baseline)
            candidate[0] += 0.25
            with baseline_path.open("wb") as destination:
                baseline.tofile(destination)
            with candidate_path.open("wb") as destination:
                candidate.tofile(destination)

            metrics = quality.compare_logits(
                baseline_path,
                candidate_path,
                expected_token_id=10,
                expected_count=12,
                suppress_token_ids=[11],
            )
            self.assertTrue(metrics["candidate_logits_distinct"])
            self.assertEqual(10, metrics["production_baseline_top1"])
            self.assertTrue(metrics["refined_top1_match"])

            candidate = array.array("f", reversed(baseline))
            with candidate_path.open("wb") as destination:
                candidate.tofile(destination)
            metrics = quality.compare_logits(
                baseline_path,
                candidate_path,
                expected_token_id=11,
                expected_count=12,
                suppress_token_ids=[],
            )
            self.assertFalse(metrics["refined_top1_match"])

            with self.assertRaisesRegex(quality.ContractError, "duplicate or out-of-range"):
                quality.compare_logits(
                    baseline_path,
                    candidate_path,
                    expected_token_id=11,
                    expected_count=12,
                    suppress_token_ids=[1, 1],
                )

    def test_dump_paths_are_step_and_phase_unique(self) -> None:
        base = Path("/tmp/evidence/logits")
        self.assertEqual(
            [
                Path("/tmp/evidence/logits.0.prefill.f32"),
                Path("/tmp/evidence/logits.1.decode.f32"),
                Path("/tmp/evidence/logits.2.decode.f32"),
            ],
            quality.expected_dump_paths(base, 3),
        )


if __name__ == "__main__":
    unittest.main()
