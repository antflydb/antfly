#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))

import transformers_weights_oracle as oracle


class TransformersWeightsOracleContractTests(unittest.TestCase):
    def test_defaults_preserve_canonical_cpu_oracle(self) -> None:
        args = oracle.parse_args(
            [
                "--weights-dir",
                "/weights",
                "--processor-dir",
                "/processor",
                "--image",
                "/image.jpg",
                "--logits-output",
                "/logits.f32le",
                "--output",
                "/oracle.json",
            ]
        )
        self.assertEqual("cpu", args.device)
        self.assertEqual("bfloat16", args.dtype)
        self.assertEqual("eager", args.attn_implementation)
        self.assertEqual("device_map", args.load_strategy)
        self.assertEqual("view", args.logit_transfer)
        self.assertEqual(0, args.warmup_runs)
        self.assertEqual(1, args.timed_runs)
        self.assertEqual(0, args.logits_to_keep)
        self.assertFalse(args.profile_stages)

    def test_mps_benchmark_options_are_explicit(self) -> None:
        args = oracle.parse_args(
            [
                "--weights-dir",
                "/weights",
                "--processor-dir",
                "/processor",
                "--image",
                "/image.jpg",
                "--device",
                "mps",
                "--dtype",
                "float16",
                "--attn-implementation",
                "sdpa",
                "--load-strategy",
                "cpu_then_move",
                "--logit-transfer",
                "clone",
                "--warmup-runs",
                "2",
                "--timed-runs",
                "5",
                "--logits-to-keep",
                "1",
                "--profile-stages",
                "--logits-output",
                "/logits.f32le",
                "--output",
                "/oracle.json",
            ]
        )
        self.assertEqual("mps", args.device)
        self.assertEqual("float16", args.dtype)
        self.assertEqual("sdpa", args.attn_implementation)
        self.assertEqual("cpu_then_move", args.load_strategy)
        self.assertEqual("clone", args.logit_transfer)
        self.assertEqual(2, args.warmup_runs)
        self.assertEqual(5, args.timed_runs)
        self.assertEqual(1, args.logits_to_keep)
        self.assertTrue(args.profile_stages)

    def test_percentile_uses_nearest_rank(self) -> None:
        self.assertEqual(1.0, oracle.percentile([3.0, 1.0, 2.0], 0.01))
        self.assertEqual(2.0, oracle.percentile([3.0, 1.0, 2.0], 0.50))
        self.assertEqual(3.0, oracle.percentile([3.0, 1.0, 2.0], 0.95))

    def test_stage_summary_preserves_samples_and_reports_tail(self) -> None:
        summary = oracle.summarize_stage_samples(
            [
                {"vision": 1.0, "decoder": 2.0, "lm_head": 0.1, "unattributed": 0.2},
                {"vision": 1.2, "decoder": 1.8, "lm_head": 0.2, "unattributed": 0.1},
            ]
        )
        self.assertEqual(1.1, summary["stages"]["vision"]["median"])
        self.assertEqual(1.2, summary["stages"]["vision"]["p95"])
        self.assertEqual(2, len(summary["samples"]))


if __name__ == "__main__":
    unittest.main()
