#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))

import compare_qwen3vl_precision_backends as comparison


class PrecisionBackendComparisonTests(unittest.TestCase):
    def native(self) -> dict:
        return {
            "request": {
                "prompt": "describe",
                "image_sha256": "a" * 64,
                "max_tokens": 1,
                "max_merged_tokens": 576,
            },
            "precision_contract": {
                "high_precision_native": "bf16",
                "high_precision_transformers_mps": "bfloat16",
                "q4_native": "q4_k_m decoder with q8_0 projector",
            },
            "transformers_mps": {"report": {"configuration": {"dtype": "bfloat16"}}},
            "native": {
                "bf16": {
                    "median": {"core_seconds": 3.0},
                    "timing_boundary": "native",
                    "determinism": {"generated_token_ids": [1986]},
                    "runs": [{"parity": {"visual_token_count": 532}}],
                },
                "q4": {
                    "median": {"core_seconds": 2.0},
                    "timing_boundary": "native",
                    "determinism": {"generated_token_ids": [1986]},
                    "runs": [{"parity": {"visual_token_count": 532}}],
                },
            },
        }

    def mlx(self, profile: str) -> dict:
        return {
            "profile": profile,
            "precision_contract": {"profile": profile},
            "request": {
                "prompt": "describe",
                "image_sha256": "a" * 64,
                "max_tokens": 1,
                "max_merged_tokens": 576,
                "visual_token_count": 532,
            },
            "benchmark": {"median": {"end_to_end_seconds": 4.0}, "timed": [{"generated_token_ids": [1986]}]},
        }

    def test_high_precision_comparison_requires_matching_profile(self) -> None:
        row = comparison.profile_comparison(self.native(), self.mlx("bf16"), "bf16")
        self.assertTrue(row["token_sequence_exact"])
        self.assertEqual(4.0 / 3.0, row["mlx_warmed_request_over_native_core_ratio"])
        with self.assertRaisesRegex(comparison.ComparisonError, "profile mismatch"):
            comparison.profile_comparison(self.native(), self.mlx("q4"), "bf16")

    def test_comparison_rejects_different_request(self) -> None:
        mlx = self.mlx("q4")
        mlx["request"]["image_sha256"] = "b" * 64
        with self.assertRaisesRegex(comparison.ComparisonError, "requests do not match"):
            comparison.profile_comparison(self.native(), mlx, "q4")

    def test_existing_report_is_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "existing.json"
            output.write_text('{"preserve": true}\n')
            result = comparison.main(
                [
                    "--native-report", str(output),
                    "--mlx-bf16-report", str(output),
                    "--mlx-q4-report", str(output),
                    "--output", str(output),
                ]
            )
            self.assertEqual(2, result)
            self.assertEqual('{"preserve": true}\n', output.read_text())


if __name__ == "__main__":
    unittest.main()
