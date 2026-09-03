#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))

import benchmark_qwen3vl_native_image_batch as benchmark


class ComparisonTests(unittest.TestCase):
    def test_comparison_uses_image_throughput_not_request_throughput(self) -> None:
        single = {"input_count": 1, "median": {"generate_seconds": 10.0, "vision_seconds": 4.0, "prefill_seconds": 6.0}}
        batch = {"input_count": 2, "median": {"generate_seconds": 15.0, "vision_seconds": 7.0, "prefill_seconds": 8.0}}
        metrics = benchmark.comparison(single, batch)
        self.assertAlmostEqual(0.75, metrics["single_images_per_core_second"] / metrics["batch_images_per_core_second"])
        self.assertAlmostEqual(4.0 / 3.0, metrics["core_throughput_gain"])
        self.assertAlmostEqual(8.0 / 7.0, metrics["vision_throughput_gain"])
        self.assertAlmostEqual(1.5, metrics["prefill_throughput_gain"])

    def test_main_binds_binary_to_provenance(self) -> None:
        with (
            mock.patch.object(benchmark, "validate_args", return_value=[Path("batch.jpg")] ),
            mock.patch.object(benchmark, "validate_managed_bundle", return_value={"bundle": True}),
            mock.patch.object(benchmark, "run_profile", side_effect=[
                {"input_count": 1, "median": {"generate_seconds": 1.0, "vision_seconds": 0.4, "prefill_seconds": 0.6}},
                {"input_count": 2, "median": {"generate_seconds": 1.5, "vision_seconds": 0.7, "prefill_seconds": 0.8}},
            ]),
            mock.patch.object(benchmark, "git_provenance", return_value={"ok": True}) as provenance,
            mock.patch.object(benchmark, "write_json_atomic"),
            mock.patch.object(benchmark, "parse_args") as parse_args,
            mock.patch("builtins.print"),
        ):
            args = mock.Mock()
            args.model_dir = Path("model")
            args.work_dir = mock.Mock()
            args.image = Path("single.jpg")
            args.batch_size = 2
            args.max_tokens = 1
            args.prompt = "prompt"
            args.max_rss_mib = 1.0
            args.min_free_percent = 10
            args.max_swap_growth_mib = 0.0
            args.timeout_seconds = 1.0
            args.antfly_bin = Path("antfly-inference")
            parse_args.return_value = args
            self.assertEqual(0, benchmark.main([]))
            provenance.assert_called_once_with(args.antfly_bin)

    def test_performance_environment_records_absent_and_enabled_gates(self) -> None:
        with mock.patch.dict(
            benchmark.os.environ,
            {
                "TERMITE_METAL_ENABLE_QWEN3VL_PREPARED_FFN": "1",
                "TERMITE_METAL_ENABLE_QWEN3VL_FORWARD_FRAME": "1",
                "TERMITE_METAL_DISABLE_QWEN3VL_DECODE_FRAME": "1",
            },
            clear=True,
        ):
            environment = benchmark.performance_environment()
        self.assertEqual("1", environment["TERMITE_METAL_ENABLE_QWEN3VL_PREPARED_FFN"])
        self.assertEqual("1", environment["TERMITE_METAL_ENABLE_QWEN3VL_FORWARD_FRAME"])
        self.assertEqual("1", environment["TERMITE_METAL_DISABLE_QWEN3VL_DECODE_FRAME"])
        self.assertIsNone(environment["TERMITE_METAL_ENABLE_Q4_K_HIGH_ROW_MM"])
        self.assertEqual(set(benchmark.PERFORMANCE_ENVIRONMENT_KEYS), set(environment))

    def test_stage_timing_requires_complete_nonfailing_metal_evidence(self) -> None:
        timing = {
            "metal": {
                "stage_timing_ns": {
                    "scope": "runtime_frame",
                    "enabled": 1,
                    "supported": 1,
                    "complete": 1,
                    "samples": 2,
                    "failures": 0,
                    "prefill": {
                        "frames": 1,
                        "gpu": 10,
                        "attention": 2,
                        "ffn": 3,
                        "ple": 0,
                        "tail": 1,
                        "embedding": 2,
                        "other": 2,
                    },
                    "decode": {
                        "frames": 1,
                        "gpu": 20,
                        "attention": 4,
                        "ffn": 6,
                        "ple": 0,
                        "tail": 2,
                        "embedding": 4,
                        "other": 4,
                    },
                }
            }
        }
        evidence = benchmark.stage_timing_evidence(timing, profile="single", run=1)
        self.assertEqual(2, evidence["samples"])
        self.assertEqual(6, evidence["decode"]["ffn"])

    def test_stage_timing_rejects_incomplete_evidence(self) -> None:
        timing = {
            "metal": {
                "stage_timing_ns": {
                    "enabled": 1,
                    "supported": 1,
                    "complete": 0,
                    "samples": 0,
                    "failures": 1,
                }
            }
        }
        with self.assertRaisesRegex(benchmark.ImageBatchBenchmarkError, "complete stage timing"):
            benchmark.stage_timing_evidence(timing, profile="single", run=1)

    def test_image_geometry_binds_aggregate_visual_tokens_to_image_grids(self) -> None:
        parity = {
            "visual_token_count": 1064,
            "images": [
                {
                    "source_width": 2048,
                    "source_height": 1416,
                    "resized_width": 896,
                    "resized_height": 608,
                    "grid_thw": [1, 38, 56],
                    "patch_rows": 2128,
                    "patch_columns": 768,
                },
                {
                    "source_width": 2048,
                    "source_height": 1416,
                    "resized_width": 896,
                    "resized_height": 608,
                    "grid_thw": [1, 38, 56],
                    "patch_rows": 2128,
                    "patch_columns": 768,
                },
            ],
        }
        visual_tokens, images = benchmark.image_geometry_evidence(
            parity, profile="batch", run=1, expected_count=2
        )
        self.assertEqual(1064, visual_tokens)
        self.assertEqual([1, 38, 56], images[0]["grid_thw"])

    def test_image_geometry_rejects_mismatched_visual_token_count(self) -> None:
        with self.assertRaisesRegex(benchmark.ImageBatchBenchmarkError, "does not match"):
            benchmark.image_geometry_evidence(
                {
                    "visual_token_count": 533,
                    "images": [
                        {
                            "source_width": 2048,
                            "source_height": 1416,
                            "resized_width": 896,
                            "resized_height": 608,
                            "grid_thw": [1, 38, 56],
                            "patch_rows": 2128,
                            "patch_columns": 768,
                        }
                    ],
                },
                profile="single",
                run=1,
                expected_count=1,
            )


if __name__ == "__main__":
    unittest.main()
