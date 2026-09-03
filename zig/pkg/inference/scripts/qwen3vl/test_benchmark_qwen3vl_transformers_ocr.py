#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))

import benchmark_qwen3vl_transformers_ocr as benchmark


def args(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "weights_dir": Path("/weights"),
        "processor_dir": Path("/processor"),
        "image": Path("/image.jpg"),
        "prompt": "Read all visible text.",
        "device": "mps",
        "dtype": "bfloat16",
        "attn_implementation": "sdpa",
        "load_strategy": "device_map",
        "warmup_runs": 1,
        "timed_runs": 3,
        "max_tokens": 64,
        "max_merged_tokens": 576,
        "timeout_seconds": 180.0,
        "sample_interval_seconds": 0.25,
        "max_rss_mib": 8192.0,
        "min_free_percent": 15,
        "max_swap_growth_mib": 0.0,
        "mps_high_watermark_ratio": 0.8,
        "mps_low_watermark_ratio": 0.7,
        "mps_prefer_metal": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class TransformersOcrBenchmarkTests(unittest.TestCase):
    def test_defaults_bind_complete_ocr_request(self) -> None:
        parsed = benchmark.parse_args(
            [
                "--weights-dir",
                "/weights",
                "--processor-dir",
                "/processor",
                "--image",
                "/image.jpg",
                "--device",
                "mps",
                "--output",
                "/report.json",
            ]
        )
        self.assertEqual("bfloat16", parsed.dtype)
        self.assertEqual("sdpa", parsed.attn_implementation)
        self.assertEqual("device_map", parsed.load_strategy)
        self.assertEqual(64, parsed.max_tokens)
        self.assertEqual(576, parsed.max_merged_tokens)
        self.assertEqual(1, parsed.warmup_runs)
        self.assertEqual(3, parsed.timed_runs)

    def test_worker_command_replays_greedy_request_contract(self) -> None:
        command = benchmark.worker_command(args(), Path("/work/worker.json"))
        self.assertIn("--worker", command)
        self.assertEqual("/work/worker.json", command[command.index("--worker-output") + 1])
        self.assertEqual("64", command[command.index("--max-tokens") + 1])
        self.assertEqual("576", command[command.index("--max-merged-tokens") + 1])
        self.assertEqual("mps", command[command.index("--device") + 1])

    def test_mps_environment_disables_fallback_and_fast_math(self) -> None:
        environment = benchmark.benchmark_environment(args())
        self.assertEqual("0", environment["PYTORCH_ENABLE_MPS_FALLBACK"])
        self.assertEqual("0", environment["PYTORCH_MPS_FAST_MATH"])
        self.assertEqual("0.8", environment["PYTORCH_MPS_HIGH_WATERMARK_RATIO"])
        self.assertEqual("0.7", environment["PYTORCH_MPS_LOW_WATERMARK_RATIO"])

    def test_cpu_environment_omits_mps_specific_controls(self) -> None:
        environment = benchmark.benchmark_environment(args(device="cpu"))
        self.assertNotIn("PYTORCH_ENABLE_MPS_FALLBACK", environment)
        self.assertEqual("1", environment["HF_HUB_OFFLINE"])
        self.assertEqual("1", environment["TRANSFORMERS_OFFLINE"])

    def test_worker_validation_requires_exact_request_and_determinism(self) -> None:
        worker = {
            "schema": benchmark.WORKER_SCHEMA,
            "model": {"sha256": benchmark.MODEL_SHA256},
            "runtime": {"device": "mps"},
            "request": {"max_tokens": 64, "max_merged_tokens": 576},
            "benchmark": {
                "timed_token_sequences_deterministic": True,
                "generated_token_ids": [1, 2, 3],
                "generated_token_count": 3,
            },
        }
        checks = benchmark.validate_worker_report(worker, args())
        self.assertTrue(all(checks.values()))
        worker["benchmark"]["generated_token_count"] = 4
        mismatch = benchmark.validate_worker_report(worker, args())
        self.assertFalse(mismatch["generated_token_count"])

    def test_finish_reason_distinguishes_cap_and_eos(self) -> None:
        self.assertEqual("length", benchmark._finish_reason([1, 2], 2, 9))
        self.assertEqual("eos", benchmark._finish_reason([1, 9], 4, 9))
        self.assertEqual("generation_stop", benchmark._finish_reason([1, 2], 4, 9))

    def test_merged_visual_tokens_use_grid_geometry_not_placeholder_count(self) -> None:
        self.assertEqual(567, benchmark.merged_visual_token_count([[1, 54, 42]], 2))
        with self.assertRaisesRegex(benchmark.QualificationError, "not divisible"):
            benchmark.merged_visual_token_count([[1, 5, 5]], 2)


if __name__ == "__main__":
    unittest.main()
