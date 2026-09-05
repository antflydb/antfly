#!/usr/bin/env python3

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))

import benchmark_qwen3vl_transformers_mps as benchmark


def args(**overrides: object) -> SimpleNamespace:
    script_dir = Path(__file__).resolve().parent
    values: dict[str, object] = {
        "output": Path("/tmp/nonexistent-qwen3vl-mps-report.json"),
        "oracle_script": script_dir / "transformers_weights_oracle.py",
        "requirements_file": script_dir / "requirements-qwen3vl-oracle.txt",
        "reference_json": None,
        "reference_logits": None,
        "warmup_runs": 1,
        "timed_runs": 3,
        "max_merged_tokens": 576,
        "logits_to_keep": 1,
        "profile_stages": False,
        "mps_high_watermark_ratio": 0.8,
        "mps_low_watermark_ratio": 0.7,
        "timeout_seconds": 180.0,
        "sample_interval_seconds": 0.25,
        "max_rss_mib": 8192.0,
        "min_free_percent": 15,
        "max_swap_growth_mib": 0.0,
        "mps_prefer_metal": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class MpsBenchmarkContractTests(unittest.TestCase):
    def test_defaults_use_validated_resident_mps_path(self) -> None:
        parsed = benchmark.parse_args(
            [
                "--weights-dir",
                "/weights",
                "--processor-dir",
                "/processor",
                "--image",
                "/image.jpg",
                "--output",
                "/report.json",
            ]
        )
        self.assertEqual("float16", parsed.dtype)
        self.assertEqual("sdpa", parsed.attn_implementation)
        self.assertEqual("device_map", parsed.load_strategy)
        self.assertEqual("clone", parsed.logit_transfer)
        self.assertEqual(1, parsed.warmup_runs)
        self.assertEqual(3, parsed.timed_runs)
        self.assertEqual(1, parsed.logits_to_keep)
        self.assertFalse(parsed.profile_stages)

    def test_profile_options_reach_oracle_command(self) -> None:
        command = benchmark.oracle_command(
            args(
                weights_dir=Path("/weights"),
                processor_dir=Path("/processor"),
                image=Path("/image.jpg"),
                prompt="describe",
                dtype="float16",
                attn_implementation="sdpa",
                load_strategy="device_map",
                logit_transfer="clone",
                logits_to_keep=1,
                profile_stages=True,
            ),
            Path("/work"),
        )
        self.assertIn("--profile-stages", command)
        self.assertEqual("1", command[command.index("--logits-to-keep") + 1])

    def test_environment_disables_fallback_and_fast_math(self) -> None:
        environment = benchmark.build_environment(args())
        self.assertEqual("0", environment["PYTORCH_ENABLE_MPS_FALLBACK"])
        self.assertEqual("0", environment["PYTORCH_MPS_FAST_MATH"])
        self.assertEqual("0.8", environment["PYTORCH_MPS_HIGH_WATERMARK_RATIO"])
        self.assertEqual("0.7", environment["PYTORCH_MPS_LOW_WATERMARK_RATIO"])
        self.assertEqual("0", environment["PYTORCH_MPS_PREFER_METAL"])

    def test_invalid_watermark_order_fails_closed(self) -> None:
        with mock.patch.object(benchmark.platform, "system", return_value="Darwin"):
            with self.assertRaisesRegex(benchmark.QualificationError, "low watermark"):
                benchmark.validate_args(
                    args(mps_high_watermark_ratio=0.7, mps_low_watermark_ratio=0.8)
                )

    def test_reference_contract_binds_request_and_logits(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            logits = Path(raw) / "reference.f32le"
            logits.write_bytes(b"reference logits")
            digest = hashlib.sha256(logits.read_bytes()).hexdigest()
            request = {
                "prompt": "describe",
                "image_sha256": "a" * 64,
                "input_ids": [1, 2, 3],
                "image_grid_thw": [[1, 4, 6]],
            }
            reference = {
                "schema": benchmark.ORACLE_SCHEMA,
                "model": {"sha256": benchmark.MODEL_SHA256},
                "request": request,
                "last_logits": {"f32le_sha256": digest},
            }
            candidate = {
                "model": {"sha256": benchmark.MODEL_SHA256},
                "request": dict(request),
            }
            contract = benchmark.reference_contract(reference, candidate, logits)
            self.assertTrue(contract["pass"])
            candidate["request"]["input_ids"] = [9]
            mismatch = benchmark.reference_contract(reference, candidate, logits)
            self.assertFalse(mismatch["pass"])
            self.assertFalse(mismatch["checks"]["input_ids"])

    def test_parity_limits_are_stricter_than_quantized_gate(self) -> None:
        passing = {
            "size_match": True,
            "finite": True,
            "cosine_similarity": 0.999,
            "pearson_correlation": 0.999,
            "mean_abs": 0.1,
            "rmse": 0.2,
            "max_abs": 1.0,
            "top_k_overlap": 10,
        }
        self.assertTrue(benchmark.parity_quality_pass(passing))
        self.assertFalse(benchmark.parity_quality_pass({**passing, "max_abs": 2.1}))


if __name__ == "__main__":
    unittest.main()
