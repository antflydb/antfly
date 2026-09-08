#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))

import benchmark_qwen3vl_transformers_cuda as benchmark


def args(**overrides: object) -> SimpleNamespace:
    script_dir = Path(__file__).resolve().parent
    values: dict[str, object] = {
        "output": Path("/tmp/nonexistent-qwen3vl-cuda-report.json"),
        "oracle_script": script_dir / "transformers_weights_oracle.py",
        "requirements_file": script_dir / "requirements-qwen3vl-oracle.txt",
        "reference_json": None,
        "reference_logits": None,
        "warmup_runs": 1,
        "timed_runs": 3,
        "max_merged_tokens": 576,
        "logits_to_keep": 1,
        "profile_stages": False,
        "timeout_seconds": 180.0,
        "sample_interval_seconds": 0.25,
        "max_rss_mib": 8192.0,
        "max_gpu_memory_mib": 12288.0,
        "min_free_percent": 15,
        "max_swap_growth_mib": 0.0,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class CudaBenchmarkContractTests(unittest.TestCase):
    def test_defaults_use_validated_resident_cuda_path(self) -> None:
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
        self.assertEqual("bfloat16", parsed.dtype)
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

    def test_environment_disables_tf32_and_enables_deterministic_cublas(self) -> None:
        environment = benchmark.build_environment(args())
        self.assertEqual(":4096:8", environment["CUBLAS_WORKSPACE_CONFIG"])
        self.assertEqual("0", environment["NVIDIA_TF32_OVERRIDE"])

    def test_non_linux_platform_fails_closed(self) -> None:
        with mock.patch.object(benchmark.platform, "system", return_value="Darwin"):
            with self.assertRaisesRegex(benchmark.QualificationError, "requires Linux"):
                benchmark.validate_args(args())

    def test_existing_report_is_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "report.json"
            output.write_text("original", encoding="utf-8")
            result = benchmark.main(
                [
                    "--weights-dir",
                    "/weights",
                    "--processor-dir",
                    "/processor",
                    "--image",
                    "/image.jpg",
                    "--output",
                    str(output),
                ]
            )
            self.assertEqual(2, result)
            self.assertEqual("original", output.read_text(encoding="utf-8"))

    def test_report_created_during_run_is_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "report.json"

            def concurrent_report(_args: object) -> None:
                output.write_text("concurrent report", encoding="utf-8")
                raise benchmark.QualificationError("benchmark failed")

            with (
                mock.patch.object(
                    benchmark, "parse_args", return_value=args(output=output)
                ),
                mock.patch.object(
                    benchmark, "validate_args", side_effect=concurrent_report
                ),
            ):
                self.assertEqual(2, benchmark.main([]))
            self.assertEqual("concurrent report", output.read_text(encoding="utf-8"))
            self.assertEqual([output], list(Path(raw).iterdir()))

    def test_exclusive_publication_writes_complete_report_and_cleans_temporary(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "report.json"
            report = {"pass": False, "failure": "benchmark failed"}
            benchmark.write_json_atomic(output, report, overwrite=False)
            self.assertEqual(report, json.loads(output.read_text(encoding="utf-8")))
            self.assertEqual([output], list(Path(raw).iterdir()))

    def test_dangling_output_symlink_is_not_followed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            target = Path(raw) / "missing.json"
            output = Path(raw) / "report.json"
            output.symlink_to(target)
            with mock.patch.object(
                benchmark, "parse_args", return_value=args(output=output)
            ):
                self.assertEqual(2, benchmark.main([]))
            self.assertTrue(output.is_symlink())
            self.assertFalse(target.exists())

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
