#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))

import benchmark_qwen3vl_mlx_vlm as benchmark


class MlxVlmBenchmarkContractTests(unittest.TestCase):
    def test_requirements_pin_chat_template_runtime(self) -> None:
        requirements = (
            Path(__file__).resolve().parent / "requirements-qwen3vl-mlx-vlm.txt"
        ).read_text(encoding="utf-8").splitlines()
        self.assertIn("jinja2==3.1.6", requirements)
        self.assertIn("mlx-vlm==0.6.17", requirements)

    def test_fingerprint_binds_qwen3vl_tree(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            (root / "config.json").write_text(json.dumps({"model_type": "qwen3_vl"}))
            (root / "weights.safetensors").write_bytes(b"weights")
            evidence = benchmark.fingerprint_model_dir(root)
            self.assertEqual(2, len(evidence["artifacts"]))
            self.assertEqual(64, len(evidence["tree_sha256"]))

    def test_fingerprint_rejects_non_qwen3vl_model(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            (root / "config.json").write_text(json.dumps({"model_type": "qwen2_vl"}))
            with self.assertRaisesRegex(benchmark.MlxVlmBenchmarkError, "model_type"):
                benchmark.fingerprint_model_dir(root)

    def test_token_trace_ignores_final_summary_duplicate(self) -> None:
        intermediate = SimpleNamespace(token=1986, finish_reason=None, generation_tokens=1)
        final = SimpleNamespace(token=1986, finish_reason="length", generation_tokens=1)
        tokens, actual_final = benchmark._token_trace([intermediate, final], 1)
        self.assertEqual([1986], tokens)
        self.assertIs(final, actual_final)

    def test_token_trace_rejects_short_generation(self) -> None:
        final = SimpleNamespace(token=None, finish_reason="length", generation_tokens=0)
        with self.assertRaisesRegex(benchmark.MlxVlmBenchmarkError, "generated 0 tokens"):
            benchmark._token_trace([final], 1)

    def test_profiles_keep_high_precision_and_q4_distinct(self) -> None:
        self.assertEqual(("bf16", "q4"), benchmark.PROFILES)

    def test_default_pixel_cap_matches_native_visual_token_limit(self) -> None:
        self.assertEqual(576, benchmark.MAX_MERGED_VISUAL_TOKENS)
        self.assertEqual(589824, benchmark.COMPARISON_MAX_PIXELS)

    def test_precision_contract_rejects_wrong_quantization(self) -> None:
        self.assertEqual(
            0, benchmark.precision_contract("bf16", {"model_type": "qwen3_vl"})["quantization_bits"]
        )
        self.assertEqual(
            4, benchmark.precision_contract("q4", {"quantization": {"bits": 4}})["quantization_bits"]
        )
        with self.assertRaisesRegex(benchmark.MlxVlmBenchmarkError, "quantization.bits"):
            benchmark.precision_contract("q4", {"quantization": {"bits": 8}})

    def test_existing_report_is_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            output = root / "existing.json"
            output.write_text('{"preserve": true}\n')
            result = benchmark.main(
                [
                    "--model-dir", str(root), "--image", str(output),
                    "--profile", "bf16", "--output", str(output),
                ]
            )
            self.assertEqual(2, result)
            self.assertEqual('{"preserve": true}\n', output.read_text())

    def test_missing_optional_runtime_dependency_writes_failure_report(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "failure.json"
            args = SimpleNamespace(output=output, model_dir=Path(raw))
            with (
                mock.patch.object(benchmark, "parse_args", return_value=args),
                mock.patch.object(benchmark, "validate_args"),
                mock.patch.object(benchmark, "fingerprint_model_dir", return_value={}),
                mock.patch.object(benchmark, "run", side_effect=ImportError("missing jinja2")),
            ):
                self.assertEqual(2, benchmark.main([]))
            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertFalse(report["pass"])
            self.assertIn("missing jinja2", report["failure"])


if __name__ == "__main__":
    unittest.main()
