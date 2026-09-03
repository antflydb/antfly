#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
import json
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))

import benchmark_qwen3vl_native_precision as benchmark


class NativePrecisionBenchmarkContractTests(unittest.TestCase):
    def test_high_precision_logit_gate_accepts_strict_metrics(self) -> None:
        metrics = {
            "size_match": True,
            "finite": True,
            "cosine_similarity": 0.999,
            "pearson_correlation": 0.999,
            "mean_abs": 0.1,
            "rmse": 0.2,
            "max_abs": 1.0,
            "top_k_overlap": 10,
        }
        self.assertTrue(benchmark.high_precision_logit_pass(metrics))
        self.assertFalse(benchmark.high_precision_logit_pass({**metrics, "top_k_overlap": 8}))

    def test_high_precision_profile_is_not_a_production_receipt(self) -> None:
        self.assertEqual("bf16-reference-bundle-v1", benchmark.high_precision.OUTPUT_IDENTITY["variant"])
        self.assertNotEqual("q4-k-m-bundle-v1", benchmark.high_precision.OUTPUT_IDENTITY["variant"])

    def test_precision_limits_are_stricter_than_q4_gate(self) -> None:
        self.assertGreater(
            benchmark.HIGH_PRECISION_LIMITS["min_cosine_similarity"], 0.95
        )
        self.assertLess(benchmark.HIGH_PRECISION_LIMITS["max_rmse"], 1.25)

    def test_existing_report_is_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "existing.json"
            output.write_text('{"preserve": true}\n')
            result = benchmark.main(
                [
                    "--high-precision-model-dir", raw,
                    "--q4-model-dir", raw,
                    "--weights-dir", raw,
                    "--processor-dir", raw,
                    "--image", str(output),
                    "--antfly-bin", str(output),
                    "--output", str(output),
                ]
            )
            self.assertEqual(2, result)
            self.assertEqual('{"preserve": true}\n', output.read_text())

    def test_high_precision_mps_reference_is_bf16(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            work_dir = Path(raw)
            captured: list[str] = []

            def fake_mps_main(argv: list[str]) -> int:
                captured.extend(argv)
                output = Path(argv[argv.index("--output") + 1])
                artifacts = Path(argv[argv.index("--work-dir") + 1])
                artifacts.mkdir()
                (artifacts / "mps_last_logits.f32le").write_bytes(b"logits")
                output.write_text(json.dumps({"pass": True, "oracle": {}}))
                return 0

            args = SimpleNamespace(
                weights_dir=work_dir,
                processor_dir=work_dir,
                image=work_dir / "image.jpg",
                prompt="describe",
                mps_warmup_runs=1,
                mps_timed_runs=1,
                mps_timeout_seconds=1.0,
                mps_max_rss_mib=1024.0,
                min_free_percent=15,
                max_swap_growth_mib=0.0,
            )
            with mock.patch.object(benchmark.mps_benchmark, "main", side_effect=fake_mps_main):
                result = benchmark.run_mps(args, work_dir)
            self.assertEqual("bfloat16", captured[captured.index("--dtype") + 1])
            self.assertTrue(Path(result["logits"]).is_file())


if __name__ == "__main__":
    unittest.main()
