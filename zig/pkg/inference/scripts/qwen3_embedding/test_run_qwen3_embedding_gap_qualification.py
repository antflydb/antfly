#!/usr/bin/env python3
"""Qualification bookkeeping must measure the serving worker and fail closed."""
from pathlib import Path
import copy
from contextlib import redirect_stderr
import io
import tempfile
from types import SimpleNamespace
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_qwen3_embedding_gap_qualification as qualification


class ProcessMemoryTests(unittest.TestCase):
    def test_supervisor_worker_and_descendants_are_counted_once(self):
        sample = "100 1 7\n101 100 500\n102 101 20\n200 1 9999\n"
        with mock.patch.object(qualification.subprocess, "run", return_value=SimpleNamespace(stdout=sample)):
            result = qualification.process_tree_memory(100)
        self.assertEqual(527 * 1024, result["rss_bytes"])
        self.assertEqual([100, 101, 102], [row["pid"] for row in result["processes"]])

    def test_missing_owned_supervisor_fails_instead_of_reporting_zero_memory(self):
        with mock.patch.object(qualification.subprocess, "run", return_value=SimpleNamespace(stdout="200 1 9999\n")):
            with self.assertRaisesRegex(RuntimeError, "supervisor disappeared"):
                qualification.process_tree_memory(100)

    def test_swap_measurement_keeps_fractional_mebibytes_and_fails_on_unknown_output(self):
        self.assertEqual(round(4253.44 * 1024**2), qualification.swap_used_bytes("vm.swapusage: total = 5120.00M used = 4253.44M free = 866.56M"))
        with self.assertRaisesRegex(RuntimeError, "parse host swap"):
            qualification.swap_used_bytes("swap information unavailable")


class BaselineGateTests(unittest.TestCase):
    def report(self, latency=10):
        return {"args": {"fixture_token_count": 256, "batch_sizes": [1], "task_type": "document",
                         "query_prefix": None, "seed": 1, "iters": 20, "warmup": 3, "antfly_server_args": "same budgets"},
                "comparison_contract": {"strict": True, "model_files": {"identical": True,
                         "antfly": {"sha256": "same weights"}, "reference": {"sha256": "same weights"}}},
                "comparisons": [{"pass": True}],
                "results": [{"target": "antfly", "batch": 1, "dimensions": 1024, "input_tokens": 256, "samples_ms": [latency] * 20}]}

    def test_regression_is_measured_against_fixed_baseline(self):
        self.assertTrue(qualification.baseline_regression(self.report(9), self.report(10))["pass"])
        result = qualification.baseline_regression(self.report(12), self.report(10))
        self.assertFalse(result["pass"])
        self.assertAlmostEqual(10 / 12, result["lower_95"])

    def test_different_workload_and_failed_parity_cannot_qualify(self):
        baseline = self.report()
        for key, value in (("batch_sizes", [8]), ("fixture_token_count", 511), ("antfly_server_args", "larger budget")):
            changed = copy.deepcopy(baseline)
            changed["args"][key] = value
            with self.assertRaisesRegex(ValueError, "workload mismatch"):
                qualification.baseline_regression(self.report(), changed)
        baseline["comparisons"][0]["pass"] = False
        with self.assertRaisesRegex(ValueError, "vector parity"):
            qualification.baseline_regression(self.report(), baseline)

    def test_short_and_nonfinite_samples_fail_closed(self):
        for samples in ([10] * 19, [10] * 19 + [float("nan")], [10] * 19 + [0]):
            baseline = self.report()
            baseline["results"][0]["samples_ms"] = samples
            with self.assertRaisesRegex(ValueError, "20 positive finite samples"):
                qualification.baseline_regression(self.report(), baseline)


class ConfigurationValidationTests(unittest.TestCase):
    def test_invalid_paired_configuration_never_creates_results_or_starts_servers(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "results"
            common = ["qualify", "--antfly", "missing", "--llama", "missing", "--model-dir", "missing", "--fixture", "missing", "--output-dir", str(output)]
            for extra, message in (
                (["--stage", "short", "--baseline-antfly", "missing"], "baseline-antfly requires"),
                (["--stage", "regression", "--baseline-antfly", "missing", "--baseline-dir", "missing"], "baseline-antfly requires"),
                (["--antfly-port", "18100", "--reference-port", "18100"], "distinct ports"),
            ):
                with self.subTest(extra=extra), mock.patch.object(sys, "argv", common + extra), redirect_stderr(io.StringIO()) as stderr, mock.patch.object(qualification, "server") as start:
                    with self.assertRaises(SystemExit) as result:
                        qualification.main()
                    self.assertEqual(2, result.exception.code)
                    self.assertIn(message, stderr.getvalue())
                    start.assert_not_called()
                    self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
