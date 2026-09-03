from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import benchmark_gliner2_lora_perf as benchmark


class BenchmarkGateTest(unittest.TestCase):
    def test_args_with_seed_replaces_or_appends_seed(self) -> None:
        self.assertEqual(
            ["--steps", "4", "--seed", "23"],
            benchmark.args_with_seed(["--steps", "4"], 23),
        )
        self.assertEqual(
            ["--seed", "37", "--steps", "4"],
            benchmark.args_with_seed(["--seed", "11", "--steps", "4"], 37),
        )

    def test_production_wrapper_uses_five_isolated_seeds(self) -> None:
        wrapper = (
            Path(__file__).resolve().parent / "run_gliner2_lora_perf_gate.sh"
        ).read_text(encoding="utf-8")
        self.assertIn('perf_seeds="11,23,37,53,71"', wrapper)

    def test_production_readiness_uses_uninstrumented_warm_gate(self) -> None:
        wrapper = (
            Path(__file__).resolve().parent / "run_gliner2_lora_production_readiness.sh"
        ).read_text(encoding="utf-8")
        default_block = wrapper.split("default_cmd=(", 1)[1].split(")\nhead_cmd=(", 1)[
            0
        ]
        head_block = wrapper.split("head_cmd=(", 1)[1].split(")\nif ", 1)[0]
        self.assertIn('"--warm-production-ready"', default_block)
        self.assertNotIn('\n  "--production-ready"\n', default_block)
        self.assertNotIn('"--op-stats"', default_block)
        self.assertIn('"--op-stats"', head_block)

    def test_warm_perf_presets_do_not_enable_timing_profilers(self) -> None:
        wrapper = (
            Path(__file__).resolve().parent / "run_gliner2_lora_perf_gate.sh"
        ).read_text(encoding="utf-8")
        warm_block = wrapper.split(
            "if (( warm_research || warm_production_ready )); then", 1
        )[1].split("fi", 1)[0]
        self.assertNotIn("op_stats=1", warm_block)
        self.assertNotIn("loop_profile=1", warm_block)
        self.assertNotIn("hazard_profile=1", warm_block)
        self.assertIn("if (( ! warm_production_ready )); then\n    op_stats=1", wrapper)
        self.assertIn("max_dot_general_count_median=1102", wrapper)
        self.assertIn("max_zig_metal_planned_barriers_median=81", wrapper)
        self.assertIn("max_zig_metal_planned_scopes_median=126", wrapper)

    def test_perf_wrapper_preserves_virtualenv_launcher_symlink(self) -> None:
        wrapper = (
            Path(__file__).resolve().parent / "run_gliner2_lora_perf_gate.sh"
        ).read_text(encoding="utf-8")
        self.assertIn("resolve_executable_path()", wrapper)
        self.assertIn("os.path.abspath(os.path.expanduser(sys.argv[1]))", wrapper)
        self.assertIn(
            'python_bin="$(resolve_executable_path "${python_bin}")"', wrapper
        )
        self.assertNotIn('python_bin="$(resolve_path "${python_bin}")"', wrapper)

    def test_run_compare_does_not_reuse_stale_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            run_dir = out_dir / "run-01"
            run_dir.mkdir()
            (run_dir / "comparison_report.json").write_text(
                json.dumps({"summary": {"step_count_valid": True}}), encoding="utf-8"
            )
            completed = SimpleNamespace(returncode=0, stdout="help only")
            with mock.patch.object(benchmark.subprocess, "run", return_value=completed):
                result = benchmark.run_compare(["--help"], out_dir, 1, 1, None)
            self.assertEqual(1, result["returncode"])
            self.assertEqual({}, result["summary"])
            self.assertIn(
                "missing or invalid current comparison report", result["output_tail"]
            )

    def test_metric_rejects_nonfinite_values(self) -> None:
        self.assertIsNone(benchmark.metric({"value": float("nan")}, "value"))
        self.assertIsNone(benchmark.metric({"value": float("inf")}, "value"))

    def test_loss_parity_must_be_true_in_every_requested_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = {
                "run": 1,
                "returncode": 0,
                "elapsed_seconds": 0.0,
                "argv": [],
                "report_path": "unused",
                "summary": {"step_count_valid": True},
                "output_tail": "",
            }
            argv = [
                "benchmark",
                "--runs",
                "1",
                "--out-dir",
                tmp,
                "--require-loss-parity",
            ]
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.object(benchmark, "run_compare", return_value=result),
                mock.patch.object(sys, "stdout", io.StringIO()),
            ):
                self.assertEqual(1, benchmark.main())
            report = json.loads(
                (Path(tmp) / "perf_summary.json").read_text(encoding="utf-8")
            )
            self.assertIn(
                "loss parity was required", " ".join(report["summary"]["failures"])
            )

    def test_rejects_nonfinite_limits_before_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            argv = [
                "benchmark",
                "--runs",
                "1",
                "--out-dir",
                tmp,
                "--max-zig-median-ms",
                "nan",
            ]
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.object(benchmark, "run_compare") as run,
                mock.patch.object(sys, "stderr", io.StringIO()),
            ):
                with self.assertRaisesRegex(SystemExit, "2"):
                    benchmark.main()
                run.assert_not_called()

    def test_independently_trained_tensor_equality_is_diagnostic_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = {
                "run": 1,
                "returncode": 0,
                "elapsed_seconds": 0.0,
                "argv": [],
                "report_path": "unused",
                "summary": {
                    "step_count_valid": True,
                    "trained_adapter_parity_ran": True,
                    "trained_adapter_parity_ok": False,
                },
                "output_tail": "",
            }
            argv = ["benchmark", "--runs", "1", "--out-dir", tmp, "--", "--skip-python"]
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.object(benchmark, "run_compare", return_value=result),
                mock.patch.object(sys, "stdout", io.StringIO()),
            ):
                self.assertEqual(0, benchmark.main())
            report = json.loads(
                (Path(tmp) / "perf_summary.json").read_text(encoding="utf-8")
            )
            diagnostic = report["summary"]["trained_adapter_tensor_equality_diagnostic"]
            self.assertFalse(diagnostic["gating"])
            self.assertEqual(0, diagnostic["within_diagnostic_tolerance_count"])


if __name__ == "__main__":
    unittest.main()
