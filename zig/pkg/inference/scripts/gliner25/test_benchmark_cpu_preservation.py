from __future__ import annotations

import copy
import tempfile
from pathlib import Path
import types
import unittest
from unittest import mock

import benchmark_cpu_preservation as preservation
import benchmark_metal_v2 as campaign
from test_benchmark_metal import FakeGuard, contract as fixture_contract


class CPUPreservationTests(unittest.TestCase):
    def test_fresh_cpu_binary_uses_cpu_reference_only_and_all_actual_pair_samples(self):
        fixture = fixture_contract()
        commands, actions = {}, []
        class Worker:
            def __init__(self, arm, command, env, directory, guard):
                commands[arm] = command
                self.arm, self.cleanup = arm, {}
                self.process = types.SimpleNamespace(wait=lambda **kwargs: 0)
            def receive(self, timeout):
                return {"event": "ready", "arm": self.arm, "scope": campaign.cpu.SCOPE,
                    "timing_boundary": campaign.cpu.TIMING_BOUNDARY, **{
                        "model_id": fixture["bundle"]["model_id"], "revision": fixture["bundle"]["revision"],
                        "model_files": fixture["bundle"]["files"]},
                    "dtype": "float32", "threads": 1, "interop_threads": 1, "qualification": False,
                    "build_mode": "ReleaseFast", "scheduler": "serial_io",
                    "cases_sha256": campaign.oracle.sha256_file(fixture["case_path"])}
            def request(self, op, name="", timeout=35):
                actions.append((self.arm, op, name))
                if op == "stop":
                    return {"event": "stopped"}
                return {"event": "result", "duration_ns": 100 if self.arm == "native" else 200,
                        "input_ids": [1, 2, 3], "output": copy.deepcopy(fixture["expected"][name])}
            def close(self):
                self.cleanup = {"complete": True}
        args = types.SimpleNamespace(native_bin=Path("/new/frozen/cpu-binary"), model_root=Path("/models"),
            upstream=Path("/source"), warmup=5, pairs=30, timeout_ms=1000, startup_timeout=10,
            max_rss_mib=512, execution_policy="reference_v1", legacy_reference_bin=True, purpose="benchmark")
        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(campaign, "Worker", Worker), mock.patch.object(campaign, "ResourceGuard", FakeGuard), \
             mock.patch.object(campaign, "hardware_receipt", return_value={"power_source": "ac", "low_power_mode": "0"}), \
             mock.patch.object(campaign.oracle, "verify_model_dir", return_value=fixture["bundle"]), \
             mock.patch.object(campaign.oracle, "verify_upstream_checkout"):
            result = campaign.run_pair(args, fixture, "cpu", Path(temporary) / "pair",
                repetition=1, phase="measurement", selected=fixture["cases"], native_backend="native")
        self.assertEqual("complete", result["status"])
        self.assertEqual("/new/frozen/cpu-binary", commands["native"][0])
        self.assertNotIn("--execution-policy", commands["native"])
        self.assertNotIn("--diagnostics", commands["native"])
        self.assertEqual("benchmark_cpu.py", Path(commands["python"][1]).name)
        self.assertEqual(30 * len(fixture["cases"]), len(result["pairs"]))
        self.assertEqual(35 * len(fixture["cases"]) * 2, sum(op == "run" for _, op, _ in actions))
        for row in result["comparisons"].values():
            self.assertEqual(0.5, row["native_over_python_latency"]["median"])
            self.assertNotIn("metal_ns", row)
        with self.assertRaises(campaign.BenchmarkError):
            campaign.run_pair(args, fixture, "mps", Path("/unused"), repetition=0, phase="measurement",
                              selected=fixture["cases"], native_backend="native")

    def model(self):
        variant = "small"
        fixture = campaign.oracle.read_json(campaign.cpu.case_fixture(variant))
        bundle = preservation.bundle_for(variant)
        manifest = campaign.oracle.load_manifest()
        ready = {"event": "ready", "scope": campaign.cpu.SCOPE, "timing_boundary": campaign.cpu.TIMING_BOUNDARY,
                 "model_id": bundle["model_id"], "revision": bundle["revision"], "model_files": bundle["files"],
                 "dtype": "float32", "threads": 1, "qualification": False}
        rows = {}
        pairs = []
        comparison = {"sample_pairs": 30, "native_ns": campaign.paired_benchmark.distribution([100] * 30),
            "python_ns": campaign.paired_benchmark.distribution([200] * 30),
            "native_over_python_latency": campaign.paired_benchmark.paired_log_ratio_ci([(100, 200)] * 30, samples=2000)}
        for case in fixture["cases"]:
            expected = campaign.cpu.canonical_result(case["expected"])
            rows[case["id"]] = {"outputs_match_oracle": True, "confidence_absolute_tolerance": 5e-4,
                "expected": expected, "outputs": {"native": expected, "python": expected}, "input_ids": [1, 2, 3]}
            pairs.extend({"pair": index, "case_id": case["id"], "valid": True,
                "order": ["native", "python"] if index % 2 else ["python", "native"],
                "native": {"duration_ns": 100}, "python": {"duration_ns": 200}} for index in range(1, 31))
        return {"model": variant, "status": "complete", "model_bundle": bundle, "validation": rows,
            "pairs": pairs, "comparisons": {name: copy.deepcopy(comparison) for name in rows},
            "workers": {"native": {**ready, "arm": "native", "build_mode": "ReleaseFast", "scheduler": "serial_io",
                "cases_sha256": campaign.oracle.sha256_file(campaign.cpu.case_fixture(variant))},
                "python": {**ready, "arm": "python", "interop_threads": 1, "provenance": {
                    "runtime": manifest["runtime"], "commit": manifest["upstream"]["commit"], "device": "cpu", "dtype": "float32"}}}}

    def test_audit_rederives_ci_and_rejects_pair_loss_output_drift_or_foreign_model(self):
        model = self.model()
        self.assertEqual(10, len(preservation.checked_model(model)))
        mutations = (
            lambda value: value["pairs"].pop(),
            lambda value: value["pairs"][0]["native"].update(duration_ns=True),
            lambda value: value["pairs"].__setitem__(1, value["pairs"][0]),
            lambda value: value["model_bundle"].update(revision="0" * 40),
            lambda value: next(iter(value["comparisons"].values()))["native_over_python_latency"].update(upper_95=0.1),
            lambda value: next(iter(value["validation"].values())).update(confidence_absolute_tolerance=0.01),
        )
        for mutate in mutations:
            changed = copy.deepcopy(model)
            mutate(changed)
            with self.subTest(mutation=mutate), self.assertRaises((campaign.cpu.BenchmarkError, campaign.BenchmarkError)):
                preservation.checked_model(changed)

    def test_three_distinct_reports_are_mandatory_before_any_file_open(self):
        with mock.patch.object(preservation, "read_pinned") as read:
            with self.assertRaises(campaign.cpu.BenchmarkError):
                preservation.audit_reports([Path("a"), Path("a"), Path("b")])
            read.assert_not_called()


if __name__ == "__main__":
    unittest.main()
