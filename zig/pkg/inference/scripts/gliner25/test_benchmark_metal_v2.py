from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

import benchmark_metal_v2 as bench
from test_benchmark_metal import FakeGuard, contract as fixture_contract, ready as ready_v1
from test_metal_runtime_v2 import response as response_v2


class DriverTests(unittest.TestCase):
    def run_fake(self, *, purpose="measurement", mutate=None, policy="optimized_v2", legacy=False,
                 campaign_purpose="benchmark", workspace_capacity=0):
        fixture = fixture_contract()
        events, commands = [], {}

        class FakeWorker:
            def __init__(self, arm, command, env, directory, guard):
                self.arm, self.cleanup = arm, {}
                self.process = types.SimpleNamespace(wait=lambda **kwargs: 0)
                commands[arm] = command
                events.append((arm, "start", ""))

            def receive(self, timeout):
                event = ready_v1(self.arm, fixture)
                event["scope"] = bench.SCOPE
                if self.arm == bench.NATIVE:
                    if legacy:
                        event["scope"] = bench.v1.SCOPE
                    else:
                        event.update(runtime_contract_version=2, execution_policy=policy,
                                     runtime_policy=copy.deepcopy(bench.runtime.POLICIES[policy]),
                                     model_live_bytes=4096 if policy == "optimized_v2" else 0,
                                     workspace_capacity_bytes=workspace_capacity)
                return event

            def request(self, op, case_id="", timeout=35):
                events.append((self.arm, op, case_id))
                if op == "stop":
                    event = {"event": "stopped", "arm": self.arm, "scope": bench.SCOPE}
                    if self.arm == bench.NATIVE and not legacy:
                        event.update(runtime_contract_version=2, execution_policy=policy,
                                     model_live_bytes=0, transient_live_bytes=0, pending_device_bytes=0)
                else:
                    event, _ = response_v2(policy)
                    event.update(arm=self.arm, case_id=case_id, scope=bench.SCOPE,
                                 duration_ns=100 if self.arm == bench.NATIVE else 200,
                                 output=copy.deepcopy(fixture["expected"][case_id]),
                                 input_device="mps")
                    if legacy:
                        for key in ("runtime_contract_version", "execution_policy"):
                            event.pop(key)
                if mutate:
                    mutate(self.arm, op, case_id, event)
                return event

            def close(self):
                events.append((self.arm, "close", ""))
                self.cleanup = {"complete": True}

        with tempfile.TemporaryDirectory() as temporary:
            args = types.SimpleNamespace(native_bin=Path("/unused/native"), model_root=Path("/unused/models"),
                upstream=Path("/unused/upstream"), warmup=1, pairs=2, timeout_ms=1000,
                startup_timeout=10, max_rss_mib=512, execution_policy=policy, legacy_reference_bin=legacy)
            args.purpose = campaign_purpose
            path = Path(temporary) / "pair"
            with mock.patch.object(bench, "Worker", FakeWorker), mock.patch.object(bench, "ResourceGuard", FakeGuard), \
                 mock.patch.object(bench, "hardware_receipt", return_value={"power_source": "ac", "low_power_mode": "0"}), \
                 mock.patch.object(bench.oracle, "verify_model_dir", return_value=fixture["bundle"]), \
                 mock.patch.object(bench.oracle, "verify_upstream_checkout"):
                result = bench.run_pair(args, fixture, "mps", path, repetition=0, phase=purpose, selected=fixture["cases"])
            journal = [json.loads(line) for line in (path / "events.jsonl").read_text().splitlines()]
        return result, events, commands, journal

    def test_optimized_policy_forwarded_and_final_cleanup_required(self):
        result, events, commands, journal = self.run_fake()
        self.assertEqual("complete", result["status"])
        self.assertEqual(["--execution-policy", "optimized_v2"], commands[bench.NATIVE][-2:])
        self.assertIn("metal_python_worker_v2.py", commands["fastino_mps"][1])
        self.assertTrue(result["final_owned_cleanup"]["final_owned_cleanup_proved"])
        self.assertEqual(2, sum(op == "close" for _, op, _ in events))
        for name in fixture_contract()["cases"]:
            self.assertEqual(2, result["comparisons"][name]["sample_pairs"])
        self.assertEqual(8, sum(row["stage"] == "measurement" for row in journal))

    def test_explicit_original_reference_does_not_invent_v2_cleanup_evidence(self):
        result, _, commands, _ = self.run_fake(policy="reference_v1", legacy=True)
        self.assertEqual("complete", result["status"])
        self.assertNotIn("--execution-policy", commands[bench.NATIVE])
        self.assertFalse(result["final_owned_cleanup"]["final_owned_cleanup_proved"])

    def test_diagnostics_are_separate_and_never_produce_latency_comparisons(self):
        result, events, commands, journal = self.run_fake(purpose="diagnostic")
        self.assertEqual("complete", result["status"])
        self.assertEqual({}, result["comparisons"])
        self.assertEqual(4, sum(op == "run" for _, op, _ in events))
        self.assertFalse(any(row["stage"] in ("measurement", "warmup") for row in journal))
        self.assertEqual(["--diagnostics", "true"], commands[bench.NATIVE][-2:])

    def test_diagnostic_campaign_preflight_uses_instrumentation_but_legacy_never_does(self):
        _, _, commands, _ = self.run_fake(purpose="preflight", campaign_purpose="diagnostic")
        self.assertEqual(["--diagnostics", "true"], commands[bench.NATIVE][-2:])
        _, _, commands, _ = self.run_fake(purpose="preflight", campaign_purpose="diagnostic",
                                         policy="reference_v1", legacy=True)
        self.assertNotIn("--diagnostics", commands[bench.NATIVE])

    def test_pending_close_or_request_upload_invalidates_run_and_retains_events(self):
        for failed_stage in ("stop", "validate"):
            def mutate(arm, op, case, event):
                if arm == bench.NATIVE and op == failed_stage:
                    if op == "stop":
                        event["pending_device_bytes"] = 4
                    else:
                        event["runtime_stats"]["actual_weight_upload_bytes"] = 4
            with self.subTest(stage=failed_stage):
                result, events, _, journal = self.run_fake(mutate=mutate)
                self.assertEqual("failed", result["status"])
                self.assertEqual({}, result["comparisons"])
                self.assertTrue(journal)
                self.assertEqual(2, sum(op == "close" for _, op, _ in events))

    def test_schema_or_output_failure_does_not_admit_earlier_case_samples(self):
        first = fixture_contract()["cases"][0]
        count = 0
        def mutate(arm, op, case, event):
            nonlocal count
            if arm == "fastino_mps" and op == "run" and case == first:
                count += 1
                if count == 3:
                    event["output"]["entities"] = []
        result, _, _, journal = self.run_fake(mutate=mutate)
        self.assertEqual("partial", result["status"])
        self.assertNotIn(first, result["comparisons"])
        self.assertEqual(2, len([row for row in result["pairs"] if row["case_id"] == first]))
        self.assertTrue(any(row["stage"] == "measurement" for row in journal))

    def test_policy_cannot_omit_native_flag_without_explicit_reference(self):
        args = types.SimpleNamespace(execution_policy="optimized_v2", legacy_reference_bin=True)
        with self.assertRaises(bench.BenchmarkError):
            bench.driver(args)

    def test_workspace_growth_on_rejected_case_keeps_later_case_measurable(self):
        first, second = fixture_contract()["cases"]
        live = 0
        def mutate(arm, op, case, event):
            nonlocal live
            if arm == bench.NATIVE and op != "stop":
                owner, _ = response_v2(wb=live, wa=1024, generation=1)
                event.update(owned_memory=owner["owned_memory"], runtime_stats=owner["runtime_stats"])
                live = 1024
                if case == first:
                    event["output"]["classifications"].append({"name": "unexpected", "labels": []})
        result, events, _, journal = self.run_fake(mutate=mutate, workspace_capacity=2048)
        self.assertEqual("partial", result["status"])
        self.assertEqual("blocked", result["case_status"][first]["status"])
        self.assertEqual("complete", result["case_status"][second]["status"])
        self.assertEqual([second], list(result["comparisons"]))
        self.assertEqual(2, result["comparisons"][second]["sample_pairs"])
        self.assertEqual(1, len(result["failures"]))
        self.assertTrue(any(row["stage"] == "validation" and row["response"].get("case_id") == first
                            for row in journal))
        self.assertFalse(any(op == "run" and case == first for _, op, case in events))
        self.assertTrue(result["final_owned_cleanup"]["final_owned_cleanup_proved"])


class AggregationTests(unittest.TestCase):
    def report(self, repetitions=(0, 1, 2)):
        comparison = {"sample_pairs": 30, "metal_ns": {"median": 100}, "python_ns": {"median": 200},
                      "python_over_metal_speedup": {"lower_95": 1.9, "median": 2.0, "upper_95": 2.1}}
        return {"contracts": [{"model": "small", "cases": ["case"]}], "baselines": ["mps"],
                "execution_policy": "optimized_v2", "repetitions": 3,
                "runs": [{"phase": "measurement", "model": "small", "baseline": "mps",
                          "repetition": repetition, "comparisons": {"case": copy.deepcopy(comparison)},
                          "hardware_start": {"power_source": "ac", "low_power_mode": "0"}}
                         for repetition in repetitions]}

    def test_exact_repetitions_accept_any_saved_order_but_not_duplicate_sessions(self):
        for repetitions in ((0, 1, 2), (2, 0, 1)):
            row = bench.aggregate(self.report(repetitions))[0]
            self.assertEqual("complete", row["status"])
            self.assertTrue(row["milestone"]["passed"])
        for repetitions in ((0, 0, 0), (0, 0, 2), (0, 1, 3), (0, 1), (0, 1, 2, 2),
                            (False, 1, 2), (0, 1.0, 2), (0, "1", 2)):
            with self.subTest(repetitions=repetitions):
                row = bench.aggregate(self.report(repetitions))[0]
                self.assertEqual("blocked", row["status"])
                self.assertFalse(row["milestone"]["passed"])
                self.assertEqual(len(repetitions), len(row["repetitions"]))
                self.assertIn("exact unique IDs", row["error"])

    def test_case_comparison_cannot_override_its_worker_repetition_identity(self):
        report = self.report((0, 0, 0))
        for claimed, run in enumerate(report["runs"]):
            run["comparisons"]["case"]["repetition"] = claimed
        row = bench.aggregate(report)[0]
        self.assertEqual([0, 0, 0], [value["repetition"] for value in row["repetitions"]])
        self.assertFalse(row["milestone"]["passed"])

    def test_one_failing_independent_interval_cannot_hide_behind_two_winners(self):
        report = self.report()
        report["runs"][1]["comparisons"]["case"]["python_over_metal_speedup"]["lower_95"] = .82
        row = bench.aggregate(report)[0]
        self.assertEqual("complete", row["status"])
        self.assertFalse(row["milestone"]["passed"])


if __name__ == "__main__":
    unittest.main()
