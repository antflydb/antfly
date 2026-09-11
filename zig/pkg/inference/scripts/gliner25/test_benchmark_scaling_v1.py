from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

import benchmark_scaling_v1 as driver
import benchmark_metal_v2 as campaign
import scaling_contract_v1 as contract
import scaling_runtime_v1 as runtime
from test_benchmark_metal import FakeGuard, contract as old_contract, ready as old_ready
from test_metal_runtime_v2 import admitted, response as native_response


EMPTY = {"entities": [], "classifications": [], "structures": [], "relations": []}


def inputs_fixture():
    original = old_contract()
    names = ["mixed_s128_b2_regular", "mixed_s128_b8_smoke"]
    rows = {}
    for name, batch, mode in zip(names, (2, 8), ("regular", "smoke")):
        rows[name] = {"id": name, "items": [{"id": f"{name}.{i}", "text": "Alice."} for i in range(batch)],
            "expected_input_ids": [[1, 2, 3]] * batch, "kind": "extract", "schema": {"entities": ["person"]},
            "profile": {"mode": mode}}
    return {**original, "workload": "scaling", "cases": names, "source_cases": rows,
            "expected": {name: [copy.deepcopy(EMPTY) for _ in row["items"]] for name, row in rows.items()},
            "prepared": Path("/unused/prepared"), "requests_sha256": "a" * 64,
            "pins": {"native_inputs": {"sha256": campaign.oracle.sha256_file(original["case_path"])}},
            "reference_input_ids": {name: contract.packet(row["expected_input_ids"]) for name, row in rows.items()}}


def readiness(arm, inputs):
    value = old_ready(arm, inputs)
    value.update(scope=contract.SCOPE, workload="scaling", requests_sha256=inputs["requests_sha256"],
                 cases_sha256=inputs["pins"]["native_inputs"]["sha256"])
    if arm == campaign.NATIVE:
        value.update(runtime_contract_version=2, execution_policy="optimized_v2",
                     runtime_policy=copy.deepcopy(campaign.runtime.POLICIES["optimized_v2"]), model_live_bytes=4096)
    else:
        value.update(scaling_input_pins=inputs["pins"])
    return value


class ScalingDriverTests(unittest.TestCase):
    def test_real_pair_orchestration_uses_batch_outputs_and_descriptive_samples(self):
        inputs = inputs_fixture()
        commands, events = {}, []
        class Worker:
            def __init__(self, arm, command, env, directory, guard):
                self.arm, self.cleanup = arm, {}
                commands[arm] = command
                self.process = types.SimpleNamespace(wait=lambda **kwargs: 0)
            def receive(self, timeout):
                return readiness(self.arm, inputs)
            def request(self, op, name="", timeout=35):
                events.append((self.arm, op, name))
                if op == "stop":
                    return {"event": "stopped", "scope": contract.SCOPE, "runtime_contract_version": 2,
                        "execution_policy": "optimized_v2", "model_live_bytes": 0,
                        "transient_live_bytes": 0, "pending_device_bytes": 0}
                value, _ = native_response()
                value.update(scope=contract.SCOPE, outputs=copy.deepcopy(inputs["expected"][name]),
                             output=copy.deepcopy(EMPTY), input_device="mps",
                             **inputs["reference_input_ids"][name])
                return value
            def close(self):
                self.cleanup = {"complete": True}
        args = types.SimpleNamespace(native_bin=Path("/new/metal"), model_root=Path("/models"), upstream=Path("/source"),
            purpose="benchmark", execution_policy="optimized_v2", legacy_reference_bin=False,
            warmup=1, pairs=3, timeout_ms=1000, startup_timeout=10, max_rss_mib=512)
        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(campaign, "Worker", Worker), mock.patch.object(campaign, "ResourceGuard", FakeGuard), \
             mock.patch.object(campaign, "hardware_receipt", return_value={"power_source": "ac", "low_power_mode": "0"}), \
             mock.patch.object(campaign.oracle, "verify_model_dir", return_value=inputs["bundle"]), \
             mock.patch.object(campaign.oracle, "verify_upstream_checkout"), \
             mock.patch.object(contract, "load", return_value=inputs):
            result = campaign.run_pair(args, inputs, "mps", Path(temporary) / "pair",
                repetition=0, phase="measurement", selected=inputs["cases"][:1])
        self.assertEqual("complete", result["status"])
        self.assertEqual(["--workload", "scaling"], commands[campaign.NATIVE][-2:])
        self.assertEqual("metal_scaling_python_worker.py", Path(commands["fastino_mps"][1]).name)
        self.assertIn("--prepared", commands["fastino_mps"])
        row = result["comparisons"][inputs["cases"][0]]
        self.assertEqual(3, row["sample_pairs"])
        self.assertIsNone(row["confidence_interval"])
        self.assertFalse(row["latency_acceptance_claim"])
        self.assertNotIn("python_over_metal_speedup", row)
        self.assertTrue(result["final_owned_cleanup"]["final_owned_cleanup_proved"])

    def run_fake_campaign(self, *, source_smoke_status="complete", paired_smoke_status="validated",
                          cleanup_failed=False):
        inputs, calls = inputs_fixture(), []
        smoke = inputs["cases"][1]
        def cpu_capture(args, values, selected, directory):
            calls.append(("cpu", selected.copy(), args.repetitions, args.warmup, args.pairs))
            result = {"status": "complete", "model": values["model"], "phase": "source_cpu_capture",
                      "cases": {name: {"status": "complete", "outputs": inputs["expected"][name]}
                                for name in selected}}
            if source_smoke_status != "complete":
                result.update(status="partial")
                result["cases"][smoke] = {"status": source_smoke_status, "error": "source B8 failure evidence"}
            if cleanup_failed:
                result.update(status="failed", cleanup_error="source owner was not reaped")
            return result
        def pair(args, values, baseline, directory, *, repetition, phase, selected):
            calls.append((phase, selected.copy(), args.repetitions, args.warmup, args.pairs))
            result = {"status": "complete", "model": values["model"], "phase": phase,
                "case_status": {name: {"status": "validated" if phase == "preflight" else "complete"} for name in selected},
                "comparisons": {name: {"sample_pairs": 3} for name in selected} if phase == "measurement" else {}}
            if phase == "preflight" and smoke in selected and paired_smoke_status != "validated":
                result.update(status="partial")
                result["case_status"][smoke] = {"status": paired_smoke_status, "stage": "validation",
                                                "error": "native B8 output parity failure evidence"}
            return result
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "native"
            binary.write_bytes(b"frozen")
            args = types.SimpleNamespace(output=root / "new", native_bin=binary, prepared=Path("/inputs"),
                model_root=Path("/models"), upstream=Path("/source"), model="small", cases=None,
                purpose="benchmark", execution_policy="optimized_v2", max_rss_mib=512,
                timeout_ms=1000, startup_timeout=10)
            with mock.patch.object(contract, "load", side_effect=lambda *args: copy.deepcopy(inputs)), \
                 mock.patch.object(driver, "capture_cpu", side_effect=cpu_capture), \
                 mock.patch.object(campaign, "run_pair", side_effect=pair), \
                 mock.patch.object(campaign, "source_snapshot", return_value={"source_files_sha256": "c" * 64}), \
                 mock.patch.object(driver.oracle, "verify_dependencies", return_value={}), \
                 mock.patch.object(driver.oracle, "verify_upstream_checkout", return_value={}), \
                 mock.patch.object(driver.oracle, "verify_config_fixtures"), \
                 mock.patch.object(driver.oracle, "verify_model_dir", return_value=inputs["bundle"]):
                exit_code = driver.driver(args)
            report = json.loads((args.output / "report.json").read_bytes())
        return exit_code, report, calls, inputs

    def test_default_campaign_is_one_by_one_by_three_and_b8_remains_correctness_only(self):
        exit_code, report, calls, inputs = self.run_fake_campaign()
        self.assertEqual(0, exit_code)
        self.assertEqual(["cpu", "preflight", "measurement"], [row[0] for row in calls])
        self.assertTrue(all(row[2:] == (1, 1, 3) for row in calls))
        self.assertEqual(inputs["cases"], calls[1][1])
        self.assertEqual(inputs["cases"][:1], calls[2][1])
        self.assertEqual("correctness_only", report["summary"][1]["status"])
        self.assertFalse(report["original_30_case_gate"])
        self.assertFalse(report["confidence_intervals"])
        self.assertTrue(report["parity_validated"])

    def test_failed_and_unprocessed_b8_keep_denominator_and_original_error_evidence(self):
        for source_status, expected in (("failed", "failed"), ("unprocessed", "blocked")):
            with self.subTest(source_status=source_status):
                exit_code, report, calls, inputs = self.run_fake_campaign(source_smoke_status=source_status)
                self.assertEqual(2, exit_code)
                self.assertFalse(report["parity_validated"])
                self.assertEqual(inputs["cases"], [row["case_id"] for row in report["summary"]])
                regular, smoke = report["summary"]
                self.assertEqual("complete", regular["status"])
                self.assertEqual(expected, smoke["status"])
                self.assertTrue(smoke["correctness_only"])
                self.assertNotIn("sample_pairs", smoke)
                self.assertEqual("source B8 failure evidence", smoke["evidence"][0]["error"])
                self.assertEqual(inputs["cases"][:1], calls[1][1])
                self.assertEqual(inputs["cases"][:1], calls[2][1])

    def test_failed_b8_preflight_keeps_regular_samples_and_exact_failure_evidence(self):
        exit_code, report, calls, inputs = self.run_fake_campaign(paired_smoke_status="blocked")
        self.assertEqual(2, exit_code)
        self.assertEqual("complete", report["summary"][0]["status"])
        smoke = report["summary"][1]
        self.assertEqual("blocked", smoke["status"])
        self.assertEqual("native B8 output parity failure evidence", smoke["evidence"][1]["error"])
        self.assertEqual("validation", smoke["evidence"][1]["stage"])
        self.assertNotIn("sample_pairs", smoke)
        self.assertEqual(inputs["cases"][:1], calls[2][1])

    def test_early_cleanup_failure_still_reports_every_b8_and_stops_campaign(self):
        exit_code, report, calls, inputs = self.run_fake_campaign(cleanup_failed=True)
        self.assertEqual(2, exit_code)
        self.assertEqual("failed", report["status"])
        self.assertEqual(["cpu"], [row[0] for row in calls])
        self.assertEqual(inputs["cases"], [row["case_id"] for row in report["summary"]])
        self.assertTrue(all(row["status"] == "failed" for row in report["summary"]))
        self.assertEqual("source owner was not reaped", report["summary"][1]["evidence"][0]["run_cleanup_error"])

    def test_later_batch_item_parity_failure_advances_only_valid_owner_receipt(self):
        inputs = inputs_fixture()
        name = inputs["cases"][0]
        case, expected = inputs["source_cases"][name], inputs["expected"][name]
        state = admitted(capacity=2048)
        value, _ = native_response(wa=1024, generation=1)
        value.update(outputs=copy.deepcopy(expected), output=copy.deepcopy(EMPTY),
                     **inputs["reference_input_ids"][name])
        # Sample zero remains identical: a later batch item must still reject.
        value["outputs"][1]["classifications"].append({"name": "unexpected", "labels": []})
        with self.assertRaisesRegex(campaign.cpu.BenchmarkError, "outputs\\[1\\]"):
            runtime.result(campaign.NATIVE, value, case, expected, validation=True,
                           phase="validation", native_receipt=state)
        self.assertEqual((1024, 1), (state.workspace_live_bytes, state.workspace_generation))
        valid, _ = native_response(wb=1024, wa=1024, generation=1)
        valid.update(outputs=copy.deepcopy(expected), output=copy.deepcopy(EMPTY),
                     **inputs["reference_input_ids"][name])
        self.assertEqual(expected, runtime.result(campaign.NATIVE, valid, case, expected,
                         validation=True, phase="validation", native_receipt=state))

    def test_cpu_capture_unsafe_error_retains_unprocessed_rows_and_reaps_worker(self):
        inputs = inputs_fixture()
        closed = []
        class Worker:
            def __init__(self, *args):
                self.cleanup = {}
                self.process = types.SimpleNamespace(wait=lambda **kwargs: 1)
            def receive(self, timeout):
                return readiness("fastino_cpu", inputs)
            def request(self, *args):
                return {"event": "error", "category": "out_of_memory", "message": "bounded OOM",
                        "recoverable": False, "device_unsafe": True}
            def close(self):
                closed.append(True)
                self.cleanup = {"complete": True}
        args = types.SimpleNamespace(max_rss_mib=512, model_root=Path("/models"), upstream=Path("/source"),
            prepared=Path("/inputs"), startup_timeout=10, timeout_ms=1000, execution_policy="optimized_v2")
        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(driver, "Worker", Worker), mock.patch.object(driver, "ResourceGuard", FakeGuard):
            report = driver.capture_cpu(args, inputs, inputs["cases"], Path(temporary) / "capture")
            self.assertTrue((Path(temporary) / "capture/events.jsonl").is_file())
        self.assertEqual("failed", report["status"])
        self.assertEqual("failed", report["cases"][inputs["cases"][0]]["status"])
        self.assertEqual("unprocessed", report["cases"][inputs["cases"][1]]["status"])
        self.assertEqual([True], closed)
        self.assertTrue(report["cleanup"]["complete"])


if __name__ == "__main__":
    unittest.main()
