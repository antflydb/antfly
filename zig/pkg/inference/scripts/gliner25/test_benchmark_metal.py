from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

import benchmark_metal as bench


def contract():
    fixture = bench.oracle.read_json(bench.cpu.case_fixture("small"))
    model = bench.oracle.load_manifest()["models"]["small"]
    names = [case["id"] for case in fixture["cases"][:2]]
    return {
        "model": "small",
        "bundle": {"model_id": model["model_id"], "revision": model["revision"],
                   "files": {name: {key: item[key] for key in ("size_bytes", "sha256")}
                             for name, item in model["files"].items()}},
        "case_path": bench.cpu.case_fixture("small"), "cases": names,
        "expected": {case["id"]: bench.cpu.canonical_result(case["expected"])
                     for case in fixture["cases"][:2]},
    }


def ready(arm, fixture):
    result = {
        "event": "ready", "arm": arm, "scope": bench.SCOPE, "timing_boundary": bench.TIMING_BOUNDARY,
        "model": fixture["model"], "model_id": fixture["bundle"]["model_id"],
        "revision": fixture["bundle"]["revision"], "model_files": fixture["bundle"]["files"],
        "dtype": "float32", "threads": 1, "qualification": False,
    }
    if arm == bench.NATIVE:
        result.update(backend="metal", device="metal", build_mode="ReleaseFast", scheduler="serial_requests",
                      runtime_ready=True, external_frame=False,
                      math_policy="production_default", host_fallback_allowed=False,
                      native_environment={name: None for name in bench.NATIVE_KNOBS},
                      cases_sha256=bench.oracle.sha256_file(fixture["case_path"]))
    else:
        device = arm.removeprefix("fastino_")
        result.update(
            device=device, provenance={"device": device}, interop_threads=1,
            parameter_device=device, floating_dtype="float32", deterministic_algorithms=True, strict_extraction=True,
            requests_sha256=bench.oracle.sha256_file(bench.oracle.FIXTURES / "requests.json"),
            math_policy="pytorch_fp32_deterministic_no_mps_fallback_no_fast_math_v1",
            synchronization_policy=("torch_mps_synchronize_before_start_and_after_extract_v1"
                                    if device == "mps" else "synchronous_cpu_v1"),
        )
    return result


class FakeGuard:
    def __init__(self, limit):
        self.limit = limit

    def receipt(self):
        return {"max_rss_bytes": self.limit}


class ContractTests(unittest.TestCase):
    def test_readiness_rejects_wrong_device_sync_artifact_and_build(self):
        fixture = contract()
        for arm in (bench.NATIVE, "fastino_mps", "fastino_cpu"):
            correct = ready(arm, fixture)
            bench.checked_ready(arm, correct, fixture["bundle"], "small", fixture["case_path"])
            mutations = [("revision", "wrong"), ("threads", 2), ("dtype", "float16"),
                         ("timing_boundary", "encoder_only"), ("qualification", True)]
            if arm == bench.NATIVE:
                mutations += [("device", "cpu"), ("build_mode", "Debug"), ("external_frame", True),
                              ("runtime_ready", False)]
            else:
                mutations += [("device", "cpu" if arm == "fastino_mps" else "mps"),
                              ("provenance", {"device": "cuda"}), ("synchronization_policy", "none"),
                              ("math_policy", "fallback_allowed"), ("deterministic_algorithms", False)]
            for key, value in mutations:
                with self.subTest(arm=arm, key=key), self.assertRaises(bench.BenchmarkError):
                    bench.checked_ready(arm, {**correct, key: value}, fixture["bundle"], "small", fixture["case_path"])

    def test_environment_uses_one_explicit_production_profile(self):
        values = {name: "1" for name in (*bench.NATIVE_KNOBS, *bench.MPS_KNOBS)}
        with mock.patch.dict(os.environ, values):
            env, evidence = bench.worker_environment()
        for name in bench.NATIVE_KNOBS:
            self.assertNotIn(name, env)
            self.assertEqual("1", evidence["inherited"][name])
        self.assertEqual("0", env["PYTORCH_ENABLE_MPS_FALLBACK"])
        self.assertEqual("0", env["PYTORCH_MPS_FAST_MATH"])
        self.assertNotIn("PYTORCH_MPS_PREFER_METAL", env)
        for name in bench.cpu.THREAD_ENV:
            self.assertEqual("1", env[name])

    def test_frozen_extraction_token_captures_are_loaded_for_all_variants(self):
        for variant in bench.VARIANTS:
            with self.subTest(variant=variant), mock.patch.object(bench.oracle, "verify_model_dir", return_value={}):
                loaded = bench.load_contract(variant, Path("/unused"), None)
            self.assertEqual(10, len(loaded["cases"]))
            self.assertEqual(8, len(loaded["reference_input_ids"]))
            self.assertNotIn("constrained_classification", loaded["reference_input_ids"])
            self.assertNotIn("joint_ie", loaded["reference_input_ids"])
            self.assertTrue(all(ids and all(type(token) is int for token in ids)
                                for ids in loaded["reference_input_ids"].values()))

    def run_fake(self, *, phase="measurement", mutate=None, cleanup_complete=True, power_change=False):
        fixture = contract()
        events = []

        class FakeWorker:
            def __init__(self, arm, command, env, directory, guard):
                self.arm = arm
                self.process = types.SimpleNamespace(wait=lambda **kwargs: 0)
                self.cleanup = {}
                self.counts = {}
                events.append((arm, "start", None))

            def receive(self, timeout):
                return ready(self.arm, fixture)

            def request(self, op, case_id="", timeout=35):
                events.append((self.arm, op, case_id))
                if op == "stop":
                    return {"event": "stopped", "arm": self.arm}
                key = (op, case_id)
                self.counts[key] = self.counts.get(key, 0) + 1
                response = {
                    "event": "result", "arm": self.arm, "case_id": case_id,
                    "duration_ns": 100 if self.arm == bench.NATIVE else 200,
                    "input_ids": [1, 2, 3] if op == "validate" else None,
                    "input_device": self.arm.removeprefix("fastino_"),
                    "output": copy.deepcopy(fixture["expected"][case_id]),
                    "gpu_work_submitted": self.arm == bench.NATIVE,
                }
                if self.arm == bench.NATIVE:
                    memory = {key: 0 for key in (
                        "device_owned_live_bytes", "host_mirror_live_bytes",
                        "device_owned_buffers_created", "device_owned_buffers_released",
                        "device_owned_bytes_created", "device_owned_bytes_released",
                        "host_mirror_allocations", "host_mirror_frees")}
                    response.update(
                        external_frame=False, request_stats={"encoder": {"device_dispatches": 12}},
                        host_fallback_evidence={"strict_device_dispatch": True,
                                               "host_mirror_allocations_delta": 0,
                                               "host_mirror_download_bytes_delta": 0,
                                               "to_host_device_calls_delta": 0},
                        owned_memory={"before": dict(memory), "after": dict(memory)},
                    )
                if mutate:
                    mutate(self.arm, op, case_id, self.counts[key], response, fixture)
                return response

            def close(self):
                self.cleanup = {"closed": True, "complete": cleanup_complete}
                events.append((self.arm, "close", None))

        with tempfile.TemporaryDirectory() as temporary:
            args = types.SimpleNamespace(
                native_bin=Path("/unused/metal"), model_root=Path("/unused/models"),
                upstream=Path("/unused/upstream"), warmup=1, pairs=2,
                timeout_ms=1000, startup_timeout=10, max_rss_mib=512,
            )
            directory = Path(temporary) / "run"
            with mock.patch.object(bench, "Worker", FakeWorker), mock.patch.object(bench, "ResourceGuard", FakeGuard), \
                 mock.patch.object(bench, "hardware_receipt", side_effect=[
                     {"power_source": "ac", "low_power_mode": "0"},
                     {"power_source": "battery" if power_change else "ac", "low_power_mode": "0"}]), \
                 mock.patch.object(bench.oracle, "verify_model_dir", return_value=fixture["bundle"]), \
                 mock.patch.object(bench.oracle, "verify_upstream_checkout"):
                result = bench.run_pair(args, fixture, "mps", directory, repetition=0,
                                        phase=phase, selected=fixture["cases"])
            journal = [json.loads(line) for line in (directory / "events.jsonl").read_text().splitlines()]
            self.assertEqual(json.loads(json.dumps(result)), json.loads((directory / "run.json").read_text()))
        return result, events, journal

    def test_validation_precedes_timing_and_pair_order_is_balanced(self):
        result, events, journal = self.run_fake()
        self.assertEqual("complete", result["status"])
        calls = [(arm, op) for arm, op, _ in events if op not in ("start", "close", "stop")]
        self.assertEqual([(bench.NATIVE, "validate"), ("fastino_mps", "validate")] * 2, calls[:4])
        for name in contract()["cases"]:
            rows = [row for row in result["pairs"] if row["case_id"] == name]
            self.assertEqual([(bench.NATIVE, "fastino_mps"), ("fastino_mps", bench.NATIVE)],
                             [row["order"] for row in rows])
            self.assertEqual(2.0, result["comparisons"][name]["python_over_metal_speedup"]["median"])
        measured = [event for event in journal if event["stage"] == "measurement"]
        self.assertEqual(8, len(measured))
        self.assertTrue(all("output" in event["response"] for event in measured))
        self.assertEqual(2, sum(op == "close" for _, op, _ in events))

    def test_recoverable_unsupported_case_is_preserved_without_discarding_other_rows(self):
        def unsupported(arm, op, name, count, response, fixture):
            if arm == "fastino_mps" and name == fixture["cases"][0] and op == "validate":
                response.clear()
                response.update(event="error", category="unsupported_operator", message="unsupported op",
                                recoverable=True, device_unsafe=False)
        result, events, journal = self.run_fake(mutate=unsupported)
        failed, successful = contract()["cases"]
        self.assertEqual("partial", result["status"])
        self.assertEqual("blocked", result["case_status"][failed]["status"])
        self.assertNotIn(failed, result["comparisons"])
        self.assertIn(successful, result["comparisons"])
        self.assertFalse(any(op == "run" and name == failed for _, op, name in events))
        self.assertTrue(any(event.get("response", {}).get("event") == "error" for event in journal))

    def test_measured_parity_failure_invalidates_entire_case_but_retains_raw_samples(self):
        def corrupt(arm, op, name, count, response, fixture):
            if arm == "fastino_mps" and name == fixture["cases"][0] and op == "run" and count == 3:
                response["output"]["entities"] = []
        result, _, journal = self.run_fake(mutate=corrupt)
        failed = contract()["cases"][0]
        self.assertEqual("partial", result["status"])
        self.assertNotIn(failed, result["comparisons"])
        self.assertEqual(2, len([row for row in result["pairs"] if row["case_id"] == failed]))
        self.assertTrue(any(not row["valid"] for row in result["pairs"]))
        self.assertEqual(7, len([event for event in journal if event["stage"] == "measurement"]))

    def test_unsafe_device_failure_aborts_comparison_and_closes_both_workers(self):
        def fail(arm, op, name, count, response, fixture):
            if arm == "fastino_mps":
                response.update(event="error", recoverable=False, device_unsafe=True, message="device lost")
        result, events, _ = self.run_fake(mutate=fail)
        self.assertEqual("failed", result["status"])
        self.assertEqual({}, result["comparisons"])
        self.assertEqual(2, sum(op == "close" for _, op, _ in events))

    def test_native_memory_leak_and_missing_dispatch_are_fatal(self):
        for failure in ("leak", "missing_dispatch", "host_fallback", "boolean_live_bytes", "float_live_bytes"):
            def corrupt(arm, op, name, count, response, fixture):
                if arm == bench.NATIVE:
                    if failure == "leak":
                        response["owned_memory"]["after"]["device_owned_live_bytes"] = 1024
                    elif failure == "missing_dispatch":
                        response["request_stats"]["encoder"]["device_dispatches"] = 0
                    elif failure == "boolean_live_bytes":
                        response["owned_memory"]["after"]["device_owned_live_bytes"] = False
                    elif failure == "float_live_bytes":
                        response["owned_memory"]["after"]["host_mirror_live_bytes"] = 0.0
                    else:
                        response["host_fallback_evidence"]["to_host_device_calls_delta"] = 1
            with self.subTest(failure=failure):
                result, events, _ = self.run_fake(mutate=corrupt)
                self.assertEqual("failed", result["status"])
                self.assertEqual({}, result["comparisons"])
                self.assertEqual(2, sum(op == "close" for _, op, _ in events))

    def test_preflight_has_no_warmups_or_measured_calls(self):
        result, events, _ = self.run_fake(phase="preflight")
        self.assertEqual("complete", result["status"])
        self.assertFalse(any(op == "run" for _, op, _ in events))
        self.assertEqual({}, result["comparisons"])

    def test_previously_failed_idempotent_cleanup_still_invalidates_run(self):
        result, _, _ = self.run_fake(cleanup_complete=False)
        self.assertEqual("failed", result["status"])
        self.assertTrue(result["cleanup_errors"])
        self.assertEqual({}, result["comparisons"])

    def test_power_change_retains_samples_but_invalidates_repetition(self):
        result, _, journal = self.run_fake(power_change=True)
        self.assertEqual("failed", result["status"])
        self.assertFalse(result["power_profile_stable"])
        self.assertEqual({}, result["comparisons"])
        self.assertTrue(all(value["status"] == "blocked" for value in result["case_status"].values()))
        self.assertEqual(8, len([event for event in journal if event["stage"] == "measurement"]))

    def run_driver_fake(self, *, cleanup_error=False, source_drift=False):
        events = []
        initial = {"head": "head", "source_files_sha256": "0" * 64, "files": {}}
        final = {**initial, "source_files_sha256": "1" * 64} if source_drift else initial

        def load(variant, model_root, requested):
            fixture = contract()
            fixture["model"] = variant
            return fixture

        def pair(args, fixture, baseline, directory, *, repetition, phase, selected):
            events.append((phase, repetition, fixture["model"], baseline))
            run = {"phase": phase, "repetition": repetition, "model": fixture["model"],
                   "baseline": baseline, "status": "complete", "failures": [],
                   "comparisons": {}, "case_status": {
                       name: {"status": "complete" if phase == "measurement" else "validated"}
                       for name in selected}}
            if phase == "measurement":
                run["comparisons"] = {name: {
                    "metal_ns": {"median": 1e6}, "python_ns": {"median": 2e6},
                    "python_over_metal_speedup": {"median": 2, "lower_95": 1.5, "upper_95": 2.5},
                } for name in selected}
            if cleanup_error:
                run.update(status="failed", cleanup_errors=["surviving owned descendant"])
            return run

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            native = root / "native"
            native.write_bytes(b"fake test executable identity")
            args = types.SimpleNamespace(
                model="all", baseline="both", cases=None, repetitions=3, warmup=1, pairs=2,
                native_bin=native, model_root=root, upstream=root, output=root / "output",
                timeout_ms=1000, startup_timeout=10, max_rss_mib=512,
            )
            with mock.patch.object(bench, "run_pair", side_effect=pair), \
                 mock.patch.object(bench, "load_contract", side_effect=load), \
                 mock.patch.object(bench, "source_snapshot", side_effect=[initial, final]), \
                 mock.patch.object(bench, "hardware_receipt", return_value={}), \
                 mock.patch.object(bench.oracle, "verify_config_fixtures"), \
                 mock.patch.object(bench.oracle, "verify_reference_fixtures"), \
                 mock.patch.object(bench.oracle, "verify_dependencies", return_value={}), \
                 mock.patch.object(bench.oracle, "verify_upstream_checkout", return_value={}), \
                 mock.patch("builtins.print"):
                code = bench.driver(args)
            report = json.loads((args.output / "report.json").read_text())
        return code, report, events

    def test_entire_preflight_matrix_precedes_fresh_rotating_measurement_repetitions(self):
        code, report, events = self.run_driver_fake()
        self.assertEqual(0, code)
        self.assertEqual(["preflight"] * 6, [event[0] for event in events[:6]])
        self.assertEqual(["measurement"] * 18, [event[0] for event in events[6:]])
        for repetition, order in enumerate((["small", "base", "multi"],
                                           ["base", "multi", "small"],
                                           ["multi", "small", "base"])):
            self.assertEqual([model for model in order for _ in range(2)],
                             [event[2] for event in events[6 + repetition * 6:12 + repetition * 6]])
        self.assertEqual(12, len(report["summary"]))
        self.assertTrue(all(row["status"] == "complete" for row in report["summary"]))

    def test_campaign_stops_spawning_after_cleanup_failure(self):
        code, report, events = self.run_driver_fake(cleanup_error=True)
        self.assertEqual(2, code)
        self.assertEqual(1, len(events))
        self.assertEqual("failed", report["status"])
        self.assertIn("cleanup", report["error"])

    def test_source_drift_blocks_campaign_qualification(self):
        code, report, _ = self.run_driver_fake(source_drift=True)
        self.assertEqual(2, code)
        self.assertEqual("failed", report["status"])
        self.assertFalse(report["parity_validated"])
        self.assertIn("source identity changed", report["error"])

    def test_repeatability_requires_all_intervals_and_all_repetitions(self):
        fixture = contract()
        name = fixture["cases"][0]
        report = {"contracts": [{"model": "small", "cases": [name]}], "baselines": ["mps"],
                  "repetitions": 3, "runs": []}
        for repetition in range(3):
            report["runs"].append({
                "phase": "measurement", "model": "small", "baseline": "mps", "repetition": repetition,
                "comparisons": {name: {
                    "metal_ns": {"median": 1e6}, "python_ns": {"median": 2e6},
                    "python_over_metal_speedup": {"median": 2, "lower_95": 1.5, "upper_95": 2.5},
                }},
            })
        row = bench.aggregate(report)[0]
        self.assertEqual("metal_faster", row["repeatability"])
        self.assertEqual(2, row["median_speedup"])
        report["runs"][1]["hardware_start"] = {"power_source": "battery", "low_power_mode": "0"}
        self.assertEqual("blocked", bench.aggregate(report)[0]["status"])
        report["runs"][1].pop("hardware_start")
        report["runs"][1]["comparisons"][name]["python_over_metal_speedup"]["lower_95"] = 0.9
        self.assertEqual("inconclusive", bench.aggregate(report)[0]["repeatability"])
        report["runs"].pop()
        self.assertEqual("blocked", bench.aggregate(report)[0]["status"])


if __name__ == "__main__":
    unittest.main()
