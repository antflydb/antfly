# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import unittest
import performance_gate as gate


def receipts():
    def record(backend):
        return dict(
            backend=backend,
            status="pass",
            precision="float32",
            suite_sha256="suite",
            checkpoint_receipt_sha256="weights",
            binary_sha256="a" * 64,
            cases={
                "image": dict(
                    vector=[1.0, 0.0],
                    vm_measured_before=[0, 0, 0],
                    vm_measured_after=[0, 0, 0],
                    timing=dict(samples_seconds=[1.0, 1.0]),
                    preprocessing_and_inference_timing=dict(samples_seconds=[1.0, 1.0]),
                )
            },
        )

    return record("native"), record("cpu")


class PerformanceGate(unittest.TestCase):
    def test_case_regressions_and_insufficient_evidence_cannot_hide_in_average(self):
        a, b = receipts()
        self.assertEqual(gate.evaluate(a, b, minimum_samples=2)["status"], "pass")
        a["cases"]["audio"] = copy.deepcopy(a["cases"]["image"])
        b["cases"]["audio"] = copy.deepcopy(b["cases"]["image"])
        a["cases"]["image"]["timing"]["samples_seconds"] = [0.01, 0.01]
        a["cases"]["audio"]["timing"]["samples_seconds"] = [2.0, 2.0]
        result = gate.evaluate(a, b, minimum_samples=2)
        self.assertEqual(result["status"], "fail")
        self.assertEqual([r["status"] for r in result["cases"]], ["pass", "fail"])
        self.assertFalse(gate.evaluate(*receipts())["cases"][0]["sufficient_samples"])

    def test_reject_unmatched_precision_weights_binary_scope_or_coverage(self):
        for field, value in [
            ("precision", "bf16"),
            ("suite_sha256", "other"),
            ("checkpoint_receipt_sha256", "other"),
            ("binary_sha256", ""),
            ("status", "running"),
            ("backend", "metal"),
            ("measurement_mode", "encoder"),
        ]:
            a, b = receipts()
            a[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                gate.evaluate(a, b)
        a, b = receipts()
        a["cases"] = {}
        with self.assertRaises(ValueError):
            gate.evaluate(a, b)

    def test_parity_and_request_boundary_use_actual_samples(self):
        a, b = receipts()
        a["cases"]["image"]["timing"]["p50_seconds"] = 0.001
        a["cases"]["image"]["timing"]["samples_seconds"] = [2.0, 2.0]
        self.assertEqual(
            gate.evaluate(a, b, minimum_samples=2)["cases"][0]["median_ratio"], 2.0
        )
        a["cases"]["image"]["timing"]["samples_seconds"] = [0.1, 0.1]
        a["cases"]["image"]["vector"] = [0.0, 1.0]
        self.assertEqual(gate.evaluate(a, b, minimum_samples=2)["status"], "fail")

    def test_paging_and_missing_memory_evidence_fail_even_when_fast(self):
        a, b = receipts()
        a["cases"]["image"]["vm_measured_after"] = [0, 1, 0]
        result = gate.evaluate(a, b, minimum_samples=2)
        self.assertEqual(result["status"], "fail")
        self.assertFalse(result["cases"][0]["quiet_memory"])
        a, b = receipts()
        del a["cases"]["image"]["vm_measured_before"]
        del a["cases"]["image"]["vm_measured_after"]
        self.assertEqual(gate.evaluate(a, b, minimum_samples=2)["status"], "fail")
        for report in (a, b):
            report["cases"]["image"].pop("vm_measured_before", None)
            report["cases"]["image"].pop("vm_measured_after", None)
            report["before"] = {"vm_stat": "Pageouts: 5.\nSwapins: 6.\nSwapouts: 7.\n"}
            report["after"] = {"vm_stat": "Pageouts: 5.\nSwapins: 6.\nSwapouts: 7.\n"}
        self.assertEqual(gate.evaluate(a, b, minimum_samples=2)["status"], "pass")

    def test_container_receipt_paging_uses_conditions_snapshots(self):
        for backend in ("darwin", "linux"):
            a, b = receipts()
            for r in (a, b):
                case = r["cases"]["image"]
                del case["vm_measured_before"], case["vm_measured_after"]
                if backend == "darwin":
                    state = {
                        "vm_stat": {
                            "output": "Pageouts: 5.\nSwapins: 6.\nSwapouts: 7.\n"
                        }
                    }
                else:
                    state = {"vmstat": "pswpin 5\npswpout 6\n"}
                r["conditions_before"] = copy.deepcopy(state)
                r["conditions_after"] = copy.deepcopy(state)
            self.assertEqual(gate.evaluate(a, b, minimum_samples=2)["status"], "pass")
            del a["conditions_after"]
            self.assertEqual(gate.evaluate(a, b, minimum_samples=2)["status"], "fail")

    def test_video_and_prepared_encoder_receipts(self):
        a, b = receipts()
        for r in (a, b):
            r["corpus_sha256"] = r.pop("suite_sha256")
            case = r["cases"]["image"]
            case["completed_timing"] = case.pop("timing")
        self.assertEqual(gate.evaluate(a, b, minimum_samples=2)["status"], "pass")
        a, b = receipts()
        for r in (a, b):
            r["measurement_mode"] = "encoder"
        self.assertEqual(
            gate.evaluate(a, b, boundary="encoder", minimum_samples=2)["status"], "pass"
        )


def regression_receipts(backend="native", precision=None):
    precision = precision or ("bfloat16" if backend == "cuda" else "float32")
    a, b = receipts()
    for r in (a, b):
        r.update(backend=backend, precision=precision, pooling_precision="float32",
                 suite_sha256="b" * 64, checkpoint_receipt_sha256="c" * 64,
                 tool_sha256="d" * 64, reference_tool_sha256="e" * 64,
                 measurement_mode="default", timing_scope="completed HTTP request",
                 implementation_controls={}, model_identity="same-model-and-recipe",
                 case_selection=["image"], max_vector_error=1e-4,
                 decision={"choice": "yes", "similarities": {"yes": 1.0, "no": 0.0}, "margin": 1.0})
        linux = backend == "cuda"
        r["vm_counter_order"] = ["pswpin", "pswpout"] if linux else ["pageouts", "swapins", "swapouts"]
        device = {"status": "observed", "kind": {"native": "cpu", "metal": "metal", "cuda": "cuda"}[backend]}
        if backend == "metal":
            device["device"] = {"sppci_model": "fixture GPU"}
        if backend == "cuda":
            device.update(uuid="GPU-fixture", name="fixture GPU", driver_version="fixture driver", pci_bus_id="0:0:0", memory_mib=100, memory_capacity_status="observed", query_output="GPU-fixture, fixture GPU, fixture driver, 0:0:0, 100\n")
        snapshot = {"host_identity": {"platform": "linux" if linux else "darwin", "node": "fixture-host", "machine": "arm64", "release": "fixture-release", "cpu_count": 4},
                    "hardware": "fixture CPU", "device_identity": device,
                    "thread_budget": {"environment": {key: "1" if key == "OMP_NUM_THREADS" else None for key in gate.compare.THREAD_ENVIRONMENT_KEYS}, "cpu_count": 4, "affinity": [0, 1], "cgroup_cpu_max": "max 100000"}}
        if linux:
            snapshot["host_identity"].update(boot_id="fixture-boot", memory_bytes=1000000)
            snapshot["thread_budget"]["cgroup_cpu_max"] = [{"path": "/sys/fs/cgroup/fixture/cpu.max", "value": "max 100000"}]
        r["before"], r["after"] = copy.deepcopy(snapshot), copy.deepcopy(snapshot)
        r["cases"]["image"]["vm_measured_before"] = [0] * len(r["vm_counter_order"])
        r["cases"]["image"]["vm_measured_after"] = [0] * len(r["vm_counter_order"])
        r["cases"]["image"]["tokens"] = 5
        r["cases"]["image"]["timing"]["samples_seconds"] = [1.0] * 20
        if precision == "bfloat16":
            r["reference_result_sha256"] = "f" * 64
            r["parity"] = {"vectors": {"image": {"cosine": 1.0, "max_abs": 0.0, "actual_norm": 1.0}}}
    return a, b


def regression_gate(a, b, *, quality=True, **kwargs):
    if a.get("precision") == "bfloat16" and quality:
        oracle = copy.deepcopy(b)
        oracle["backend"] = "torch-cuda"
        kwargs.update(quality_reference=oracle, quality_reference_sha256="f" * 64)
    return gate.evaluate_no_regression(a, b, **kwargs)


class NoRegressionGate(unittest.TestCase):
    def test_same_backend_raw_distribution_gates_and_no_implied_pairing(self):
        for backend in ("native", "metal", "cuda"):
            a, b = regression_receipts(backend)
            a["binary_sha256"] = "1" * 64
            result = regression_gate(a, b)
            self.assertEqual(result["status"], "pass")
            self.assertFalse(result["production_qualified"])
            self.assertIn("no recorded pairing", result["sample_relationship"])
            a["cases"]["image"]["timing"]["samples_seconds"] = [1.06] * 20
            self.assertEqual(regression_gate(a, b)["status"], "fail")
        a, b = regression_receipts()
        a["cases"]["image"]["timing"].update(
            samples_seconds=[1.0] * 18 + [1.11, 1.11], p95_seconds=0.1)
        result = regression_gate(a, b)
        self.assertEqual(result["cases"][0]["median_ratio"], 1.0)
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["cases"][0]["p95_ratio"], 1.11)

    def test_rejects_missing_changed_and_mixed_identities(self):
        changes = dict(backend="metal", precision="bfloat16", pooling_precision="bfloat16",
                       suite_sha256="0" * 64, checkpoint_receipt_sha256="0" * 64,
                       tool_sha256="0" * 64, binary_sha256="bad", measurement_mode="encoder",
                       timing_scope="enqueue only", model_identity="different model",
                       implementation_controls={"fast_math": "1"}, case_selection=["other"], status="failed")
        for field, value in changes.items():
            a, b = regression_receipts()
            a[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                regression_gate(a, b)
        for field in ("suite_sha256", "checkpoint_receipt_sha256", "tool_sha256",
                      "binary_sha256", "pooling_precision", "measurement_mode", "timing_scope", "model_identity"):
            a, b = regression_receipts()
            del a[field]
            with self.subTest(missing=field), self.assertRaises(ValueError):
                regression_gate(a, b)
        a, b = regression_receipts()
        a["corpus_sha256"] = b["corpus_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "distinct"):
            regression_gate(a, b)

    def test_finite_samples_sufficiency_and_both_vector_norms(self):
        for sample in (float("nan"), float("inf"), -1.0, 0.0, True, "1"):
            a, b = regression_receipts()
            a["cases"]["image"]["timing"]["samples_seconds"][0] = sample
            with self.subTest(sample=sample), self.assertRaises(ValueError):
                regression_gate(a, b)
        a, b = regression_receipts()
        b["cases"]["image"]["timing"]["samples_seconds"] = [1.0] * 9
        self.assertEqual(regression_gate(a, b)["status"], "fail")
        a, b = regression_receipts()
        b["cases"]["image"]["vector"] = [1.001, 0.0]
        self.assertEqual(regression_gate(a, b)["status"], "fail")
        a, b = regression_receipts()
        a["cases"]["image"]["vector"] = [float("nan"), 0.0]
        with self.assertRaises(ValueError):
            regression_gate(a, b)

    def test_quality_includes_decisions_retrieval_and_memory(self):
        a, b = regression_receipts()
        a["decision"]["choice"] = "no"
        self.assertEqual(regression_gate(a, b)["status"], "fail")
        a, b = regression_receipts()
        a["retrieval"] = {"rankings": {"query": ["b", "a"]}, "scores": {"query": {"a": 0.0, "b": 1.0}}}
        b["retrieval"] = {"rankings": {"query": ["a", "b"]}, "scores": {"query": {"a": 1.0, "b": 0.0}}}
        self.assertEqual(regression_gate(a, b)["status"], "fail")
        a, b = regression_receipts()
        a["cases"]["image"]["vm_measured_after"] = [0, 1, 0]
        self.assertEqual(regression_gate(a, b)["status"], "fail")

    def test_bf16_needs_existing_independent_quality_contract(self):
        for backend in ("cuda",):
            a, b = regression_receipts(backend, "bfloat16")
            self.assertEqual(regression_gate(a, b)["status"], "pass")
            with self.assertRaisesRegex(ValueError, "blocked"):
                regression_gate(a, b, quality=False)
            oracle = copy.deepcopy(b)
            with self.assertRaisesRegex(ValueError, "blocked"):
                gate.evaluate_no_regression(a, b, quality_reference=oracle, quality_reference_sha256="f" * 64)
            for field, value in (("reference_result_sha256", "0" * 64),
                                 ("max_vector_error", 1e-3), ("parity", {})):
                bad = copy.deepcopy(a)
                bad[field] = value
                with self.subTest(backend=backend, field=field), self.assertRaisesRegex(ValueError, "blocked"):
                    regression_gate(bad, b)
            a["parity"]["vectors"]["image"]["max_abs"] = float("nan")
            with self.assertRaisesRegex(ValueError, "blocked"):
                regression_gate(a, b)

    def test_unsupported_backend_precision_cannot_claim_current_producer_contract(self):
        for backend, precision in (("native", "bfloat16"), ("metal", "bfloat16"), ("cuda", "float32")):
            a, b = regression_receipts(backend, precision)
            with self.subTest(backend=backend, precision=precision), self.assertRaises(ValueError):
                regression_gate(a, b)

    def test_missing_changed_host_device_threads_and_counter_order_block(self):
        for when in ("before", "after"):
            for field in ("host_identity", "hardware", "thread_budget", "device_identity"):
                a, b = regression_receipts()
                a[when].pop(field)
                with self.subTest(when=when, missing=field), self.assertRaises(ValueError):
                    regression_gate(a, b)
        for field in ("host_identity", "hardware", "thread_budget", "device_identity"):
            a, b = regression_receipts()
            b["after"][field] = {"different": "host, device or budget"}
            with self.subTest(changed=field), self.assertRaises(ValueError):
                regression_gate(a, b)
        a, b = regression_receipts()
        for r in (a, b):
            for when in ("before", "after"):
                r[when]["thread_budget"]["environment"] = {}
        with self.assertRaises(ValueError):
            regression_gate(a, b)
        a, b = regression_receipts("cuda")
        a["after"]["device_identity"]["status"] = "unavailable"
        with self.assertRaises(ValueError):
            regression_gate(a, b)
        a, b = regression_receipts("cuda")
        a["vm_counter_order"] = ["pageouts", "swapins", "swapouts"]
        with self.assertRaises(ValueError):
            regression_gate(a, b)
        a, b = regression_receipts("cuda")
        a["cases"]["image"]["vm_measured_after"] = [0, 0, 0]
        with self.assertRaises(ValueError):
            regression_gate(a, b)

    def test_linux_missing_boot_memory_and_quota_observations_block(self):
        for field, value in (("boot_id", None), ("boot_id", ""), ("memory_bytes", None), ("memory_bytes", 0), ("memory_bytes", True)):
            a, b = regression_receipts("cuda")
            for r in (a, b):
                for when in ("before", "after"):
                    r[when]["host_identity"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                regression_gate(a, b)
        for value in ("max 100000", [], [{"path": "/tmp/cpu.max", "value": "max 100000"}],
                      [{"path": "/sys/fs/cgroup/cpu.max", "value": "max"}],
                      [{"path": "/sys/fs/cgroup/cpu.max", "value": "max 0"}],
                      [{"path": "/sys/fs/cgroup/cpu.max", "value": "oops 100000"}],
                      [{"path": "/sys/fs/cgroup/cpu.max", "value": "0 100000"}]):
            a, b = regression_receipts("cuda")
            for r in (a, b):
                for when in ("before", "after"):
                    r[when]["thread_budget"]["cgroup_cpu_max"] = copy.deepcopy(value)
            with self.subTest(quota=value), self.assertRaises(ValueError):
                regression_gate(a, b)

    def test_gb10_unavailable_capacity_is_not_unknown_device_or_peak_memory(self):
        a, b = regression_receipts("cuda")
        for r in (a, b):
            for when in ("before", "after"):
                r[when]["device_identity"].update(memory_mib=None, memory_capacity_status="unavailable", query_output="GPU-fixture, GB10, driver, pci, [N/A]\n")
        result = regression_gate(a, b)
        self.assertEqual(result["status"], "pass")
        self.assertFalse(result["physical_memory_qualified"])
        for field in ("uuid", "driver_version", "pci_bus_id"):
            bad = copy.deepcopy(a)
            bad["before"]["device_identity"].pop(field)
            with self.subTest(field=field), self.assertRaises(ValueError):
                regression_gate(bad, b)
        a["before"]["device_identity"]["memory_mib"] = 0
        with self.assertRaises(ValueError):
            regression_gate(a, b)

    def test_encoder_explicit_boundary_and_current_tokens(self):
        a, b = regression_receipts()
        for r in (a, b):
            r.pop("model_identity")
            r.update(measurement_mode="encoder", timing_scope="prepared IDs/mask",
                     decision={"status": "not_run"})
        self.assertEqual(regression_gate(a, b, boundary="encoder")["status"], "pass")
        a["cases"]["image"]["tokens"] = 6
        with self.assertRaises(ValueError):
            regression_gate(a, b, boundary="encoder")


if __name__ == "__main__":
    unittest.main()
