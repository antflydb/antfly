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


if __name__ == "__main__":
    unittest.main()
