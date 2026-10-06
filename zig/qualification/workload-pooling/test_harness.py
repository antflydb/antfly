"""Qualification gates must fail closed when measurements or identities differ."""

import copy
import unittest
from compare_receipts import compare
from limits import check_container
from load_clients import percentile


class QualificationTests(unittest.TestCase):
    def receipt(self):
        return dict(
            passed=True,
            server_exit_code=0,
            binary_sha256="candidate",
            manifest_sha256="manifest",
            configuration={"admission": {}, "pgwire": {}},
            process_limits={"max_fds": 512},
            enforced_limits={"platform": "Linux", "memory_bytes": 2147483648},
            manifest={
                "comparison_thresholds": {
                    "low_load_p95_ratio_max": 1.25,
                    "throughput_ratio_min": 0.9,
                },
                "phases": [{"name": "low-load"}, {"name": "sustained-overload"}],
            },
            phases=[
                dict(
                    name=name,
                    offered=100,
                    retries=0,
                    success_p95_seconds=0.1,
                    successful_per_second=20,
                )
                for name in ("low-load", "sustained-overload")
            ],
        )

    def test_equal_successful_receipts_do_not_qualify_automatic_defaults(self):
        result = compare(self.receipt(), self.receipt())
        self.assertTrue(result["passed"])
        self.assertFalse(result["automatic_defaults_qualified"])

    def test_resource_and_manifest_identity_mismatch_fail(self):
        baseline = self.receipt()
        for field in ("manifest_sha256", "process_limits", "enforced_limits"):
            candidate = self.receipt()
            candidate[field] = "different"
            self.assertFalse(compare(baseline, candidate)["passed"])

    def test_failed_shutdown_missing_phase_and_retries_fail(self):
        for change in ("shutdown", "phase", "retries", "qualification", "zero-success"):
            candidate = self.receipt()
            if change == "shutdown":
                candidate["server_exit_code"] = 1
            elif change == "phase":
                candidate["phases"].pop()
            elif change == "retries":
                candidate["phases"][0]["retries"] = 1
            elif change == "qualification":
                candidate["passed"] = False
            else:
                candidate["phases"][0]["success_p95_seconds"] = None
            self.assertFalse(compare(self.receipt(), candidate)["passed"])

    def test_latency_and_throughput_regressions_fail(self):
        candidate = self.receipt()
        candidate["phases"][0]["success_p95_seconds"] = 0.126
        self.assertFalse(compare(self.receipt(), candidate)["passed"])
        candidate = self.receipt()
        candidate["phases"][1]["successful_per_second"] = 17.9
        self.assertFalse(compare(self.receipt(), candidate)["passed"])

    def test_linux_manifest_requires_actual_equal_container_limits(self):
        manifest = {
            "container_limits": {
                "cpu_count": 2,
                "memory_bytes": 2147483648,
                "pids": 512,
            }
        }
        measured = dict(platform="Linux", **manifest["container_limits"])
        self.assertFalse(check_container(manifest, measured))
        for field in ("cpu_count", "memory_bytes", "pids"):
            unavailable = copy.deepcopy(measured)
            unavailable[field] = None
            self.assertTrue(check_container(manifest, unavailable))
        self.assertTrue(check_container(manifest, dict(measured, platform="Darwin")))

    def test_same_binary_comparison_requires_equal_nonwaiting_configuration(self):
        baseline, candidate = self.receipt(), self.receipt()
        candidate["configuration"]["admission"]["max_request_memory_bytes"] = 64
        self.assertFalse(compare(baseline, candidate)["passed"])
        baseline["configuration"]["admission"]["max_request_memory_bytes"] = 64
        candidate["configuration"]["admission"]["query"] = {
            "waiting": {"max_wait_ms": 250}
        }
        baseline["configuration"]["admission"]["query"] = {
            "waiting": {"max_wait_ms": 0}
        }
        self.assertTrue(compare(baseline, candidate)["passed"])

    def test_empty_latency_sample_has_no_successful_percentile(self):
        self.assertIsNone(percentile([], 0.95))
        self.assertEqual(percentile([0.3, 0.1, 0.2], 0.5), 0.2)


if __name__ == "__main__":
    unittest.main()
