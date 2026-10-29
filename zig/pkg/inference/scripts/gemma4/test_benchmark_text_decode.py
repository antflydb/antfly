# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Prevent shared GPU activity from being reported as isolated performance."""
import unittest

from benchmark_text_decode import audit_gpu_activity


class GpuAuditTests(unittest.TestCase):
    samples = [{"started_at_unix": 1793295796.2, "finished_at_unix": 1793295798.8}]

    def test_own_supervisor_worker_and_graphics_do_not_count_as_competitors(self):
        log = """
#Date Time gpu pid type sm mem enc dec jpg ofa fb ccpm command
20261029 17:43:16 0 100 C 99 10 - - - - 5000 0 worker
20261029 17:43:17 0 101 C 90 10 - - - - 5000 0 server
20261029 17:43:17 0 200 G 15 10 - - - - 100 0 Xorg
"""
        report = audit_gpu_activity(log, {100, 101}, self.samples)
        self.assertTrue(report["quiet"])
        self.assertEqual(report["observed_seconds"], 2)

    def test_other_compute_activity_rejects_run(self):
        log = """
20261029 17:43:16 0 100 C 99 10 - - - - 5000 0 worker
20261029 17:43:17 0 200 C 3 0 - - - - 100 0 competitor
"""
        report = audit_gpu_activity(log, {100}, self.samples)
        self.assertFalse(report["quiet"])
        self.assertEqual(report["competing_activity"][0]["pid"], 200)

    def test_unknown_compute_activity_and_missing_telemetry_reject_run(self):
        log = """
20261029 17:43:16 0 100 C 99 10 - - - - 5000 0 worker
20261029 17:43:17 0 200 C - 0 - - - - 100 0 competitor
"""
        self.assertFalse(audit_gpu_activity(log, {100}, self.samples)["quiet"])
        self.assertFalse(audit_gpu_activity("Not supported", {100}, self.samples)["quiet"])

    def test_idle_service_is_allowed_and_activity_outside_window_is_excluded(self):
        log = """
20261029 17:43:10 0 200 C 99 10 - - - - 5000 0 competitor
20261029 17:43:16 0 200 C 0 0 - - - - 5000 0 competitor
20261029 17:43:17 0 100 C 99 10 - - - - 5000 0 worker
20261029 17:43:25 0 200 C 99 10 - - - - 5000 0 competitor
"""
        self.assertTrue(audit_gpu_activity(log, {100}, self.samples)["quiet"])


if __name__ == "__main__":
    unittest.main()
