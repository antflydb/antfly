# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fail-closed checks for Laya performance qualification artifacts."""

import copy
import math
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from benchmark_laya_metal import run_pair, run_window, validate_measurement


class MeasurementTests(unittest.TestCase):
    def test_paging_retries_both_sides_and_retains_rejected_pair(self):
        calls, rejected = [], []

        def measure(mode, attempt):
            calls.append((mode, attempt))
            paged = mode == "resident" and attempt == 0
            return {
                "mode": mode,
                "attempt": attempt,
                "passed": not paged,
                "measurement_valid": True,
                "paging_delta": {"Pageouts": int(paged), "Swapouts": 0},
            }

        accepted = run_pair(("legacy", "resident"), measure, 2, rejected)
        self.assertEqual(
            calls, [("legacy", 0), ("resident", 0), ("legacy", 1), ("resident", 1)]
        )
        self.assertEqual([r["attempt"] for r in accepted], [1, 1])
        self.assertEqual(len(rejected), 1)
        self.assertEqual(len(rejected[0]["runs"]), 2)

    def test_invalid_measurement_never_retries_and_paging_retries_are_bounded(self):
        for valid, expected_calls in ((False, 1), (True, 3)):
            calls, rejected = [], []

            def measure(mode, attempt, calls=calls, valid=valid):
                calls.append((mode, attempt))
                return {
                    "passed": False,
                    "measurement_valid": valid,
                    "paging_delta": {"Pageouts": 1, "Swapouts": 0},
                }

            with self.assertRaises(RuntimeError):
                run_pair(("resident", "legacy"), measure, 2, rejected)
            self.assertEqual(len(calls), expected_calls)
            self.assertEqual(len(rejected), expected_calls)

    def test_timeout_stops_wrapped_child(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "survived"
            child = (
                "import time,pathlib; time.sleep(0.6); "
                f"pathlib.Path({str(marker)!r}).touch()"
            )
            wrapper = (
                "import subprocess,sys; "
                f"subprocess.run([sys.executable, '-c', {child!r}])"
            )
            with (
                open(os.devnull, "w") as log,
                self.assertRaises(subprocess.TimeoutExpired),
            ):
                run_window([sys.executable, "-c", wrapper], os.environ, log, 0.3)
            time.sleep(0.6)
            self.assertFalse(marker.exists(), "timed-out child kept running")

    def setUp(self):
        self.payload = {
            "batch": 2,
            "mixed": True,
            "samples_ns": [100, 200],
            "resident": {
                "prepared": True,
                "activation_host_accesses": 0,
                "intermediate_readbacks": 0,
                "host_fallbacks": 0,
                "cached_activation_bytes": 0,
                "requests": 12,
                "physical_download_calls": 12,
                "physical_download_bytes": 480,
                "output_readback_bytes": 480,
                "physical_upload_bytes": 4096,
                "input_upload_bytes": 4096,
            },
        }

    def valid(self, payload=None, mode="resident"):
        return validate_measurement(
            self.payload if payload is None else payload, mode, 2, "mixed", 2
        )

    def test_valid_resident_and_legacy_routes(self):
        self.assertTrue(self.valid())
        self.assertFalse(self.valid(mode="legacy"))
        self.payload["resident"] = None
        self.assertTrue(self.valid(mode="legacy"))
        self.assertFalse(self.valid())

    def test_missing_or_malformed_timings_fail(self):
        for samples in (
            [],
            [100],
            [0, 100],
            [-1, 100],
            [True, 100],
            [math.nan, 100],
            [math.inf, 100],
            ["100", 200],
        ):
            with self.subTest(samples=samples):
                self.payload["samples_ns"] = samples
                self.assertFalse(self.valid())

    def test_shape_and_profile_must_match(self):
        self.payload["batch"] = 1
        self.assertFalse(self.valid())
        self.payload["batch"] = 2
        self.payload["mixed"] = False
        self.assertFalse(self.valid())

    def test_missing_counters_and_transfer_violations_fail(self):
        for key in self.payload["resident"]:
            with self.subTest(missing=key):
                changed = copy.deepcopy(self.payload)
                del changed["resident"][key]
                self.assertFalse(self.valid(changed))
        for key in (
            "activation_host_accesses",
            "intermediate_readbacks",
            "host_fallbacks",
            "cached_activation_bytes",
            "physical_download_calls",
            "physical_download_bytes",
            "physical_upload_bytes",
        ):
            with self.subTest(changed=key):
                changed = copy.deepcopy(self.payload)
                changed["resident"][key] += 1
                self.assertFalse(self.valid(changed))


if __name__ == "__main__":
    unittest.main()
