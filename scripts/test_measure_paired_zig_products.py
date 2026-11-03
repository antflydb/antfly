#!/usr/bin/env python3
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

import tempfile
import shutil
import subprocess
import sys
import threading
import unittest
from pathlib import Path

from measure_paired_zig_products import (
    ROOT,
    CacheLease,
    run_group,
    clear_outputs,
    definitions,
    metrics,
    order,
    valid_memory_capture,
)


class ProductMeasurements(unittest.TestCase):
    def test_cold_memory_requires_dominant_compiler_and_current_samples(self):
        memory = {
            "aggregate_is_same_poll": True,
            "units": {"antfly": {"peak_rss_bytes": 500}},
        }
        self.assertFalse(valid_memory_capture(memory, "cold"))
        memory["units"]["storage_engine"] = {"peak_rss_bytes": 1000}
        self.assertTrue(valid_memory_capture(memory, "cold"))
        memory["aggregate_is_same_poll"] = False
        self.assertFalse(valid_memory_capture(memory, "cold"))
        self.assertTrue(
            valid_memory_capture({"aggregate_is_same_poll": True, "units": {}}, "warm")
        )

    def test_alternates_complete_pairs(self):
        self.assertEqual(
            order(3),
            ["baseline", "candidate", "candidate", "baseline", "baseline", "candidate"],
        )

    def test_failed_baseline_never_becomes_a_speedup(self):
        runs = [
            {"pair": 0, "side": side, "phase": "cold", "exit_code": code}
            for side, code in (("baseline", 1), ("candidate", 0))
        ]
        self.assertEqual(metrics(runs), [{"pair": 0, "comparable": False}])

    def test_success_measures_both_products(self):
        runs = [
            {
                "pair": 0,
                "side": side,
                "phase": "cold",
                "exit_code": 0,
                "capture_valid": True,
                "elapsed_s": seconds,
                "products": {"antfly": {"bytes": a}, "libantfly": {"bytes": b}},
                "compiler_memory": {
                    "sampled_aggregate_peak_bytes": seconds * 100,
                    "aggregate_is_same_poll": True,
                    "units": {"engine": {"sampled_peak_rss_bytes": seconds * 50}},
                },
            }
            for side, seconds, a, b in (
                ("baseline", 10, 100, 50),
                ("candidate", 8, 90, 52),
            )
        ]
        self.assertEqual(
            metrics(runs)[0]["product_byte_deltas"], {"antfly": -10, "libantfly": 2}
        )
        self.assertEqual(metrics(runs)[0]["elapsed_ratio"], 0.8)
        self.assertEqual(metrics(runs)[0]["sampled_aggregate_peak_ratio"], 0.8)
        self.assertEqual(
            metrics(runs)[0]["compiler_peak_byte_deltas"], {"engine": -100}
        )
        runs[1]["compiler_memory"]["aggregate_is_same_poll"] = False
        self.assertIsNone(metrics(runs)[0]["sampled_aggregate_peak_ratio"])

    def test_missing_memory_capture_never_becomes_a_speedup(self):
        runs = [
            {
                "pair": 0,
                "side": side,
                "phase": "cold",
                "exit_code": 0,
                "capture_valid": valid,
            }
            for side, valid in (("baseline", False), ("candidate", True))
        ]
        self.assertEqual(metrics(runs), [{"pair": 0, "comparable": False}])

    def test_extra_options_cannot_change_comparison_profile(self):
        for value in (
            "--cache-dir=/tmp/cache",
            "-Dstrip=false",
            "-Doptimize=small",
            "-Dtarget=other",
            "-Dmetal=true",
            "-Dcuda=true",
            "-Dsystem-blas=true",
        ):
            with self.assertRaises(ValueError):
                definitions([value])
        self.assertEqual(
            definitions(["-Dshared-image-operations=true"]),
            ["-Dshared-image-operations=true"],
        )

    def test_evicted_cache_is_not_silently_recreated(self):
        fixtures = ROOT / "zig/.zig-cache/tool-fixtures"
        fixtures.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=fixtures) as temp:
            tree = Path(temp)
            lease = CacheLease(tree)
            shutil.rmtree(tree / "zig/.zig-cache/local")
            with self.assertRaisesRegex(RuntimeError, "cache was removed"):
                lease.pulse()
            lease.close()
            self.assertFalse((tree / "zig/.zig-cache/local").exists())

    def test_running_build_aborts_if_cache_is_evicted(self):
        fixtures = ROOT / "zig/.zig-cache/tool-fixtures"
        fixtures.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=fixtures) as temp:
            tree = Path(temp)

            def evict():
                shutil.rmtree(tree / "zig/.zig-cache/local")

            timer = threading.Timer(0.2, evict)
            timer.start()
            try:
                with self.assertRaisesRegex(RuntimeError, "cache was removed"):
                    run_group(
                        [sys.executable, "-c", "import time; time.sleep(30)"],
                        cwd=tree,
                        env=None,
                        output=subprocess.DEVNULL,
                        timeout=5,
                        cache_tree=tree,
                    )
            finally:
                timer.cancel()
                timer.join()
            self.assertFalse((tree / "zig/.zig-cache/local").exists())

    def test_cold_reset_preserves_downloads_not_artifacts(self):
        fixtures = ROOT / "zig/.zig-cache/tool-fixtures"
        fixtures.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=fixtures) as temp:
            tree = Path(temp)
            for name in (
                "zig/.zig-cache/local/o/x",
                "zig/.zig-cache/global/o/x",
                "zig/.zig-cache/global/p/package/x",
                "zig/zig-out/bin/antfly",
            ):
                path = tree / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fixture")
            clear_outputs(tree)
            self.assertTrue((tree / "zig/.zig-cache/global/p/package/x").is_file())
            self.assertFalse((tree / "zig/.zig-cache/global/o").exists())
            self.assertFalse((tree / "zig/.zig-cache/local").exists())
            self.assertFalse((tree / "zig/zig-out").exists())


if __name__ == "__main__":
    unittest.main()
