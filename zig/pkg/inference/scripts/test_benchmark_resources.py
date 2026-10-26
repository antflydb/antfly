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

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import benchmark_resources as guard


class ResourceGuardTests(unittest.TestCase):
    def test_disk_guard_rejects_filesystem_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory).resolve()
            probe = guard.DiskSpaceGuard(output_dir)
            original_stat = guard.Path.stat

            def changed_stat(path, *args, **kwargs):
                observed = original_stat(path, *args, **kwargs)
                if path == output_dir:
                    return type("Stat", (), {"st_dev": observed.st_dev + 1})()
                return observed

            with patch.object(guard.Path, "stat", autospec=True, side_effect=changed_stat):
                with self.assertRaisesRegex(RuntimeError, "filesystem changed"):
                    probe.sample_mib()

    def test_disk_guard_keeps_original_work_and_temp_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            probe = guard.DiskSpaceGuard(Path(directory))
            with (
                patch.object(guard.Path, "cwd", side_effect=AssertionError("cwd recomputed")),
                patch.object(guard.tempfile, "gettempdir", side_effect=AssertionError("temp recomputed")),
            ):
                self.assertGreaterEqual(probe.sample_mib(), 0)

    def test_low_work_or_temp_disk_rejects_abundant_artifact_volume(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            output_dir, work_dir, temp_dir = (root / name for name in ("output", "work", "temp"))
            for path in (output_dir, work_dir, temp_dir):
                path.mkdir()
            for constrained in (output_dir, work_dir, temp_dir):
                with self.subTest(constrained=constrained):
                    sampled = []

                    def disk_usage(path):
                        sampled.append(path)
                        free = 4095 if path == constrained else 50 * 1024
                        return type("Disk", (), {"free": free * 1048576})()

                    with (
                        patch.object(guard.Path, "cwd", return_value=work_dir),
                        patch.object(guard.tempfile, "gettempdir", return_value=str(temp_dir)),
                        patch.object(guard.shutil, "disk_usage", side_effect=disk_usage),
                        patch.object(guard, "memory_free_percent", return_value=80),
                        patch.object(guard, "swapout_bytes", return_value=0),
                        patch.object(guard.subprocess, "Popen") as launch,
                    ):
                        result = guard.run_guarded(["unused"], output_dir / "report.json",
                                                   min_disk_free_mib=4096)
                    launch.assert_not_called()
                    self.assertFalse(result["pass"])
                    self.assertIn("free disk", result["violation"])
                    self.assertEqual(result["min_disk_free_mib"], 4095)
                    self.assertEqual(set(sampled), {output_dir, work_dir, temp_dir})

    def test_unreadable_work_volume_fails_closed_before_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory).resolve() / "report.json"
            work_dir = Path.cwd().resolve()

            def disk_usage(path):
                if path == work_dir:
                    raise OSError("work volume unavailable")
                return type("Disk", (), {"free": 50 * 1024 * 1048576})()

            with (
                patch.object(guard.shutil, "disk_usage", side_effect=disk_usage),
                patch.object(guard, "memory_free_percent", return_value=80),
                patch.object(guard, "swapout_bytes", return_value=0),
                patch.object(guard.subprocess, "Popen") as launch,
            ):
                result = guard.run_guarded(["unused"], output, min_disk_free_mib=4096)
            launch.assert_not_called()
            self.assertFalse(result["pass"])
            self.assertIn("work volume unavailable", result["violation"])

    def test_group_rss_counts_workers_without_the_leader(self):
        self.assertEqual(guard.group_rss_mib(7, "7 1024\n9 8192\n7 2048\n"), 3)

    def test_preflight_failure_does_not_launch(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(guard, "memory_free_percent", return_value=2),
            patch.object(guard, "swapout_bytes", return_value=0),
            patch.object(guard.subprocess, "Popen") as launch,
        ):
            output = Path(directory) / "report.json"
            result = guard.run_guarded(["unused"], output, min_disk_free_mib=0)
            launch.assert_not_called()
            self.assertFalse(result["pass"])
            self.assertIn("free memory", result["violation"])
            self.assertEqual(json.loads(output.read_text()), result)

    def test_timeout_terminates_owned_command(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(guard, "memory_free_percent", return_value=80),
            patch.object(guard, "swapout_bytes", return_value=0),
            patch.object(guard, "group_rss_mib", return_value=1),
            patch.object(guard, "group_live_processes", return_value=1),
            patch.object(guard, "wait_group_drained", return_value=(0, 0)),
        ):
            result = guard.run_guarded(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                Path(directory) / "report.json",
                timeout=0.1,
                interval=0.05,
                min_disk_free_mib=0,
            )
            self.assertFalse(result["pass"])
            self.assertIn("timeout", result["violation"])
            self.assertLess(result["elapsed_seconds"], 5)
            self.assertLess(result["returncode"], 0)


if __name__ == "__main__":
    unittest.main()
