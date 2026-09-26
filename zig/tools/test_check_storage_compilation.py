# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Test measurement accounting without invoking a compiler."""

import importlib.util
import unittest
import sys
import tempfile
from unittest import mock
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "check_storage_compilation",
    Path(__file__).with_name("check_storage_compilation.py"),
)
assert SPEC and SPEC.loader
measurement = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(measurement)


class BuildMemoryAccounting(unittest.TestCase):
    def test_concurrent_descendants_exclude_unrelated_builds(self):
        snapshot = """
        100 1 20
        101 100 100
        102 100 200
        103 102 50
        200 1 9000
        201 200 8000
        """
        self.assertEqual(measurement.tree_rss(snapshot, 100), (370 * 1024, 200 * 1024))

    def test_finished_and_missing_processes(self):
        self.assertEqual(measurement.tree_rss("200 1 9000", 100), (0, 0))
        self.assertEqual(measurement.tree_rss("100 1 20", 100), (20 * 1024, 20 * 1024))


class BuildFailureEvidence(unittest.TestCase):
    def test_timeout_preserves_output_and_measurements(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.object(measurement.subprocess, "check_output", return_value=""),
        ):
            code, output, measured = measurement.measured_build(
                [
                    sys.executable,
                    "-u",
                    "-c",
                    "import time; print('compiler diagnostic'); time.sleep(60)",
                ],
                Path(directory),
                timeout_seconds=0.5,
            )
        self.assertNotEqual(code, 0)
        self.assertIn("compiler diagnostic", output)
        self.assertTrue(measured["timed_out"])
        self.assertFalse(measured["cpu_accounting_complete"])
        self.assertGreater(measured["wall_seconds"], 0)

    def test_report_replaces_previous_running_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            measurement.write_report(report, [{"status": "running"}])
            measurement.write_report(report, [{"status": "failed", "returncode": -9}])
            self.assertIn('"returncode": -9', report.read_text())
            self.assertFalse(report.with_suffix(".json.tmp").exists())

    def test_physical_build_uses_bounded_runner(self):
        arguments = [
            "build",
            "check-storage-compilation",
            "--cache-dir",
            "private-cache",
        ]
        command = measurement.bounded_build_command("pinned-zig", arguments)
        self.assertEqual(command[0], sys.executable)
        self.assertEqual(Path(command[1]).name, "run_bounded_zig_build.py")
        self.assertEqual(command[2:], ["--zig", "pinned-zig", "--", *arguments])


if __name__ == "__main__":
    unittest.main()
