# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Test measurement accounting without invoking a compiler."""

import importlib.util
import json
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
    def test_contract_rollover_discards_both_caches_and_keeps_mutations(self):
        expected = (
            ("cold", set()),
            ("warm", set()),
            ("read coordination", {"antfly-runtime-distributed"}),
            ("write coordination", {"antfly-runtime-distributed"}),
            ("physical DB", {"antfly-storage-kernel"}),
            ("physical local query", {"antfly-storage-kernel"}),
            ("owner integration test", {"storage-owner-tests"}),
            ("consumer test root", {"api-table-read-tests"}),
            ("storage contract cold", set()),
            ("storage contract warm", set()),
            (
                "storage contract",
                {
                    "antfly-storage-kernel",
                    "antfly-runtime-distributed",
                    "storage-owner-tests",
                },
            ),
        )
        seen = []
        first_cache = None
        first_global = None

        def fake_build(command, cwd, *, progress):
            nonlocal first_cache, first_global
            label, rebuilt = expected[len(seen)]
            local_cache = Path(command[command.index("--cache-dir") + 1])
            global_cache = Path(command[command.index("--global-cache-dir") + 1])
            self.assertEqual(local_cache.parent, cwd.parent.parent)
            self.assertEqual(global_cache.parent, cwd.parent.parent)
            if first_cache is None:
                first_cache, first_global = local_cache, global_cache
            else:
                self.assertEqual(
                    (local_cache, global_cache), (first_cache, first_global)
                )
            if label == "storage contract cold":
                self.assertFalse(local_cache.exists())
                self.assertTrue(global_cache.is_dir())
                self.assertFalse((global_cache / "marker").exists())
                self.assertIn(
                    b"storage compilation ownership regression",
                    (cwd / "pkg/antfly/src/api/table_reads.zig").read_bytes(),
                )
            local_cache.mkdir(exist_ok=True)
            (local_cache / "marker").touch()
            (global_cache / "marker").touch()
            names = (
                measurement.ARCHIVES
                | measurement.CONSUMERS
                | {
                    "storage-owner-tests",
                    "storage-owner-source-tests",
                    "storage-owner-enrichment-tests",
                }
            )
            output = []
            for name in sorted(names):
                kind = "test_obj" if name in measurement.CONSUMERS else "lib"
                status = (
                    "success"
                    if label in {"cold", "storage contract cold"} or name in rebuilt
                    else "cached"
                )
                output.append(f"compile {kind} {name} Debug native {status}")
            if label in {"physical DB", "physical local query"}:
                output.extend(
                    f"compile exe {name} Debug native success"
                    for name in measurement.CONSUMERS
                )
            seen.append(label)
            return 0, "\n".join(output), {"wall_seconds": 0.0, "timed_out": False}

        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            with (
                mock.patch.object(
                    sys,
                    "argv",
                    ["check_storage_compilation.py", "--report", str(report)],
                ),
                mock.patch.object(
                    measurement, "measured_build", side_effect=fake_build
                ),
            ):
                measurement.main()
            self.assertEqual(seen, [name for name, _ in expected])
            self.assertFalse(first_cache.exists())
            self.assertFalse(first_global.exists())
            self.assertEqual(len(json.loads(report.read_text())), len(expected))

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
