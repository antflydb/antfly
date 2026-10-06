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

"""Test measurement accounting without invoking a compiler."""

import importlib.util
import json
import os
import shutil
import subprocess
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


class LiteralSourceOwnership(unittest.TestCase):
    def test_control_catalog_is_current_and_has_no_physical_imports(self):
        local = measurement.ZIG_ROOT / "pkg/antfly-embedded/src"
        catalog = local / "source_catalog_control.zig"
        self.assertEqual(catalog.read_text(), measurement.control_catalog_contents())
        for relative in ("storage/db/db.zig", "storage/query.zig", "storage/write.zig"):
            with self.subTest(source=relative):
                self.assertIsNone(
                    measurement.literal_import_path(catalog, local / relative)
                )

    def test_control_roots_have_no_physical_implementation_import_path(self):
        source = measurement.ZIG_ROOT / "pkg/antfly/src"
        physical_db = measurement.ZIG_ROOT / "pkg/antfly-embedded/src/storage/db/db.zig"
        self.assertTrue(physical_db.is_file())
        roots = [
            *(
                f"runtime_{unit}_root.zig"
                for unit in (
                    "cli",
                    "distributed",
                    "enrichment_compute",
                    "serverless",
                    "inference",
                    "api_kernel",
                )
            ),
            "storage_kernel_owner_test_root.zig",
            "storage_kernel_provisioned_source_test_root.zig",
            "enrichment_compute_test_root.zig",
            "api_table_reads_test_root.zig",
            "api_table_writes_test_root.zig",
            "data_runtime_test_root.zig",
        ]
        for name in roots:
            with self.subTest(root=name):
                root = source / name
                self.assertTrue(root.is_file(), name)
                path = measurement.literal_import_path(root, physical_db)
                self.assertIsNone(path, path)
                if (
                    name.startswith("runtime_")
                    and name != "runtime_distributed_root.zig"
                ):
                    path = measurement.literal_import_path(
                        root, source / "api/table_writes.zig"
                    )
                    self.assertIsNone(path, path)

    def test_imports_in_inactive_test_bodies_are_cache_dependencies(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / "owner.zig").write_text(
                '// @import("comment.zig")\n'
                """const quote = '"';\n"""
                'const doc = "@import(\\"string.zig\\")";\n'
                'test { if (false) { _ = @import("helper.zig"); } }\n'
            )
            (root / "helper.zig").write_text('const db = @import("db.zig");\n')
            (root / "db.zig").write_text("")
            self.assertEqual(
                measurement.literal_import_path(root / "owner.zig", root / "db.zig"),
                [root / "owner.zig", root / "helper.zig", root / "db.zig"],
            )
            self.assertIsNone(
                measurement.literal_import_path(
                    root / "owner.zig", root / "comment.zig"
                )
            )
            self.assertIsNone(
                measurement.literal_import_path(root / "owner.zig", root / "string.zig")
            )


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

        def fake_build(command, cwd, *, progress, env):
            nonlocal first_cache, first_global
            label, rebuilt = expected[len(seen)]
            local_cache = Path(command[command.index("--cache-dir") + 1])
            self.assertNotIn("--global-cache-dir", command)
            global_cache = Path(env["ZIG_GLOBAL_CACHE_DIR"])
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
                output.append(f"compile {kind} {name} debug native {status}")
            if label in {"physical DB", "physical local query"}:
                output.extend(
                    f"compile exe {name} debug native success"
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
                # This case exercises the explicit contract rollover. Host disk
                # pressure has its own runtime trigger and must not change the
                # mocked sequence of builds.
                mock.patch.object(
                    measurement.shutil,
                    "disk_usage",
                    return_value=mock.Mock(free=64 << 30),
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


class StorageCompilationDiscovery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.zig = shutil.which(os.environ.get("ANTFLY_ZIG", "zig"))
        if not cls.zig:
            raise unittest.SkipTest("Zig is required for build-graph regression tests")
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        fixtures = cls.root / "tools/fixtures"
        fixtures.mkdir(parents=True)
        source = Path(__file__).parent / "fixtures"
        shutil.copyfile(source / "storage_compilation.zig", cls.root / "build.zig")
        shutil.copyfile(source / "build_profiles.zig", fixtures / "build_profiles.zig")
        (cls.root / "project_build.zig").write_text(r"""
const std = @import("std");
pub fn create(b: *std.Build) ?void {
    const omit = b.option([]const u8, "omit", "Omit a required consumer");
    const target = b.standardTargetOptions(.{});
    const files = b.addWriteFiles();
    const root = files.add("main.zig", "pub fn main() void {}\n");
    inline for (.{
        "storage-owner-tests", "storage-owner-source-tests", "storage-owner-enrichment-tests",
        "api-table-read-tests", "api-table-write-tests", "api-table-write-lifecycle-tests", "data-runtime-tests",
    }) |name| {
        if (omit == null or !std.mem.eql(u8, omit.?, name)) {
            const exe = b.addExecutable(.{ .name = name, .root_module = b.createModule(.{
                .root_source_file = root, .target = target, .optimize = .debug,
            }) });
            b.step(name, name).dependOn(&exe.step);
        }
    }
    // Any accidental inclusion of another owner fixture fails compilation.
    const unrelated = b.addExecutable(.{
        .name = "storage-owner-handoff-reopen-tests",
        .root_module = b.createModule(.{
            .root_source_file = files.add("unrelated.zig", "comptime { @compileError(\"unrelated owner fixture compiled\"); }"),
            .target = target, .optimize = .debug,
        }),
    });
    b.step("unrelated", "unrelated").dependOn(&unrelated.step);
    inline for (.{ "cli", "distributed", "storage_kernel", "enrichment_compute", "serverless", "inference", "api_kernel" }) |unit| {
        _ = b.step("runtime-unit-" ++ unit, unit);
    }
    return {};
}
""")

    def build(self, *args):
        return subprocess.run(
            [self.zig, "build", "check-storage-compilation", "-j2", *args],
            cwd=self.root,
            text=True,
            capture_output=True,
            timeout=120,
        )

    def test_extra_owner_fixture_does_not_expand_audit(self):
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_required_artifact_reports_its_identity(self):
        for name in ("storage-owner-source-tests", "api-table-read-tests"):
            with self.subTest(name=name):
                result = self.build(f"-Domit={name}")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    f"missing audited storage test artifact: {name}", result.stderr
                )


if __name__ == "__main__":
    unittest.main()
