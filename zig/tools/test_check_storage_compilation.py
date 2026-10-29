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
import unittest
import sys
import tempfile
import threading
from unittest import mock
from pathlib import Path

try:
    from .fixture_process import run_fixture_build
except ImportError:
    from fixture_process import run_fixture_build


SPEC = importlib.util.spec_from_file_location(
    "check_storage_compilation",
    Path(__file__).with_name("check_storage_compilation.py"),
)
assert SPEC and SPEC.loader
measurement = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(measurement)


class LiteralSourceOwnership(unittest.TestCase):
    def test_catalog_check_accepts_current_and_rejects_stale_without_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            catalog = root / "pkg/antfly-embedded/src/source_catalog_control.zig"
            catalog.parent.mkdir(parents=True)
            with (
                mock.patch.object(measurement, "ZIG_ROOT", root),
                mock.patch.object(
                    measurement, "control_catalog_contents", return_value="current\n"
                ),
                mock.patch.object(
                    sys,
                    "argv",
                    ["check_storage_compilation", "--check-control-catalog"],
                ),
            ):
                catalog.write_text("current\n")
                measurement.main()
                catalog.write_text("stale\n")
                with mock.patch("sys.stderr"), self.assertRaises(SystemExit) as failure:
                    measurement.main()
                self.assertEqual(failure.exception.code, 1)
                self.assertEqual(catalog.read_text(), "stale\n")

    def test_removed_physical_section_preserves_formatter_spacing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            local = root / "pkg/antfly-embedded/src"
            local.mkdir(parents=True)
            (local / "source_catalog.zig").write_text(
                "// header\n" * 16
                + '\npub const db = @import("storage/db/db.zig");\n\ncomptime {}\n'
            )
            with mock.patch.object(measurement, "ZIG_ROOT", root):
                rendered = measurement.control_catalog_contents()
            self.assertNotIn("pub const db", rendered)
            self.assertNotIn("\n\n\n", rendered)
            self.assertIn("\n\ncomptime {}\n", rendered)

    def test_control_catalog_rejects_duplicate_source_names(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local = root / "pkg/antfly-embedded/src"
            local.mkdir(parents=True)
            (local / "source_catalog.zig").write_text(
                'pub const same = @import("leaf.zig");\n' * 2
            )
            (local / "leaf.zig").write_text("")
            with mock.patch.object(measurement, "ZIG_ROOT", root):
                with self.assertRaisesRegex(
                    ValueError, "duplicate source catalog entry: same"
                ):
                    measurement.control_catalog_contents()

    def test_control_catalog_is_current_and_has_no_physical_imports(self):
        local = measurement.ZIG_ROOT / "pkg/antfly-embedded/src"
        catalog = local / "source_catalog_control.zig"
        self.assertEqual(catalog.read_text(), measurement.control_catalog_contents())
        # These are control contracts; a new transitive physical import must
        # fail this small check rather than silently hiding their exports.
        for name in (
            "common_config",
            "api_local_backups",
            "storage_db_resolver_curation",
            "metadata_local_index_reconcile",
        ):
            self.assertIn(f"pub const {name} =", catalog.read_text())
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
                    for coordination in ("api/table_reads.zig", "api/table_writes.zig"):
                        path = measurement.literal_import_path(
                            root, source / coordination
                        )
                        self.assertIsNone(path, path)

    def test_shared_query_transforms_do_not_import_coordination(self):
        source = measurement.ZIG_ROOT / "pkg/antfly/src"
        root = source / "api/query_post_processing.zig"
        for coordination in ("api/table_reads.zig", "api/table_writes.zig"):
            with self.subTest(source=coordination):
                path = measurement.literal_import_path(root, source / coordination)
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
    def test_contract_reuses_both_caches_when_disk_has_headroom(self):
        self.exercise_contract_cache(low_disk=False)

    def test_measured_audit_selects_only_its_qualified_profile(self):
        with mock.patch.object(
            measurement, "qualified_audit_memory", return_value=True
        ):
            self.exercise_contract_cache(low_disk=False, measured_memory=True)

    def test_disk_pressure_rollover_discards_both_caches_and_keeps_mutations(self):
        self.exercise_contract_cache(low_disk=True)

    def exercise_contract_cache(self, *, low_disk, measured_memory=False):
        expected = (
            ("cold", set()),
            ("warm", set()),
            (
                "read coordination",
                {"antfly-runtime-distributed", "antfly-storage-kernel"},
            ),
            ("write coordination", {"antfly-runtime-distributed"}),
            ("physical DB", {"antfly-storage-kernel"}),
            ("physical local query", {"antfly-storage-kernel"}),
            ("owner integration test", {"storage-owner-tests"}),
            ("consumer test root", {"api-table-read-tests"}),
            *(
                (
                    ("storage contract cold", set()),
                    ("storage contract warm", set()),
                )
                if low_disk
                else ()
            ),
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

        def fake_build(
            command, cwd, *, progress, env, timeout_seconds, compiler_timeout_seconds
        ):
            nonlocal first_cache, first_global
            label, rebuilt = expected[len(seen)]
            self.assertEqual(timeout_seconds, 3600 if label.endswith("cold") else 1800)
            self.assertEqual(compiler_timeout_seconds, 1800)
            local_cache = cwd / command[command.index("--cache-dir") + 1]
            self.assertEqual(command[command.index("--build-file") + 1], "./build.zig")
            self.assertNotIn("--global-cache-dir", command)
            self.assertEqual(env["ZIG_LOCAL_CACHE_DIR"], ".zig-cache")
            self.assertIn("-Dstrip=true", command)
            if measured_memory:
                self.assertIn("-Dnative-linux-ownership-memory=true", command)
                self.assertIn("-Dcpu=baseline", command)
                self.assertIn("-Dcuda=false", command)
                self.assertIn("-Dinference-optimize=debug", command)
            global_cache = cwd / env["ZIG_GLOBAL_CACHE_DIR"]
            self.assertEqual(
                (global_cache / "p" / archive_name).read_bytes(), b"pinned source"
            )
            self.assertFalse((global_cache / "o").exists())
            self.assertFalse((global_cache / "p/unrelated.tar.gz").exists())
            self.assertEqual(local_cache.parent, cwd)
            self.assertEqual(global_cache.parent, cwd)
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
            if label == "storage contract" and not low_disk:
                self.assertTrue((local_cache / "marker").is_file())
                self.assertTrue((global_cache / "marker").is_file())
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
            seed = Path(directory) / "seed"
            (seed / "p").mkdir(parents=True)
            inventory = json.loads(
                (
                    measurement.ZIG_ROOT.parent / "scripts/ci/zig-dependencies.json"
                ).read_text()
            )
            archive_name = inventory["packages"][0]["hash"] + ".tar.gz"
            (seed / "p" / archive_name).write_bytes(b"pinned source")
            (seed / "p/unrelated.tar.gz").write_bytes(b"unrelated package")
            (seed / "o").mkdir()
            (seed / "o/compiled-object").write_bytes(b"must not seed compiler output")
            with (
                mock.patch.dict(os.environ, {"ZIG_GLOBAL_CACHE_DIR": str(seed)}),
                mock.patch.object(
                    sys,
                    "argv",
                    [
                        "check_storage_compilation.py",
                        "--report",
                        str(report),
                        "--case-workers",
                        "1",
                    ],
                ),
                mock.patch.object(
                    measurement, "measured_build", side_effect=fake_build
                ),
                # Trigger disk pressure only before the final mutation.
                mock.patch.object(
                    measurement.shutil,
                    "disk_usage",
                    side_effect=lambda *_: mock.Mock(
                        free=(8 if low_disk and len(seen) == 8 else 64) << 30
                    ),
                ),
            ):
                measurement.main()
            self.assertEqual(seen, [name for name, _ in expected])
            self.assertFalse(first_cache.exists())
            self.assertFalse(first_global.exists())
            self.assertEqual(len(json.loads(report.read_text())), len(expected))

    def test_active_process_diagnostics_exclude_other_builds(self):
        snapshot = """100 1 20 30:00 0:00 S python measure.py
101 100 30 29:59 0:01 S maker build
102 101 7000 28:00 27:40 R zig build-lib --name antfly-storage-kernel -Mroot=storage.zig
200 1 90000 40:00 39:50 R zig build-lib --name unrelated
"""
        rows = measurement.process_diagnostics(snapshot, 100)
        self.assertEqual([row["pid"] for row in rows], [100, 101, 102])
        compiler = rows[-1]
        self.assertEqual(compiler["artifact"], "antfly-storage-kernel")
        self.assertEqual(compiler["cpu_time"], "27:40")
        self.assertEqual(compiler["rss_bytes"], 7000 * 1024)
        self.assertNotIn("command", compiler)
        self.assertEqual(measurement.process_diagnostics("", 100), [])

    def test_active_compiler_response_file_identifies_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "args.rsp").write_text('"--name" "api-tests" "-Mroot=pkg/api.zig"')
            rows = measurement.process_diagnostics(
                "100 1 7000 28:00 27:40 R /bin/zig test @args.rsp", 100, root
            )
            self.assertEqual(rows[0]["artifact"], "api-tests")
            self.assertEqual(rows[0]["source_root"], "pkg/api.zig")
            self.assertNotIn("command", rows[0])
            (root / "args.rsp").unlink()
            rows = measurement.process_diagnostics(
                "100 1 7000 28:00 27:40 R /bin/zig test @args.rsp", 100, root
            )
            self.assertIsNone(rows[0]["artifact"])

    def test_audit_memory_profile_requires_the_measured_compiler(self):
        with (
            mock.patch.object(measurement.sys, "platform", "linux"),
            mock.patch.object(measurement.platform, "machine", return_value="x86_64"),
            mock.patch.object(
                measurement.shutil,
                "which",
                return_value=str(measurement.QUALIFIED_AUDIT_COMPILER),
            ),
        ):
            self.assertTrue(measurement.qualified_audit_memory("zig"))
            self.assertFalse(measurement.qualified_audit_memory("zig", jobs=3))
            with mock.patch.object(
                measurement.shutil, "which", return_value="/opt/zig/zig"
            ):
                self.assertFalse(measurement.qualified_audit_memory("zig"))
            with mock.patch.object(measurement.sys, "platform", "darwin"):
                self.assertFalse(measurement.qualified_audit_memory("zig"))
            with mock.patch.object(
                measurement.platform, "machine", return_value="aarch64"
            ):
                self.assertFalse(measurement.qualified_audit_memory("zig"))

    def test_cgroup_measurement_distinguishes_anonymous_and_file_memory(self):
        with (
            mock.patch.object(measurement.sys, "platform", "linux"),
            mock.patch.object(
                measurement.Path,
                "read_text",
                return_value="anon 100\nfile 900\nfile_dirty 50\ninactive_file 700\n",
            ),
        ):
            self.assertEqual(
                measurement.cgroup_memory_stat(),
                {
                    "anon": 100,
                    "file": 900,
                    "file_dirty": 50,
                    "inactive_file": 700,
                },
            )
            with mock.patch.object(
                measurement.Path, "read_text", side_effect=FileNotFoundError
            ):
                self.assertEqual(measurement.cgroup_memory_stat(), {})

    def test_timeout_preserves_output_and_measurements(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.object(measurement.subprocess, "check_output", return_value=""),
            mock.patch.object(
                measurement,
                "active_build_processes",
                return_value=[{"artifact": "storage", "cpu_time": "29:59"}],
            ) as diagnostics,
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
        diagnostics.assert_called_once()
        self.assertEqual(measured["active_processes"][0]["cpu_time"], "29:59")
        self.assertGreater(measured["wall_seconds"], 0)

    def test_compiler_watchdog_allows_a_later_unit_without_extending_a_stuck_one(self):
        watchdog = measurement.CompilerWatchdog(1800, 0)
        first = {"pid": 1, "executable": "zig", "elapsed": "29:59"}
        self.assertFalse(watchdog.expired(1799, [first]))
        later = {"pid": 2, "executable": "zig", "elapsed": "00:14"}
        self.assertFalse(watchdog.expired(1814, [later]))
        # Another compiler's progress cannot hide an overlong concurrent unit.
        first["elapsed"] = "30:01"
        self.assertTrue(watchdog.expired(1815, [first, later]))

    def test_compiler_watchdog_bounds_idle_orchestration_and_unknown_clocks(self):
        watchdog = measurement.CompilerWatchdog(10, 0)
        self.assertFalse(watchdog.expired(9, [{"executable": "maker"}]))
        self.assertTrue(watchdog.expired(10, [{"executable": "maker"}]))
        unknown = {"pid": 1, "executable": "zig", "elapsed": "unknown"}
        self.assertFalse(watchdog.expired(11, [unknown]))
        self.assertTrue(watchdog.expired(21, [unknown]))
        self.assertTrue(
            watchdog.expired(
                22, [{"pid": 2, "executable": "zig", "elapsed": "1-00:00:01"}]
            )
        )

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


class ParallelAuditIsolation(unittest.TestCase):
    def test_workspace_clone_preserves_source_metadata_and_private_mutations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = measurement.AuditWorkspace(root / "baseline", 2)
            cloned = measurement.AuditWorkspace(root / "worker", 1)
            source.cwd.mkdir(parents=True)
            owned = source.cwd / "owned.zig"
            owned.write_text("original")
            owned.chmod(0o640)
            os.utime(owned, ns=(1_600_000_000_000_000_000,) * 2)
            (source.cwd / "internal").symlink_to(owned)
            external = root / "immutable.zig"
            external.write_text("external")
            (source.cwd / "external").symlink_to(external)
            for cache in (source.local_cache, source.global_cache):
                (cache / "o/hash").mkdir(parents=True)
                (cache / "o/hash/bytes").write_text("cached")
            measurement.clone_audit_workspace(source, cloned)
            copied = cloned.cwd / "owned.zig"
            self.assertEqual(copied.stat().st_mtime_ns, owned.stat().st_mtime_ns)
            self.assertEqual(copied.stat().st_mode, owned.stat().st_mode)
            self.assertNotEqual(copied.stat().st_ino, owned.stat().st_ino)
            copied.write_text("worker edit")
            self.assertEqual(owned.read_text(), "original")
            shutil.rmtree(source.root)
            self.assertEqual((cloned.cwd / "internal").read_text(), "worker edit")
            self.assertEqual((cloned.cwd / "external").read_text(), "external")

    def test_cache_clones_share_immutable_objects_and_keep_manifests_private(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "baseline"
            for relative in (
                "o/hash/library.a",
                "h/input.txt",
                "o/hash/cache.lock",
                "p/package/source.zig",
            ):
                path = source / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(relative)
            (source / "o/hash/absolute").symlink_to(source / "o/hash/library.a")
            (source / "o/hash/relative").symlink_to("library.a")
            left, right = root / "left", root / "right"
            measurement.clone_audit_cache(source, left)
            measurement.clone_audit_cache(source, right)
            for relative in ("o/hash/library.a", "p/package/source.zig"):
                self.assertEqual(
                    (left / relative).stat().st_ino, (right / relative).stat().st_ino
                )
            for relative in ("h/input.txt", "o/hash/cache.lock"):
                self.assertNotEqual(
                    (left / relative).stat().st_ino, (right / relative).stat().st_ino
                )
                (left / relative).write_text("worker edit")
                self.assertEqual((right / relative).read_text(), relative)
            (left / "o/new").mkdir()
            (left / "o/new/output").write_text("new compiler output")
            self.assertFalse((right / "o/new").exists())
            shutil.rmtree(left)
            shutil.rmtree(source)
            self.assertEqual(
                (right / "o/hash/absolute").read_text(), "o/hash/library.a"
            )
            self.assertEqual(
                (right / "o/hash/relative").read_text(), "o/hash/library.a"
            )
            self.assertEqual(
                (right / "o/hash/library.a").read_text(), "o/hash/library.a"
            )

    def test_unqualified_compilers_cannot_enable_two_workers(self):
        with (
            mock.patch.object(
                measurement, "qualified_audit_memory", return_value=False
            ),
            mock.patch.object(sys, "argv", ["audit", "--case-workers", "2"]),
            mock.patch.object(measurement, "clone_audit_cache") as clone,
            mock.patch("sys.stderr"),
            self.assertRaises(SystemExit) as failure,
        ):
            measurement.main()
        self.assertEqual(failure.exception.code, 2)
        clone.assert_not_called()

    def test_parallel_workers_preserve_all_probes_and_overlap_single_compilers(self):
        self.exercise_parallel()

    def test_relocated_cache_misses_fail_before_mutation_workers_start(self):
        self.exercise_parallel(dirty_clone=True)

    def test_a_failed_worker_interrupts_its_peer_and_retains_a_valid_report(self):
        self.exercise_parallel(fail_probe="read coordination")

    def exercise_parallel(self, *, dirty_clone=False, fail_probe=None):
        sources = {
            "api/table_reads.zig": (
                "read coordination",
                {"antfly-runtime-distributed", "antfly-storage-kernel"},
            ),
            "api/table_writes.zig": (
                "write coordination",
                {"antfly-runtime-distributed"},
            ),
            "storage/db/db.zig": ("physical DB", {"antfly-storage-kernel"}),
            "storage/query.zig": ("physical local query", {"antfly-storage-kernel"}),
            "storage/kernel_owner_test.zig": (
                "owner integration test",
                {"storage-owner-tests"},
            ),
            "api_table_reads_test_root.zig": (
                "consumer test root",
                {"api-table-read-tests"},
            ),
            "storage/kernel_owner_abi.zig": (
                "storage contract",
                {
                    "antfly-storage-kernel",
                    "antfly-runtime-distributed",
                    "storage-owner-tests",
                },
            ),
        }
        prior, seen, first_mutation = {}, [], set()
        barrier = threading.Barrier(2)
        lock = threading.Lock()
        names = (
            measurement.ARCHIVES
            | measurement.CONSUMERS
            | {
                "storage-owner-tests",
                "storage-owner-source-tests",
                "storage-owner-enrichment-tests",
            }
        )

        def fake_build(command, cwd, *, progress, env, **kwargs):
            self.assertEqual(command[command.index("--build-file") + 1], "./build.zig")
            current = {
                source
                for source in sources
                if (cwd.parent / measurement.source_owner_path(source))
                .read_bytes()
                .endswith(b"// storage compilation ownership regression\n")
            }
            with lock:
                initial = cwd not in prior
                changed = current - prior.get(cwd, set())
                cold = not prior
                prior[cwd] = current
            rebuilt = names if cold else set()
            if changed:
                self.assertEqual(len(changed), 1)
                label, rebuilt = sources[changed.pop()]
                self.assertIn("-j1", command)
                if cwd not in first_mutation:
                    first_mutation.add(cwd)
                    barrier.wait(timeout=5)
                with lock:
                    seen.append(label)
            else:
                label = "cold" if cold else "warm"
                self.assertTrue(initial or not current)
            local_cache = cwd / command[command.index("--cache-dir") + 1]
            local_cache.mkdir(exist_ok=True)
            (local_cache / "marker").write_text(label)
            output = [
                f"compile {'test_obj' if name in measurement.CONSUMERS else 'lib'} {name} debug native {'success' if name in rebuilt else 'cached'}"
                for name in names
            ]
            if label in {"physical DB", "physical local query"}:
                output.extend(
                    f"compile exe {name} debug native success"
                    for name in measurement.CONSUMERS
                )
            if dirty_clone and initial and not cold:
                # Private variants are not in the required root set. Repeated
                # names must not hide a rebuild behind a later cached result.
                output.extend(
                    [
                        "compile lib antfly-storage-engine debug native success",
                        "compile lib antfly-storage-engine debug native cached",
                    ]
                )
            progress({"wall_seconds": 0.0})
            return (
                (1 if label == fail_probe else 0),
                "\n".join(output),
                {"wall_seconds": 0.0, "timed_out": False},
            )

        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            with (
                mock.patch.object(
                    measurement, "qualified_audit_memory", return_value=True
                ),
                mock.patch.object(
                    measurement,
                    "seed_global_cache",
                    side_effect=lambda path: path.mkdir(parents=True, exist_ok=True),
                ),
                mock.patch.object(
                    measurement, "measured_build", side_effect=fake_build
                ),
                mock.patch.object(
                    measurement.shutil,
                    "disk_usage",
                    return_value=mock.Mock(free=100 * (1 << 30)),
                ),
                mock.patch.object(
                    sys,
                    "argv",
                    ["check_storage_compilation.py", "--report", str(report)],
                ),
            ):
                if dirty_clone:
                    with self.assertRaises(AssertionError):
                        measurement.main()
                elif fail_probe:
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "read coordination|another storage audit worker failed",
                    ):
                        measurement.main()
                else:
                    measurement.main()
            rows = json.loads(report.read_text())
            if dirty_clone:
                self.assertEqual(seen, [])
                return
            if fail_probe:
                self.assertIn(fail_probe, seen)
                self.assertLess(len(seen), len(sources))
                self.assertTrue(any(row["status"] == "failed" for row in rows))
                return
            self.assertEqual(len(rows), 11)
            self.assertTrue(all(row["status"] == "completed" for row in rows))
            self.assertCountEqual(seen, [label for label, _ in sources.values()])
            self.assertEqual(len(first_mutation), 2)


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
        (cls.root / "consumer.zig").write_text(
            "pub export fn _start() void {}\n"
            "pub export fn wWinMainCRTStartup() void {}\n"
            "pub export fn fixtureValue() u32 { return 1; }\n"
        )
        (cls.root / "project_build.zig").write_text(r"""
const std = @import("std");
pub fn create(b: *std.Build) ?void {
    const omit = b.option([]const u8, "omit", "Omit a required consumer");
    const target = b.standardTargetOptions(.{});
    const files = b.addWriteFiles();
    // These artifacts are inspected, never executed. Keep native compilation
    // and generated-source cache inputs without compiling startup, compiler-rt
    // or UBSan once per synthetic consumer in every isolated cold cache.
    const root = files.addCopyFile(b.path("consumer.zig"), "main.zig");
    inline for (.{
        "storage-owner-tests", "storage-owner-source-tests", "storage-owner-enrichment-tests",
        "api-table-read-tests", "api-table-write-tests", "api-table-write-lifecycle-tests", "data-runtime-tests",
    }) |name| {
        if (omit == null or !std.mem.eql(u8, omit.?, name)) {
            const exe = b.addExecutable(.{ .name = name, .root_module = b.createModule(.{
                .root_source_file = root, .target = target, .optimize = .debug, .strip = true,
            }) });
            exe.entry = .disabled;
            exe.bundle_compiler_rt = false;
            exe.bundle_ubsan_rt = false;
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
        return run_fixture_build(
            [
                self.zig,
                "build",
                "--build-file",
                "./build.zig",
                "check-storage-compilation",
                "-j2",
                *args,
            ],
            cwd=self.root,
            # Fixture-generated sources belong to its own lifetime, even when
            # the caller configures a disposable phase cache for other builds.
            env={**os.environ, "ZIG_LOCAL_CACHE_DIR": str(self.root / ".zig-cache")},
            timeout=120,
        )

    def test_cloned_compiler_caches_relocate_without_rebuilding_generated_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline" / "zig"
            shutil.copytree(
                self.root,
                baseline,
                ignore=shutil.ignore_patterns(".zig-cache", ".zig-global-cache"),
            )

            def build_at(checkout):
                # Discovery/missing-owner cases below still cover all seven
                # consumers. One native generated-source consumer exercises
                # the complete relocation/invalidation lifecycle here.
                result = run_fixture_build(
                    [
                        self.zig,
                        "build",
                        "--build-file",
                        "./build.zig",
                        "storage-owner-tests",
                        "-j1",
                        "--cache-dir",
                        ".zig-cache",
                        "--summary",
                        "all",
                        "--color",
                        "off",
                    ],
                    cwd=checkout,
                    env={
                        **os.environ,
                        "ZIG_LOCAL_CACHE_DIR": ".zig-cache",
                        "ZIG_GLOBAL_CACHE_DIR": ".zig-global-cache",
                    },
                    timeout=120,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                return result.stderr

            build_at(baseline)
            clone = root / "clone" / "zig"
            measurement.clone_audit_workspace(
                measurement.AuditWorkspace(baseline.parent, 1),
                measurement.AuditWorkspace(clone.parent, 1),
            )
            output = build_at(clone)
            self.assertIn("cached", output)
            self.assertNotRegex(output, r"compile (?:exe|lib) .* success")
            # The clone's new source revision and manifests are private. The
            # original still gets real compiler hits after that revision.
            # Change the tracked generator input, preserving a real source
            # invalidation without recompiling the host build runner itself.
            consumer = measurement.own(clone.parent, "zig/consumer.zig")
            consumer.write_text(
                consumer.read_text().replace(
                    "pub export fn fixtureValue() u32 { return 1; }",
                    "pub export fn fixtureValue() u32 { return 2; }",
                )
            )
            output = build_at(clone)
            self.assertRegex(output, r"compile exe .* success")
            output = build_at(baseline)
            self.assertNotRegex(output, r"compile (?:exe|lib) .* success")
            shutil.rmtree(baseline.parent)
            output = build_at(clone)
            self.assertNotRegex(output, r"compile (?:exe|lib) .* success")

    def test_extra_owner_fixture_does_not_expand_audit(self):
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_inherited_local_cache_stays_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            shared = Path(directory)
            marker = shared / "caller-owned"
            marker.write_text("preserve")
            with mock.patch.dict(os.environ, {"ZIG_LOCAL_CACHE_DIR": str(shared)}):
                result = self.build()
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(list(shared.iterdir()), [marker])
            self.assertEqual(marker.read_text(), "preserve")
            self.assertTrue((self.root / ".zig-cache" / "o").is_dir())

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
