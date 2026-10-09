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

"""Validate last-consumer reclamation and private-cache boundaries."""

import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ZIG = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "release_test_artifact", ZIG / "tools/release_test_artifact.py"
)
release_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release_module)


class CacheLifetime(unittest.TestCase):
    def test_release_validates_every_path_before_removing_any_output(self):
        with tempfile.TemporaryDirectory() as root:
            cache = Path(root) / "zig-local"
            output = cache / "o" / ("a" * 32)
            output.mkdir(parents=True)
            (output / "test").write_bytes(b"test artifact")
            outside = Path(root) / ("b" * 32)
            outside.mkdir()
            with self.assertRaises(ValueError):
                release_module.release(cache, [output, outside])
            self.assertTrue((output / "test").exists())
            self.assertGreater(release_module.release(cache, [output, output]), 0)
            self.assertFalse(output.exists())

    @unittest.skipUnless(
        sys.platform.startswith("linux") or sys.platform == "darwin",
        "Unix cache lifetime policy",
    )
    def test_shared_compiler_output_survives_late_inventory_and_is_released(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root).resolve()
            for relative in (
                "build_support/antfly/test_cache_lifetime.zig",
                "build_support/antfly/source_paths.zig",
                "tools/release_test_artifact.py",
                "tools/audit_unit_test_ownership.py",
                "tools/run_bounded_zig_build.py",
                "tools/patch_zig_0_16_build_runner_maxrss.py",
                "tools/prune_completed_test_cache.py",
                "tools/report_test_cache.py",
                "Makefile",
            ):
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ZIG / relative, target)
            (root / "probe.zig").write_text(
                'extern fn library_probe() u32; test "late inventory probe" { try @import("std").testing.expectEqual(@as(u32, 42), library_probe()); }\n'
            )
            (root / "library.zig").write_text(
                "export fn library_probe() u32 { return 42; }\n"
            )
            (root / "build.zig").write_text("""const std = @import("std");
pub fn build(b: *std.Build) void {
    const module = b.createModule(.{ .root_source_file = b.path("probe.zig"), .target = b.graph.host, .optimize = .debug });
    const library = b.addLibrary(.{ .name = "probe-library", .linkage = .static, .root_module = b.createModule(.{ .root_source_file = b.path("library.zig"), .target = b.graph.host, .optimize = .debug }) });
    module.addObjectFile(library.getEmittedBin());
    const first = b.addTest(.{ .name = "shared-probe", .root_module = module });
    const alias = b.addTest(.{ .name = "shared-probe", .root_module = module });
    const gate = b.step("unit-test", "test lifetime");
    const run = b.addRunArtifact(first);
    gate.dependOn(&run.step);
    const late = b.addSystemCommand(&.{"python3"});
    late.addFileArg(b.path("tools/audit_unit_test_ownership.py"));
    late.addArg("--protocol-executable");
    late.addArtifactArg(alias);
    late.step.dependOn(&run.step);
    const inventory = late.captureStdErr(.{});
    const check = b.addSystemCommand(&.{ "python3", "-c", "import pathlib,sys; assert 'late inventory probe' in pathlib.Path(sys.argv[1]).read_text()" });
    check.addFileArg(inventory);
    gate.dependOn(&check.step);
    @import("build_support/antfly/test_cache_lifetime.zig").add(b, gate);
}
""")
            cache = root / "zig-local"
            result = subprocess.run(
                [
                    "zig",
                    "build",
                    "unit-test",
                    "-Dunit-test-cache-release=true",
                    "--cache-dir",
                    str(cache),
                    "--summary",
                    "all",
                    "--color",
                    "off",
                ],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=120,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn(
                "Released completed compiler outputs:", result.stdout + result.stderr
            )
            self.assertFalse(list(cache.glob("o/*/shared-probe")))
            self.assertFalse(list(cache.glob("o/*/libprobe-library.a")))
            # Metadata remains only until the invocation cache is retired. The
            # normal reusable policy must keep its executable on subsequent runs.
            shutil.rmtree(cache)
            result = subprocess.run(
                ["zig", "build", "unit-test", "--cache-dir", str(cache)],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=120,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(len(list(cache.glob("o/*/shared-probe"))), 1)
            # PR CI calls this checked-out Makefile through reusable workflows
            # on main. Exercise automatic enablement and manifest retirement
            # for both a successful gate and a test failure.
            environment = dict(os.environ, CI="true", ZIG_LOCAL_CACHE_DIR=str(cache))
            command = [
                "make",
                "unit-test",
                f"ZIG_BUILD_FLAGS=--cache-dir {cache} --maxrss 2147483648 -j2",
            ]
            for succeeds in (True, False):
                if not succeeds:
                    (root / "library.zig").write_text(
                        "export fn library_probe() u32 { return 43; }\n"
                    )
                result = subprocess.run(
                    command,
                    cwd=root,
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
                output = result.stdout + result.stderr
                self.assertEqual(result.returncode == 0, succeeds, output)
                self.assertIn("Released completed phase compiler cache", output)
                self.assertEqual(list(cache.iterdir()), [])

            # An explicit relative override wins over the environment. Retire
            # its normalized path and preserve the unrelated environment cache.
            (root / "library.zig").write_text(
                "export fn library_probe() u32 { return 42; }\n"
            )
            sentinel = cache / "unrelated-cache-entry"
            sentinel.write_text("preserve")
            override = root / "cache with spaces" / "zig-local"
            (root / "nested").mkdir()
            override_arg = Path("nested") / ".." / "cache with spaces" / "zig-local"
            result = subprocess.run(
                [
                    "make",
                    "unit-test",
                    f'ZIG_BUILD_FLAGS=--cache-dir "{override_arg}" --maxrss 2147483648 -j2',
                ],
                cwd=root,
                env=environment,
                capture_output=True,
                text=True,
                timeout=120,
            )
            output = result.stdout + result.stderr
            self.assertEqual(result.returncode, 0, output)
            self.assertIn("Released completed phase compiler cache", output)
            self.assertEqual(list(override.iterdir()), [])
            self.assertEqual(sentinel.read_text(), "preserve")
            result = subprocess.run(
                ["zig", "build", "unit-test", "--cache-dir", str(override)],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=120,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(len(list(override.glob("o/*/shared-probe"))), 1)

            # Reject redirected paths before any build can delete artifacts.
            link = root / "redirected" / "zig-local"
            link.parent.mkdir()
            link.symlink_to(override, target_is_directory=True)
            result = subprocess.run(
                ["make", "unit-test", f"ZIG_BUILD_FLAGS=--cache-dir {link}"],
                cwd=root,
                env=environment,
                capture_output=True,
                text=True,
                timeout=120,
            )
            output = result.stdout + result.stderr
            self.assertNotEqual(result.returncode, 0, output)
            self.assertIn("real job-owned zig-local directory", output)
            self.assertNotIn("Released completed compiler outputs", output)
            self.assertEqual(len(list(override.glob("o/*/shared-probe"))), 1)


if __name__ == "__main__":
    unittest.main()
