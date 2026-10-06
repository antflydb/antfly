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

"""Local/server test collection must retain selection and type identity."""

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ZIG = Path(__file__).resolve().parents[1]


class LocalTestPartitions(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="antfly-local-tests-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        for relative in (
            "build_support/antfly/test_partitions.zig",
            "build_support/antfly/source_paths.zig",
            "build_support/embedded/source_owner.zig",
            "pkg/antfly-embedded/src/local/test_runner.zig",
            "pkg/antfly-embedded/src/local/test_error_logs.zig",
            "tools/audit_test_selection.py",
            "tools/run_test_partitions.py",
            "pkg/antfly/build/unit_test_ownership.zig",
            "pkg/antfly/build/unit_test_inventory.zig",
        ):
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ZIG / relative, target)
        self.write(
            "build_support/antfly/test_support.zig",
            """const std = @import("std");
pub fn configureTestRun(run: *std.Build.Step.Run) void {
    run.expectExitCode(0);
    run.has_side_effects = true;
}
""",
        )
        self.write(
            "pkg/antfly-embedded/src/local/source_catalog.zig",
            """pub const local = @import("local.zig");
comptime {
    for (@import("antfly_local_test_sources").names) |name| _ = @field(@This(), name);
}
""",
        )
        self.write(
            "pkg/antfly-embedded/src/local/local.zig",
            """const std = @import("std");
pub const Token = struct { value: u32 };
test "local owned" {
    const server = @import("antfly_server_test_sources");
    const token = Token{ .value = @import("options").value };
    try std.testing.expectEqual(@as(u32, 42), server.accept(token));
}
""",
        )
        self.write(
            "pkg/antfly/src/fixture.zig",
            """const std = @import("std");
const local = @import("antfly_local_sources").local;
pub fn accept(token: local.Token) u32 { return token.value; }
test "server owned" { try std.testing.expectEqual(@as(u32, 7), accept(.{ .value = 7 })); }
""",
        )
        self.write(
            "pkg/antfly/build/unit_test_ownership_rules.zig",
            """pub const rules: []const @import("unit_test_ownership.zig").Rule = &.{
    .{ .source = "pkg/antfly/src/fixture.zig", .artifact = "fixture",
       .selection = "all", .skip = &.{"local owned"} },
};
""",
        )
        self.write(
            "build.zig",
            """const std = @import("std");
const owner = @import("build_support/embedded/source_owner.zig");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/fixture.zig"),
        .target = target, .optimize = .debug,
    });
    owner.attach(root);
    const options = b.addOptions();
    options.addOption(u32, "value", 42);
    root.addOptions("options", options);
    root.addImport("antfly_test_error_logs", b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/local/test_error_logs.zig"),
        .target = target, .optimize = .debug,
    }));
    const tests = b.addTest(.{
        .name = "fixture", .root_module = root,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/local/test_runner.zig"), .mode = .simple },
    });
    const wrapped = b.option(bool, "wrapper", "Use the partition wrapper") orelse false;
    const run = if (wrapped) b.addSystemCommand(&.{"python3"}) else b.addRunArtifact(tests);
    if (wrapped) {
        run.addFileArg(b.path("tools/run_test_partitions.py"));
        run.addArg("--executable");
        run.addArtifactArg(tests);
        run.addArgs(&.{ "--partition-filter", "owned", "--" });
    }
    run.addPassthruArgs();
    b.step("test", "Run both owners").dependOn(&run.step);
    owner.finalize(b);
    if (b.option(bool, "aggregate", "Apply aggregate exclusions") orelse false)
        _ = @import("pkg/antfly/build/unit_test_ownership.zig").applyWithSourceOwners(b, &b.top_level_steps.get("test").?.step, @import("build_support/antfly/test_partitions.zig").consumerFor);
    owner.finalize(b);
}
""",
        )

    def write(self, relative, text):
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def build(self, *args, succeeds=True, build_options=()):
        result = subprocess.run(
            ["zig", "build", "test", "-j2", *build_options, "--", *args],
            cwd=self.root,
            text=True,
            capture_output=True,
            timeout=180,
        )
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode == 0, succeeds, output)
        return output

    def test_both_owners_run_once_with_shared_types_and_late_options(self):
        output = self.build()
        self.assertEqual(output.count("local owned..."), 1, output)
        self.assertEqual(output.count("server owned..."), 1, output)
        self.assertNotIn("multiple owners", output)

    def test_filter_can_match_either_owner_and_rejects_missing(self):
        self.assertIn("local owned...", self.build("--test-filter", "local owned"))
        self.assertIn("server owned...", self.build("--test-filter", "server owned"))
        output = self.build("--test-filter", "missing", succeeds=False)
        self.assertIn("test filter matched no declared tests", output)
        self.assertIn("local owned...", self.build("--test-filter=local owned"))
        output = self.build("--test-filter=missing", succeeds=False)
        self.assertIn("test filter matched no declared tests", output)

    def test_suite_filter_cannot_silently_empty_the_union(self):
        self.assertIn("local owned...", self.build("--suite-filter", "local owned"))
        output = self.build("--suite-filter", "missing", succeeds=False)
        self.assertIn("test filter matched no declared tests", output)

    def test_script_wrapper_runs_both_owners_and_audits_selection(self):
        options = ("-Dwrapper=true",)
        output = self.build(build_options=options)
        self.assertEqual(output.count("local owned..."), 1, output)
        self.assertEqual(output.count("server owned..."), 1, output)
        output = self.build(
            "--test-filter", "missing", build_options=options, succeeds=False
        )
        self.assertIn("test filter matched no declared tests", output)

    def test_aggregate_cloning_retains_local_owner_exclusions(self):
        output = self.build(build_options=("-Daggregate=true",))
        self.assertIn("server owned...", output)
        self.assertNotIn("local owned...", output)

    def test_control_profile_ignores_inactive_physical_imports(self):
        catalog = self.root / "pkg/antfly-embedded/src/local/source_catalog.zig"
        catalog.write_text(
            catalog.read_text()
            + '\n pub const physical = @import("physical.zig");\n'
            + 'pub const storage_db_generation_lifecycle = @import("lifecycle.zig");\n'
        )
        self.write(
            "pkg/antfly-embedded/src/local/physical.zig",
            """const DB = @import("antfly_source_root").antfly_sources.physical_db.DB;
test "physical owned" { _ = DB; }
""",
        )
        self.write(
            "pkg/antfly-embedded/src/local/lifecycle.zig",
            'test "physical lifecycle owned" {}\n',
        )
        fixture = self.root / "pkg/antfly/src/fixture.zig"
        fixture.write_text(
            fixture.read_text()
            + """
pub const antfly_sources = struct { pub const physical_db = struct {}; };
test {
    if (!@import("storage_source_options").control_only)
        _ = @import("antfly_local_sources").physical;
    _ = @import("antfly_local_sources").storage_db_generation_lifecycle;
}
"""
        )
        build = self.root / "build.zig"
        self.write(
            "pkg/antfly-embedded/src/local/source_catalog_control.zig",
            catalog.read_text().replace(
                ' pub const physical = @import("physical.zig");', ""
            ),
        )
        build.write_text(
            build.read_text().replace(
                "    owner.attach(root);",
                """    const storage = b.addOptions();
    storage.addOption(bool, "control_only", true);
    root.addOptions("storage_source_options", storage);
    owner.attach(root);""",
            )
        )
        output = self.build()
        self.assertIn("server owned...", output)
        self.assertIn("local owned...", output)
        self.assertNotIn("physical owned...", output)
        self.assertNotIn("physical lifecycle owned...", output)

    def test_strict_execution_accepts_empty_owner_and_listing_keeps_both(self):
        for name in ("server owned", "local owned"):
            output = self.build("--test-filter", name, "--require-no-skips")
            self.assertIn(name + "...", output)
        output = self.build("--list-tests", "--require-no-skips")
        self.assertEqual(output.count("TEST\tlocal.test.local owned"), 1, output)
        self.assertEqual(output.count("TEST\tfixture.test.server owned"), 1, output)

    def test_union_rejects_all_skipped_and_honors_explicit_empty(self):
        output = self.build("--skip-test-filter", "owned", succeeds=False)
        self.assertIn("test selection matched no runnable tests", output)
        output = self.build("--skip-test-filter=owned", succeeds=False)
        self.assertIn("test selection matched no runnable tests", output)
        self.build("--test-filter", "missing", "--allow-empty-test-filter")


if __name__ == "__main__":
    unittest.main()
