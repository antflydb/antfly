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

import os
import platform
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ZIG = Path(__file__).resolve().parents[1]


class LocalTestPartitions(unittest.TestCase):
    def setUp(self):
        fixture_cache = ZIG / ".zig-cache" / "local-test-partition-fixtures"
        fixture_cache.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix="owner-", dir=fixture_cache)
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        # source_owner now attaches the shared decision contract. Copy its
        # complete authored source closure so these remain real build fixtures.
        shutil.copytree(ZIG / "lib/decisions", self.root / "lib/decisions")
        for relative in (
            "pkg/inference/build/decision_modules.zig",
            "build_support/antfly/test_partitions.zig",
            "build_support/antfly/source_paths.zig",
            "build_support/embedded/source_owner.zig",
            "pkg/antfly-embedded/src/test_runner.zig",
            "pkg/antfly-embedded/src/test_error_logs.zig",
            "tools/audit_test_selection.py",
            "tools/run_test_partitions.py",
            "pkg/antfly/build/runtime_memory.zig",
            "pkg/antfly/build/test_owner_names.zig",
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
            "pkg/antfly-embedded/src/source_catalog.zig",
            """pub const local = @import("local.zig");
comptime {
    for (@import("antfly_local_test_sources").names) |name| _ = @field(@This(), name);
}
""",
        )
        self.write(
            "pkg/antfly-embedded/src/local.zig",
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
        .root_source_file = b.path("pkg/antfly-embedded/src/test_error_logs.zig"),
        .target = target, .optimize = .debug,
    }));
    const tests = b.addTest(.{
        .name = "fixture", .root_module = root,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
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
    if (b.option(bool, "owner-audit", "Audit a split source owner") orelse false) {
        const inventory = b.addRunArtifact(tests);
        inventory.addArg("--list-tests");
        const audit = b.addSystemCommand(&.{"python3"});
        audit.addFileArg(b.path("tools/audit_test_selection.py"));
        audit.addArg("--inventory");
        audit.addFileArg(inventory.captureStdErr(.{}));
        audit.addArg("--");
        audit.addPassthruArgs();
        run.step.dependOn(&audit.step);
    }
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

    def build(self, *args, succeeds=True, build_options=(), step="test"):
        fixture_env = os.environ.copy()
        fixture_env.setdefault(
            "ZIG_GLOBAL_CACHE_DIR", str(ZIG / ".zig-cache" / "global")
        )
        result = subprocess.run(
            [
                fixture_env.get("ZIG", "zig"),
                "build",
                step,
                "--cache-dir",
                str(self.root / ".zig-cache"),
                "-j2",
                *build_options,
                "--",
                *args,
            ],
            cwd=self.root,
            env=fixture_env,
            text=True,
            capture_output=True,
            timeout=180,
        )
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode == 0, succeeds, output)
        return output

    def test_shared_root_name_preserves_single_execution(self):
        # Exercise the production rule, rather than a copied label expectation.
        shutil.copyfile(
            ZIG / "pkg/antfly/build/unit_test_ownership_rules.zig",
            self.root / "pkg/antfly/build/unit_test_ownership_rules.zig",
        )
        self.write(
            "pkg/antfly/src/runtime_callback_abi.zig",
            """const std = @import("std");
test "owned ABI fixture contract" {
    var marker = std.Io.Dir.cwd().createFile(std.testing.io, "shared-ran-once", .{ .exclusive = true }) catch |err| {
        std.debug.print("duplicate fixture owner\\n", .{});
        return err;
    };
    defer marker.close(std.testing.io);
}
""",
        )
        self.write(
            "pkg/antfly/src/root.zig",
            """const std = @import("std");
pub const runtime_callback_abi = @import("runtime_callback_abi.zig");
test { _ = runtime_callback_abi; }
test "compiled CHECK root fixture contract" {
    var marker = try std.Io.Dir.cwd().createFile(std.testing.io, "root-ran-once", .{ .exclusive = true });
    defer marker.close(std.testing.io);
}
""",
        )
        self.write(
            "pkg/antfly/src/canonical.zig",
            """pub const runtime_callback_abi = @import("runtime_callback_abi.zig");
test { _ = runtime_callback_abi; }
""",
        )
        self.write(
            "build.zig",
            """const std = @import("std");
const names = @import("pkg/antfly/build/test_owner_names.zig");
const ownership = @import("pkg/antfly/build/unit_test_ownership.zig");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const logs = b.createModule(.{ .root_source_file = b.path("pkg/antfly-embedded/src/test_error_logs.zig"), .target = target, .optimize = .debug });
    const legacy = b.option(bool, "legacy-owner", "Negative control with the former anonymous label") orelse false;
    const root = b.createModule(.{ .root_source_file = b.path("pkg/antfly/src/root.zig"), .target = target, .optimize = .debug, .link_libc = true });
    const canonical = b.createModule(.{ .root_source_file = b.path("pkg/antfly/src/canonical.zig"), .target = target, .optimize = .debug, .link_libc = true });
    root.addImport("antfly_test_error_logs", logs);
    canonical.addImport("antfly_test_error_logs", logs);
    const root_tests = b.addTest(.{ .name = if (legacy) "test" else names.root_unit, .root_module = root,
        .max_rss = 1536 * 1024 * 1024,
        .filters = &.{ "compiled CHECK", "owned ABI" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple } });
    const canonical_tests = b.addTest(.{ .name = "canonical-abi-tests", .root_module = canonical,
        .max_rss = 1536 * 1024 * 1024,
        .filters = &.{"owned ABI"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple } });
    const root_run = b.addRunArtifact(root_tests);
    root_run.addArgs(&.{ "--suite-filter", "compiled CHECK", "--suite-filter", "owned ABI" });
    const canonical_run = b.addRunArtifact(canonical_tests);
    canonical_run.addArgs(&.{ "--suite-filter", "owned ABI" });
    for ([_]*std.Build.Step.Run{ root_run, canonical_run }) |run| {
        run.expectExitCode(0);
        run.has_side_effects = true;
    }
    const gate = b.step("test", "Verify aggregate ownership before execution");
    gate.dependOn(&root_run.step);
    gate.dependOn(&canonical_run.step);
    _ = ownership.apply(b, gate);
}
""",
        )
        self.build(build_options=("--maxrss", str(2 * 1024**3)))
        for name in ("shared-ran-once", "root-ran-once"):
            self.assertTrue((self.root / name).exists(), name)
            (self.root / name).unlink()
        output = self.build(
            succeeds=False,
            build_options=("--maxrss", str(2 * 1024**3), "-Dlegacy-owner=true"),
        )
        self.assertIn("duplicate fixture owner", output)

    def test_portable_install_retains_local_tests_after_binding_and_repeated_finalize(
        self,
    ):
        build = self.root / "build.zig"
        build.write_text(
            build.read_text().replace(
                "    const wrapped =",
                '    @import("build_support/antfly/test_partitions.zig").installLocalArtifact(b, tests, "portable-tests", "Install local owner", "local-tests");\n'
                "    const wrapped =",
            )
        )
        output = self.build(step="portable-tests")
        self.assertNotIn("local owned...", output)
        result = subprocess.run(
            [
                str(self.root / "zig-out/bin/local-tests"),
                "--test-filter",
                "local owned",
                "--require-no-skips",
            ],
            cwd=self.root,
            text=True,
            capture_output=True,
            timeout=30,
        )
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, 0, output)
        self.assertIn("local owned...", output)
        self.assertNotIn("server owned...", output)

    @unittest.skipUnless(
        platform.system() == "Darwin"
        and platform.machine().lower() in ("arm64", "aarch64"),
        "native Darwin admission profile",
    )
    def test_qualified_local_claim_does_not_grow_with_consumer_reservation(self):
        build = self.root / "build.zig"
        source = build.read_text().replace(
            "const target = b.standardTargetOptions(.{});",
            "const target = b.resolveTargetQuery(.{ .cpu_model = .baseline });",
        )
        source = source.replace(
            "    owner.attach(root);",
            """    @import("build_support/antfly/test_partitions.zig").setMemoryProfile(b, .{
        .native_darwin_cpu_debug = b.option(bool, "profile", "Qualified profile") orelse false,
    });
    owner.attach(root);""",
        ).replace(
            '.name = "fixture", .root_module = root,',
            '.name = "data-runtime-tests", .root_module = root,\n'
            '        .max_rss = b.option(usize, "consumer-claim", "Consumer reservation") orelse unreachable,',
        )
        source = source.replace(
            "    owner.finalize(b);\n}",
            """    owner.finalize(b);
    std.debug.print("local-claim={}\\n", .{localClaim(&b.top_level_steps.get("test").?.step).?});
}
fn localClaim(step: *std.Build.Step) ?usize {
    if (step.cast(std.Build.Step.Compile)) |artifact|
        if (std.mem.eql(u8, artifact.name, "data-runtime-tests-local"))
            return step.max_rss;
    for (step.dependencies.items) |dependency|
        if (localClaim(dependency)) |claim| return claim;
    return null;
}""",
        )
        build.write_text(source)

        def claim(consumer, qualified):
            output = self.build(
                build_options=(
                    f"-Dconsumer-claim={consumer}",
                    f"-Dprofile={str(qualified).lower()}",
                )
            )
            match = re.search(r"local-claim=(\d+)", output)
            self.assertIsNotNone(match, output)
            return int(match.group(1))

        inherited = 16 * 1024**3
        measured = claim(inherited, True)
        self.assertLess(measured, inherited)
        self.assertEqual(measured, claim(20 * 1024**3, True))
        self.assertEqual(inherited, claim(inherited, False))

    def test_authored_partition_claim_is_independent_of_consumer_claim(self):
        build = self.root / "build.zig"
        source = (
            build.read_text()
            .replace(
                '.name = "fixture", .root_module = root,',
                '.name = "fixture", .root_module = root, .max_rss = 18 * 1024 * 1024 * 1024,',
            )
            .replace(
                "    const wrapped = b.option",
                '    @import("build_support/antfly/test_partitions.zig").setAuthoredMaxRss(b, tests, 1536 * 1024 * 1024);\n    const wrapped = b.option',
            )
            .replace(
                "    owner.finalize(b);\n}",
                r"""    owner.finalize(b);
    std.debug.print("local-claim={}\n", .{localClaim(&b.top_level_steps.get("test").?.step).?});
}
fn localClaim(step: *std.Build.Step) ?usize {
    if (step.cast(std.Build.Step.Compile)) |artifact|
        if (std.mem.eql(u8, artifact.name, "fixture-local")) return step.max_rss;
    for (step.dependencies.items) |dependency|
        if (localClaim(dependency)) |claim| return claim;
    return null;
}""",
            )
        )
        build.write_text(source)
        output = self.build()
        self.assertIn("local-claim=1610612736", output)
        self.assertIn("local owned...", output)
        self.assertIn("server owned...", output)

    def test_long_run_allows_ready_sibling_compilation(self):
        self.write("probe.zig", "export fn probe() u32 { return 42; }\n")
        self.write(
            "gate.py",
            """from pathlib import Path
import sys, time
mode = sys.argv[1]
if mode == 'hold': Path('started').write_text('ready')
if mode == 'release': Path('released').write_text('done')
else:
    marker = Path('released' if mode == 'hold' else 'started')
    deadline = time.monotonic() + 30
    while not marker.exists():
        if time.monotonic() >= deadline: raise RuntimeError('ready compiler was blocked by a long Run step')
        time.sleep(0.01)
""",
        )
        self.write(
            "build.zig",
            """const std = @import("std");
fn gate(b: *std.Build, mode: []const u8) *std.Build.Step.Run {
    const run = b.addSystemCommand(&.{ "python3", "gate.py", mode });
    run.expectExitCode(0);
    run.has_side_effects = true;
    _ = run.captureStdOut(.{});
    return run;
}
pub fn build(b: *std.Build) void {
    const hold = gate(b, "hold");
    const ready = gate(b, "ready");
    const object = b.addObject(.{ .name = "overlap-probe", .root_module = b.createModule(.{
        .root_source_file = b.path("probe.zig"), .target = b.graph.host, .optimize = .debug,
    }) });
    object.step.dependOn(&ready.step);
    const release = gate(b, "release");
    release.step.dependOn(&object.step);
    const step = b.step("test", "Prove compilation overlaps a running test");
    step.dependOn(&hold.step);
    step.dependOn(&release.step);
}
""",
        )
        self.build()
        self.assertEqual((self.root / "released").read_text(), "done")

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

    def test_existing_owner_audit_keeps_local_inventory_before_runtime_args(self):
        options = ("-Downer-audit=true",)
        for name in ("local owned", "server owned"):
            with self.subTest(owner=name):
                output = self.build("--test-filter", name, build_options=options)
                self.assertIn(name + "...", output)
        output = self.build(
            "--test-filter", "missing", build_options=options, succeeds=False
        )
        self.assertIn("test filter matched no declared tests", output)

    def test_aggregate_cloning_retains_local_owner_exclusions(self):
        output = self.build(build_options=("-Daggregate=true",))
        self.assertIn("server owned...", output)
        self.assertNotIn("local owned...", output)

    def test_control_profile_ignores_inactive_physical_imports(self):
        catalog = self.root / "pkg/antfly-embedded/src/source_catalog.zig"
        catalog.write_text(
            catalog.read_text()
            + '\n pub const physical = @import("physical.zig");\n'
            + 'pub const storage_db_generation_lifecycle = @import("lifecycle.zig");\n'
        )
        self.write(
            "pkg/antfly-embedded/src/physical.zig",
            """const DB = @import("antfly_source_root").antfly_sources.physical_db.DB;
test "physical owned" { _ = DB; }
""",
        )
        self.write(
            "pkg/antfly-embedded/src/lifecycle.zig",
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
            "pkg/antfly-embedded/src/source_catalog_control.zig",
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
