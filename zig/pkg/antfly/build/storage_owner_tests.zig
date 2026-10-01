// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2

const std = @import("std");
const AntflyRootImports = @import("imports.zig").AntflyRootImports;
const runtime = @import("runtime.zig");

/// These sources require separately linked owner suites, not source shards.
pub const test_sources = [_][]const u8{
    "kernel_owner_test.zig",
    "kernel_owner_provisioned_source_test.zig",
    "enrichment_compute_test.zig",
    "kernel_owner_handoff_reopen_test.zig",
};

/// Exercise the same storage archive linked by serving and embedded consumers.
/// Root composition attaches these runs to the owning test aggregates.
pub const Result = struct { runs: [3]*std.Build.Step.Run, benchmark: *std.Build.Step.Compile };

pub fn add(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    imports: AntflyRootImports,
    vopr: *std.Build.Module,
    lmdb_engine: *std.Build.Module,
    artifacts: [std.meta.fields(runtime.RuntimeLibraryUnit).len]?*std.Build.Step.Compile,
) Result {
    const owner_filter = b.option([]const u8, "storage-owner-test-filter", "Compile and run one matching storage owner test subset");
    const source_filter = b.option([]const u8, "storage-owner-source-test-filter", "Compile and run matching provisioned storage owner tests");
    const test_metadata = @import("../../../lib/build_info/build_support.zig").create(b, .{
        .root = b.path("lib/build_info"),
        .target = target,
        .optimize = optimize,
        .version = "test",
    });
    var runs: [3]*std.Build.Step.Run = undefined;
    var benchmark: *std.Build.Step.Compile = undefined;
    inline for (.{ "storage_kernel_owner_test_root.zig", "storage_kernel_provisioned_source_test_root.zig", "enrichment_compute_test_root.zig", "storage_boundary_bench.zig" }, 0..) |root, index| {
        const module = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/" ++ root),
            .target = target,
            .optimize = optimize,
        });
        if (index == 0) module.addImport("vopr", vopr);
        var owner_imports = imports;
        owner_imports.boundary_profile = if (index == 2) .enrichment else .owner;
        if (index == 2)
            owner_imports.configureEnrichment(b, module, true)
        else
            owner_imports.configureStorage(b, module, true);
        owner_imports.storage_boundary.configureProfile(module, true, true, owner_imports.boundary_profile);
        if (comptime index < 3) {
            const tests = @import("linked_tests.zig").add(b, .{
                .name = if (index == 0) "storage-owner-tests" else if (index == 1) "storage-owner-source-tests" else "storage-owner-enrichment-tests",
                .root_module = module,
                .filters = if (index == 0 and owner_filter != null)
                    &.{owner_filter.?}
                else if (index == 1 and source_filter != null)
                    &.{source_filter.?}
                else if (index == 1)
                    &.{
                        b.fmt("storage.{s}.", .{std.fs.path.stem(test_sources[index])}),
                        "compiled structural reconciliation publishes",
                        "api.kernel_owner_source.",
                        "compiled startup catch-up retains exact retries",
                        "compiled owner coordinated ttl admission",
                        "source owner deadlines normalize executor clock epochs",
                        "distributed txn native lookup read-index rejects leader loss before storage execution",
                    }
                else
                    &.{b.fmt("storage.{s}.", .{std.fs.path.stem(test_sources[index])})},
                .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
            });
            tests.executable.root_module.addObject(test_metadata.object);
            runs[index] = tests.run(b);
            if (index == 2) {
                tests.executable.root_module.linkLibrary(artifacts[@intFromEnum(runtime.RuntimeLibraryUnit.enrichment_compute)].?);
            } else {
                inline for (.{ .storage_kernel, .enrichment_compute, .inference }) |unit|
                    tests.executable.root_module.linkLibrary(artifacts[@intFromEnum(@as(runtime.RuntimeLibraryUnit, unit))].?);
            }
        } else {
            test_metadata.link(module);
            benchmark = b.addExecutable(.{ .name = "storage_boundary_bench", .root_module = module });
        }
        if (index == 3) {
            inline for (.{ .storage_kernel, .enrichment_compute, .inference }) |unit|
                module.linkLibrary(artifacts[@intFromEnum(@as(runtime.RuntimeLibraryUnit, unit))].?);
        }
    }
    const physical_module = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/storage_kernel_owner_physical_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    physical_module.addImport("vopr", vopr);
    var physical_imports = imports;
    physical_imports.boundary_profile = .owner;
    physical_imports.configureStorage(b, physical_module, true);
    @import("storage.zig").configureLmdb(b, physical_module, lmdb_engine, true);
    physical_imports.storage_boundary.configureProfile(physical_module, true, true, .owner);
    const handoff_tests = @import("linked_tests.zig").add(b, .{
        .name = "storage-owner-handoff-reopen-tests",
        .root_module = physical_module,
        .filters = &.{"storage owner handoff receipt survives shared-context hidden to public reopen"},
        .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
    });
    handoff_tests.executable.root_module.addObject(test_metadata.object);
    inline for (.{ .storage_kernel, .enrichment_compute, .inference }) |unit|
        handoff_tests.executable.root_module.linkLibrary(artifacts[@intFromEnum(@as(runtime.RuntimeLibraryUnit, unit))].?);
    b.step("antfly-storage-owner-handoff-reopen-test", "Run physical shared-context owner handoff receipt reopen regression")
        .dependOn(&handoff_tests.run(b).step);
    return .{ .runs = runs, .benchmark = benchmark };
}
