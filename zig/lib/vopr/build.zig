// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const module = b.addModule("vopr", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const unit_tests = b.addTest(.{ .root_module = module });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run VOPR deterministic simulation and replay tests");
    test_step.dependOn(&run_unit_tests.step);

    const benchmark_module = b.createModule(.{
        .root_source_file = b.path("src/benchmark_main.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    benchmark_module.addImport("vopr", module);
    const benchmark_exe = b.addExecutable(.{ .name = "vopr-benchmark", .root_module = benchmark_module });
    const benchmark_step = b.step("benchmark", "Run deterministic VOPR search-efficiency benchmarks");
    benchmark_step.dependOn(&b.addRunArtifact(benchmark_exe).step);
}
