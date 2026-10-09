// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
const std = @import("std");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const module = createModule(b, target, optimize, b.path("."));
    if (target.result.cpu.arch.isWasm() and target.result.os.tag == .freestanding) {
        module.root_source_file = b.path("src/wasm_smoke.zig");
        const smoke = b.addExecutable(.{ .name = "sql-regex-smoke", .root_module = module });
        smoke.entry = .disabled;
        smoke.rdynamic = true;
        b.step("check", "Compile the freestanding SQL regex backend").dependOn(&smoke.step);
        const run = b.addSystemCommand(&.{"node"});
        run.addFileArg(b.path("wasm_smoke.mjs"));
        run.addArtifactArg(smoke);
        b.step("test", "Execute independent PostgreSQL contracts in freestanding WASM").dependOn(&run.step);
        return;
    }
    const tests = b.addTest(.{ .root_module = module });
    b.step("test", "Run SQL regex portability and admission tests").dependOn(&b.addRunArtifact(tests).step);
    b.step("check", "Compile SQL regex portability contracts without executing them").dependOn(&tests.step);
}

pub fn createModule(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, root: std.Build.LazyPath) *std.Build.Module {
    const module = b.createModule(.{ .root_source_file = root.path(b, "src/mod.zig"), .target = target, .optimize = optimize, .link_libc = false, .single_threaded = if (target.result.cpu.arch.isWasm()) true else null });
    module.addImport("antfly_capture_regex", b.createModule(.{ .root_source_file = root.path(b, "../regex/src/captures.zig"), .target = target, .optimize = optimize, .link_libc = false }));
    return module;
}
