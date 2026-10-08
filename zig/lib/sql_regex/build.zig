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
    module.addIncludePath(root.path(b, "vendor"));
    const common_flags: []const []const u8 = &.{ "-std=c11", "-ffreestanding", "-fno-builtin", "-DNDEBUG", "-Wall", "-Wextra", "-Wno-unused-parameter", "-Wno-sign-compare", "-Werror" };
    const flags = if (target.result.cpu.arch.isWasm()) b.allocator.dupe([]const u8, common_flags ++ &[_][]const u8{"-DANTFLY_REGEX_SINGLE_THREADED=1"}) catch @panic("OOM") else common_flags;
    for ([_][]const u8{ "vendor/regcomp.c", "vendor/regexec.c", "vendor/regfree.c", "port/bridge.c", "port/memory.c" }) |file| module.addCSourceFile(.{ .file = root.path(b, file), .flags = flags });
    return module;
}
