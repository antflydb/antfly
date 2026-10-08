// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
const std = @import("std");
pub fn build(b: *std.Build) void {
    const tests = b.addTest(.{ .root_module = createModule(b, b.standardTargetOptions(.{}), b.standardOptimizeOption(.{}), b.path(".")) });
    b.step("test", "Run SQL regex portability and admission tests").dependOn(&b.addRunArtifact(tests).step);
    b.step("check", "Compile SQL regex portability contracts without executing them").dependOn(&tests.step);
}

pub fn createModule(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, root: std.Build.LazyPath) *std.Build.Module {
    const module = b.createModule(.{ .root_source_file = root.path(b, "src/mod.zig"), .target = target, .optimize = optimize, .link_libc = true });
    module.addIncludePath(root.path(b, "vendor"));
    for ([_][]const u8{ "vendor/regcomp.c", "vendor/regexec.c", "vendor/regfree.c", "port/bridge.c" }) |file| module.addCSourceFile(.{ .file = root.path(b, file), .flags = &.{ "-std=c11", "-DNDEBUG", "-Wall", "-Wextra", "-Wno-unused-parameter", "-Wno-sign-compare", "-Werror" } });
    return module;
}
