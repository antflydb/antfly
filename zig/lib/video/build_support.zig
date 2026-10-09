// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
pub const SharedModules = struct { media: *std.Build.Module, image: *std.Build.Module };
fn configure(b: *std.Build, module: *std.Build.Module, root: std.Build.LazyPath, target: std.Build.ResolvedTarget, shared: ?SharedModules) void {
    if (shared) |existing| {
        module.addImport("antfly_media", existing.media);
    } else {
        @import("antfly_media").support.attach(b, module, root.path(b, "../media"));
    }
    module.addImport("antfly_image", if (shared) |existing| existing.image else b.createModule(.{
        .root_source_file = root.path(b, "../image/src/mod.zig"),
        .target = target,
        .optimize = module.optimize,
    }));
    if (target.result.os.tag != .macos) return;
    module.link_libc = true;
    @import("antfly_platform").addMacosSdkPaths(b, module, target);
    inline for (.{ "Foundation", "CoreFoundation", "CoreMedia", "CoreVideo", "VideoToolbox", "Metal" }) |name| module.linkFramework(name, .{});
    const files = b.addWriteFiles();
    const source = std.json.Stringify.valueAlloc(b.allocator, @embedFile("src/backends/prepare.metal"), .{}) catch @panic("shader serialization failed");
    _ = files.add("video_prepare_shader.h", b.fmt("#define VIDEO_PREPARE_SHADER {s}\n", .{source}));
    module.addIncludePath(files.getDirectory());
    module.addCSourceFile(.{ .file = root.path(b, "src/backends/apple_prepare.m"), .flags = &.{ "-fobjc-arc", "-Wall", "-Wextra", "-Werror" } });
    module.addCSourceFile(.{ .file = root.path(b, "src/backends/apple_video.m"), .flags = &.{ "-fobjc-arc", "-Wall", "-Wextra", "-Werror" } });
}
pub fn addModule(b: *std.Build, root: std.Build.LazyPath, target: std.Build.ResolvedTarget, optimize: std.builtin.Optimize) *std.Build.Module {
    const module = b.addModule("antfly_video", .{ .root_source_file = root.path(b, "src/mod.zig"), .target = target, .optimize = optimize });
    configure(b, module, root, target, null);
    return module;
}
/// Reuse both consumer modules so media and image/control types belong to one
/// Zig module when video is integrated alongside the existing audio pipeline.
pub fn attach(b: *std.Build, consumer: *std.Build.Module, root: std.Build.LazyPath, shared: SharedModules) void {
    const target = consumer.resolved_target.?;
    const module = b.createModule(.{ .root_source_file = root.path(b, "src/mod.zig"), .target = target, .optimize = consumer.optimize, .single_threaded = consumer.single_threaded });
    configure(b, module, root, target, shared);
    consumer.addImport("antfly_video", module);
}
pub fn addTests(b: *std.Build, root: std.Build.LazyPath, target: std.Build.ResolvedTarget, optimize: std.builtin.Optimize) void {
    const module = b.createModule(.{ .root_source_file = root.path(b, "video_test_root.zig"), .target = target, .optimize = optimize });
    configure(b, module, root, target, null);
    if (target.result.os.tag == .macos) module.addCSourceFile(.{ .file = root.path(b, "src/backends/apple_video_test.m"), .flags = &.{"-fobjc-arc"} });
    const tests = b.addTest(.{ .root_module = module });
    b.step("test-video", "Run video selection, decode, surface and preparation tests").dependOn(&b.addRunArtifact(tests).step);
    b.step("check-video", "Compile video tests without executing them").dependOn(&tests.step);
    addImportCheck(b, root, target, optimize);
}

/// Compile a consumer importing audio/media/image and video through shared
/// module identities, so the future model integration cannot duplicate files.
pub fn addImportCheck(b: *std.Build, root: std.Build.LazyPath, target: std.Build.ResolvedTarget, optimize: std.builtin.Optimize) void {
    const shared = SharedModules{
        .media = b.createModule(.{ .root_source_file = root.path(b, "../media/src/mod.zig"), .target = target, .optimize = optimize }),
        .image = b.createModule(.{ .root_source_file = root.path(b, "../image/src/mod.zig"), .target = target, .optimize = optimize }),
    };
    const consumer = b.createModule(.{ .root_source_file = root.path(b, "video_import_test_root.zig"), .target = target, .optimize = optimize });
    consumer.addImport("antfly_media", shared.media);
    consumer.addImport("antfly_image", shared.image);
    attach(b, consumer, root, shared);
    const tests = b.addTest(.{ .root_module = consumer });
    b.top_level_steps.get("check-video").?.step.dependOn(&tests.step);
}

pub fn addBenchmarks(b: *std.Build, root: std.Build.LazyPath, target: std.Build.ResolvedTarget, optimize: std.builtin.Optimize) void {
    if (target.result.os.tag != .macos and target.result.os.tag != .linux) return;
    const module = b.createModule(.{ .root_source_file = root.path(b, "video_benchmark_root.zig"), .target = target, .optimize = optimize, .link_libc = true });
    configure(b, module, root, target, null);
    module.addCSourceFile(.{ .file = root.path(b, "src/backends/benchmark_rss.c"), .flags = &.{ "-Wall", "-Wextra", "-Werror" } });
    if (target.result.os.tag == .macos) module.addCSourceFile(.{ .file = root.path(b, "src/backends/benchmark_device.m"), .flags = &.{ "-fobjc-arc", "-Wall", "-Wextra", "-Werror" } });
    const executable = b.addExecutable(.{ .name = "video-benchmark", .root_module = module });
    const run = b.addRunArtifact(executable);
    if (b.option([]const u8, "benchmark-input", "MP4/MOV file for the video benchmark")) |path| run.addArg(path);
    b.step("bench-video", "Measure completed decode/preparation latency, GPU time and memory").dependOn(&run.step);
    b.step("check-video-benchmark", "Compile the native video benchmark").dependOn(&executable.step);
}
