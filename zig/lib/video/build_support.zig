// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    const filter = b.option([]const u8, "video-test-filter", "Run video tests matching this substring");
    const tests = b.addTest(.{ .use_llvm = b.option(bool, "video-test-llvm", "Use LLVM for video tests"), .root_module = module, .filters = if (filter) |value| &.{value} else &.{} });
    const run_tests = b.addRunArtifact(tests);
    // Platform decoder/device availability can change independently of code.
    run_tests.has_side_effects = true;
    b.step("test-video", "Run video selection, decode, surface and preparation tests").dependOn(&run_tests.step);
    b.step("check-video", "Compile video tests without executing them").dependOn(&tests.step);
    b.step("install-video-tests", "Install portable qualification tests for execution on another host").dependOn(&b.addInstallArtifact(tests, .{ .dest_sub_path = "video-tests" }).step);
    const fuzz_module = b.createModule(.{ .root_source_file = root.path(b, "video_fuzz_root.zig"), .target = target, .optimize = optimize });
    fuzz_module.addImport("antfly_media", b.createModule(.{ .root_source_file = root.path(b, "../media/src/mod.zig"), .target = target, .optimize = optimize }));
    fuzz_module.addImport("antfly_image", b.createModule(.{ .root_source_file = root.path(b, "../image/src/mod.zig"), .target = target, .optimize = optimize }));
    const fuzz_filter = b.option([]const u8, "video-fuzz-filter", "Run fuzz targets matching substring");
    const fuzz_tests = b.addTest(.{ .root_module = fuzz_module, .use_llvm = true, .filters = if (fuzz_filter) |value| &.{value} else &.{} });
    b.step("fuzz-video", "Run portable H.264/VP9/AV1 coverage-guided fuzz targets without platform shims").dependOn(&b.addRunArtifact(fuzz_tests).step);
    addImportCheck(b, root, target, optimize);
    const qualification_module = b.createModule(.{ .root_source_file = root.path(b, "video_qualification_root.zig"), .target = target, .optimize = optimize });
    configure(b, qualification_module, root, target, null);
    const qualification = b.addExecutable(.{ .name = "video-qualification", .root_module = qualification_module });
    b.getInstallStep().dependOn(&b.addInstallArtifact(qualification, .{}).step);
    const run = b.addRunArtifact(qualification);
    run.addArg(b.option([]const u8, "qualify-mode", "hash, dump, dump-rgb (MJPEG), or mutate") orelse "hash");
    run.addArg(b.option([]const u8, "qualify-input", "MP4 corpus file") orelse "testdata/h264-sintel-original.mp4");
    run.addArg(b.option([]const u8, "qualify-count", "Number of deterministic mutations") orelse "1000");
    b.step("qualify-video", "Hash native decoded samples or run bounded corpus mutations").dependOn(&run.step);
    b.step("check-video-qualification", "Compile external-corpus qualification tool").dependOn(&qualification.step);
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
    const executable = b.addExecutable(.{ .name = "video-benchmark", .root_module = module, .use_llvm = b.option(bool, "video-use-llvm", "Use LLVM for video tests and benchmarks") });
    const run = b.addRunArtifact(executable);
    if (b.option([]const u8, "benchmark-input", "MP4/MOV file for the video benchmark")) |path| run.addArg(path) else run.addArg("-");
    run.addArg(b.option([]const u8, "benchmark-backend", "auto, cpu or cpu-copy") orelse "auto");
    run.addArg(b.option([]const u8, "benchmark-iterations", "Completed samples including cold (2..100)") orelse "21");
    run.addArg(b.option([]const u8, "benchmark-size", "Square preparation size in pixels") orelse "48");
    b.step("bench-video", "Measure completed decode/preparation latency, GPU time and memory").dependOn(&run.step);
    b.step("check-video-benchmark", "Compile the native video benchmark").dependOn(&executable.step);
    b.step("install-video-benchmark", "Install completed video benchmarking for external execution").dependOn(&b.addInstallArtifact(executable, .{}).step);
    const live_source = b.option([]const u8, "live-benchmark-video-source", "Alternate maintained video module root for source-pinned coordinator comparison");
    const live_core = b.createModule(.{ .root_source_file = if (live_source) |source| .{ .cwd_relative = source } else root.path(b, "src/mod.zig"), .target = target, .optimize = optimize });
    configure(b, live_core, root, target, null);
    const live_root = b.createModule(.{ .root_source_file = root.path(b, "src/live_coordinator_benchmark.zig"), .target = target, .optimize = optimize, .link_libc = true });
    live_root.addImport("antfly_video", live_core);
    live_root.addImport("antfly_media", live_core.import_table.get("antfly_media").?);
    const live_exe = b.addExecutable(.{ .name = "live-coordinator-benchmark", .root_module = live_root, .use_llvm = executable.use_llvm });
    b.step("install-live-coordinator-benchmark", "Install completed live validation/window benchmark").dependOn(&b.addInstallArtifact(live_exe, .{}).step);

    const webm_source = b.option([]const u8, "webm-benchmark-root-source", "Alternate source-pinned benchmark root for historical decoder comparisons");
    const webm_module = b.createModule(.{ .root_source_file = if (webm_source) |source| .{ .cwd_relative = source } else root.path(b, "webm_benchmark_root.zig"), .target = target, .optimize = optimize, .link_libc = true });
    configure(b, webm_module, root, target, null);
    // Benchmark-only source substitution permits matched experiments without
    // changing production decoder policies or adding runtime codec dependencies.
    if (b.option([]const u8, "webm-benchmark-image-source", "Alternate maintained image module root for a source-pinned comparison")) |source| {
        webm_module.addImport("antfly_image", b.createModule(.{ .root_source_file = .{ .cwd_relative = source }, .target = target, .optimize = optimize }));
    }
    webm_module.addCSourceFile(.{ .file = root.path(b, "src/backends/benchmark_rss.c"), .flags = &.{ "-Wall", "-Wextra", "-Werror" } });
    if (target.result.os.tag == .macos) webm_module.addCSourceFile(.{ .file = root.path(b, "src/backends/benchmark_device.m"), .flags = &.{ "-fobjc-arc", "-Wall", "-Wextra", "-Werror" } });
    const webm_executable = b.addExecutable(.{ .name = "webm-benchmark", .root_module = webm_module, .use_llvm = executable.use_llvm });
    const webm_run = b.addRunArtifact(webm_executable);
    webm_run.addArg(b.option([]const u8, "webm-benchmark-input", "VP8/VP9/AV1 WebM input") orelse "testdata/vp8-sintel.webm");
    webm_run.addArg(b.option([]const u8, "webm-benchmark-backend", "decode, cpu, metal or hash") orelse "decode");
    webm_run.addArg(b.option([]const u8, "webm-benchmark-iterations", "Completed samples (2..100); hash uses one pass") orelse "7");
    webm_run.addArg(b.option([]const u8, "webm-benchmark-size", "Square preparation size") orelse "384");
    webm_run.addArg(b.option([]const u8, "webm-benchmark-step", "Prepare every Nth visible picture") orelse "24");
    b.step("bench-webm", "Measure completed native VP8/VP9/AV1 decode and preparation").dependOn(&webm_run.step);
    b.step("install-webm-benchmark", "Install the WebM benchmark for external corpus or Linux execution").dependOn(&b.addInstallArtifact(webm_executable, .{}).step);
}
