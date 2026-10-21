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
pub const support = @import("build_support.zig");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    _ = support.addModule(b, b.path("."), target, optimize);
    support.addBenchmarks(b, b.path("."), target, optimize);
    support.addTests(b, b.path("."), target, optimize);
    const live_root = b.createModule(.{ .root_source_file = b.path("live_window_test_root.zig"), .target = target, .optimize = optimize });
    live_root.addImport("antfly_media", b.modules.get("antfly_video").?.import_table.get("antfly_media").?);
    live_root.addImport("antfly_image", b.modules.get("antfly_video").?.import_table.get("antfly_image").?);
    const live_tests = b.addTest(.{ .root_module = live_root, .filters = &.{ "live packet window", "live coordinator", "source timestamp sampling", "streamed live" } });
    const run_live_tests = b.addRunArtifact(live_tests);
    run_live_tests.has_side_effects = true;
    b.step("test-live-windows", "Qualify owned HLS/DASH packet windows and shared admission").dependOn(&run_live_tests.step);
    b.step("install-live-window-tests", "Install live window ownership tests for another host").dependOn(&b.addInstallArtifact(live_tests, .{ .dest_sub_path = "window-tests" }).step);
    const transport_fuzz_root = b.createModule(.{ .root_source_file = b.path("transport_fuzz_root.zig"), .target = target, .optimize = optimize });
    transport_fuzz_root.addImport("antfly_media", b.modules.get("antfly_video").?.import_table.get("antfly_media").?);
    transport_fuzz_root.addImport("antfly_image", b.modules.get("antfly_video").?.import_table.get("antfly_image").?);
    const transport_fuzz = b.addTest(.{ .root_module = transport_fuzz_root, .filters = &.{"TS normalization fuzz"}, .use_llvm = true });
    b.step("test-transport-fuzz", "Fuzz TS access-unit normalization and shared admission").dependOn(&b.addRunArtifact(transport_fuzz).step);
    const http = b.dependency("httpx", .{ .target = target, .optimize = optimize });
    const integration = b.createModule(.{ .root_source_file = b.path("transport_http_test_root.zig"), .target = target, .optimize = optimize });
    integration.addImport("httpx", http.module("httpx"));
    integration.addImport("antfly_video", b.modules.get("antfly_video").?);
    integration.addImport("antfly_media", b.modules.get("antfly_video").?.import_table.get("antfly_media").?);
    const tests = b.addTest(.{ .root_module = integration });
    const run = b.addRunArtifact(tests);
    run.has_side_effects = true;
    b.step("test-transport-http", "Qualify authenticated TS HLS packet/decoder integration").dependOn(&run.step);
    b.step("install-transport-http-tests", "Install TS HTTP qualification for another host").dependOn(&b.addInstallArtifact(tests, .{ .dest_sub_path = "transport-http-tests" }).step);
    const sampling_root = b.createModule(.{ .root_source_file = b.path("sampling_benchmark.zig"), .target = target, .optimize = optimize });
    sampling_root.addImport("antfly_media", b.modules.get("antfly_video").?.import_table.get("antfly_media").?);
    const sampling_bench = b.addExecutable(.{ .name = "video-sampling-benchmark", .root_module = sampling_root });
    b.step("bench-sampling", "Compare immutable timeline lookup with the linear sampling oracle").dependOn(&b.addRunArtifact(sampling_bench).step);
    const index_root = b.createModule(.{ .root_source_file = b.path("picture_index_benchmark.zig"), .target = target, .optimize = optimize });
    index_root.addImport("antfly_media", b.modules.get("antfly_video").?.import_table.get("antfly_media").?);
    const index_bench = b.addExecutable(.{ .name = "video-picture-index-benchmark", .root_module = index_root });
    b.step("bench-picture-index", "Compare transport header assembly with reconstructed AVC picture indexing").dependOn(&b.addRunArtifact(index_bench).step);
    const index_tests_root = b.createModule(.{ .root_source_file = b.path("picture_index_test_root.zig"), .target = target, .optimize = optimize });
    index_tests_root.addImport("antfly_media", b.modules.get("antfly_video").?.import_table.get("antfly_media").?);
    const index_tests = b.addTest(.{ .root_module = index_tests_root, .filters = &.{"AVC header"} });
    b.step("test-picture-index", "Differential AVC transport index qualification across maintained fixtures").dependOn(&b.addRunArtifact(index_tests).step);
    b.step("install-picture-index-tests", "Install portable AVC index qualification for another host").dependOn(&b.addInstallArtifact(index_tests, .{ .dest_sub_path = "picture-index-tests" }).step);
}
