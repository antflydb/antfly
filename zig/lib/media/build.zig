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
    _ = b.addModule("antfly_media", .{ .root_source_file = b.path("src/mod.zig"), .target = target, .optimize = optimize });
    const objectstore = b.dependency("objectstore", .{ .target = target, .optimize = optimize });
    const integration = b.createModule(.{ .root_source_file = b.path("objectstore_test_root.zig"), .target = target, .optimize = optimize });
    integration.addImport("objectstore", objectstore.module("objectstore"));
    integration.addImport("httpx", objectstore.module("objectstore").import_table.get("httpx").?);
    integration.addImport("antfly_media", b.modules.get("antfly_media").?);
    const tests = b.addTest(.{ .root_module = integration });
    b.step("test-media-objectstore", "Run authenticated object-store range tests").dependOn(&b.addRunArtifact(tests).step);
    b.step("check-media-objectstore", "Compile authenticated object-store range tests").dependOn(&tests.step);
    const http_integration = b.createModule(.{ .root_source_file = b.path("http_live_test_root.zig"), .target = target, .optimize = optimize });
    http_integration.addImport("httpx", objectstore.module("objectstore").import_table.get("httpx").?);
    http_integration.addImport("antfly_media", b.modules.get("antfly_media").?);
    const http_tests = b.addTest(.{ .root_module = http_integration });
    const run_http_tests = b.addRunArtifact(http_tests);
    run_http_tests.has_side_effects = true;
    b.step("test-media-http", "Run native HTTP and live HLS ingestion tests").dependOn(&run_http_tests.step);
    b.step("install-media-http-tests", "Install production HTTP qualification tests for another host").dependOn(&b.addInstallArtifact(http_tests, .{ .dest_sub_path = "media-http-tests" }).step);
    const fuzz_root = b.createModule(.{ .root_source_file = b.path("hls_master_fuzz_root.zig"), .target = target, .optimize = optimize });
    const fuzz_tests = b.addTest(.{ .root_module = fuzz_root, .filters = &.{"HLS master fuzz"}, .use_llvm = true });
    b.step("test-media-hls-fuzz", "Qualify bounded HLS master parsing and selection; accepts --fuzz=limit").dependOn(&b.addRunArtifact(fuzz_tests).step);
    b.step("check-media-http", "Compile native HTTP and live HLS ingestion tests").dependOn(&http_tests.step);
    const partial_fuzz_root = b.createModule(.{ .root_source_file = b.path("hls_partial_fuzz_root.zig"), .target = target, .optimize = optimize });
    const partial_fuzz_tests = b.addTest(.{ .root_module = partial_fuzz_root, .filters = &.{"HLS partial fuzz"}, .use_llvm = true });
    b.step("test-media-hls-partial-fuzz", "Qualify bounded partial HLS manifests with positively accepted seeds").dependOn(&b.addRunArtifact(partial_fuzz_tests).step);
    const delta_fuzz_tests = b.addTest(.{ .root_module = partial_fuzz_root, .filters = &.{"HLS delta fuzz"}, .use_llvm = true });
    b.step("test-media-hls-delta-fuzz", "Fuzz bounded delta HLS merging with positively accepted encrypted-state seeds").dependOn(&b.addRunArtifact(delta_fuzz_tests).step);
    const dates_fuzz_tests = b.addTest(.{ .root_module = partial_fuzz_root, .filters = &.{"HLS date-range fuzz"}, .use_llvm = true });
    b.step("test-media-hls-daterange-fuzz", "Fuzz immutable date-range deltas and shared admission cleanup").dependOn(&b.addRunArtifact(dates_fuzz_tests).step);
    const aes_fuzz_root = b.createModule(.{ .root_source_file = b.path("hls_aes_fuzz_root.zig"), .target = target, .optimize = optimize });
    const aes_fuzz_tests = b.addTest(.{ .root_module = aes_fuzz_root, .filters = &.{"HLS AES fuzz"}, .use_llvm = true });
    b.step("test-media-hls-aes-fuzz", "Qualify HLS AES parsing/decryption; accepts --fuzz=limit").dependOn(&b.addRunArtifact(aes_fuzz_tests).step);
    const declaration_fuzz = b.addTest(.{ .root_module = aes_fuzz_root, .filters = &.{"HLS AES fuzz media"}, .use_llvm = true });
    b.step("test-media-hls-aes-declaration-fuzz", "Fuzz HLS AES declarations with an independent report").dependOn(&b.addRunArtifact(declaration_fuzz).step);
    const cipher_fuzz = b.addTest(.{ .root_module = aes_fuzz_root, .filters = &.{ "HLS AES fuzz CBC", "HLS AES fuzz cipher seed" }, .use_llvm = true });
    b.step("test-media-hls-aes-cipher-fuzz", "Fuzz AES ciphertext with an independent report").dependOn(&b.addRunArtifact(cipher_fuzz).step);
    const transport_fuzz_root = b.createModule(.{ .root_source_file = b.path("transport_fuzz_root.zig"), .target = target, .optimize = optimize });
    const transport_fuzz = b.addTest(.{ .root_module = transport_fuzz_root, .filters = &.{"transport fuzz"}, .use_llvm = true });
    b.step("test-media-transport-fuzz", "Fuzz bounded native TS extraction with positive AVC seeds").dependOn(&b.addRunArtifact(transport_fuzz).step);
    support.addTests(b, b.path("."), target, optimize);
}
