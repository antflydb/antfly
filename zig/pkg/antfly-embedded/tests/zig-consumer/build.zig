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

//! Consumer of the fetched package, with no monorepo path dependencies.
const std = @import("std");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const dependency = b.dependency("embedded", .{ .target = target, .optimize = optimize });
    const tests = b.addTest(.{ .root_module = b.createModule(.{ .root_source_file = b.path("main.zig"), .target = target, .optimize = optimize }) });
    tests.root_module.addImport("antfly-embedded", dependency.module("antfly-embedded"));
    tests.root_module.addImport("antfly-inference", dependency.module("antfly-inference"));
    tests.root_module.link_libc = true;
    b.step("test", "Exercise fetched Apache package").dependOn(&b.addRunArtifact(tests).step);
    const native = b.step("native", "Build fetched C ABI, header and install metadata");
    native.dependOn(&dependency.builder.top_level_steps.get("capi").?.step);
    native.dependOn(&dependency.builder.top_level_steps.get("pkgconfig").?.step);
    const browser = b.addInstallArtifact(dependency.artifact("antfly_wasm"), .{});
    b.step("wasm", "Install fetched browser product").dependOn(&browser.step);
    b.step("inference-wasm32", "Build fetched inference wasm32").dependOn(&dependency.builder.top_level_steps.get("inference-wasm").?.step);
    const memory64 = b.dependency("embedded", .{
        .target = target,
        .optimize = optimize,
        .@"wasm-memory-model" = "wasm64",
    });
    b.step("inference-wasm64", "Build fetched inference wasm64").dependOn(&memory64.builder.top_level_steps.get("inference-wasm").?.step);
}
