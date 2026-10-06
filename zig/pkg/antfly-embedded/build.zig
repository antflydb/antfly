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

//! Public standalone package entry point. Release source bundles relocate the
//! composition dependency into the package; checkout builds use the shared tree.
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const composition = b.dependency("composition", .{
        .@"embedded-only" = true,
        .target = target,
        .optimize = optimize,
        .metal = b.option(bool, "metal", "Enable Metal") orelse false,
        .onnx = b.option(bool, "onnx", "Enable ONNX Runtime") orelse false,
        .cuda = b.option(bool, "cuda", "Enable CUDA") orelse false,
        .@"wasm-memory-model" = b.option([]const u8, "wasm-memory-model", "Inference browser memory model: wasm32 or wasm64") orelse "wasm32",
        .webgpu = b.option(bool, "webgpu", "Enable browser inference WebGPU") orelse false,
        .@"wasm-strip" = b.option(bool, "wasm-strip", "Strip embedded WASM debug information") orelse false,
        .pjrt = b.option(bool, "pjrt", "Enable PJRT") orelse false,
        .@"antfly-version" = b.option([]const u8, "antfly-version", "Antfly version") orelse @import("build.zig.zon").version,
    });
    for ([_][]const u8{ "antfly-embedded", "antfly-inference" }) |name| {
        b.modules.put(b.allocator, name, composition.module(name)) catch @panic("OOM");
    }
    for ([_][]const u8{ "lite", "capi", "capi-smoke", "embedded-package-test", "embedded-lake-test", "wasm", "wasm-test", "inference-wasm", "pkgconfig" }) |name| {
        const step = composition.builder.top_level_steps.get(name) orelse @panic("missing composition step");
        b.step(name, step.description).dependOn(&step.step);
    }
    for ([_][]const u8{ "antfly", "antfly-lite", "antfly_wasm" }) |name| {
        b.installArtifact(composition.artifact(name));
    }
    // Importing a dependency never schedules its default step. Standalone
    // builds default to the native products; browser builds are explicit.
    b.default_step = &b.top_level_steps.get("lite").?.step;
}
