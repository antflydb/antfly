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
    const dependency = b.dependency("embedded", .{ .target = target, .optimize = optimize });
    const tests = b.addTest(.{ .root_module = b.createModule(.{ .root_source_file = b.path("main.zig"), .target = target, .optimize = optimize }) });
    tests.root_module.addImport("antfly-embedded", dependency.module("antfly-embedded"));
    tests.root_module.addImport("antfly-inference", dependency.module("antfly-inference"));
    tests.root_module.link_libc = true;
    b.step("test", "Exercise fetched Apache package").dependOn(&b.addRunArtifact(tests).step);
    const browser = b.addInstallArtifact(dependency.artifact("antfly_wasm"), .{});
    b.step("wasm", "Install fetched browser product").dependOn(&browser.step);
}
