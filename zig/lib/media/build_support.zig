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
/// Attach the shared portable container module, inheriting the consumer target.
pub fn attach(b: *std.Build, consumer: *std.Build.Module, root: std.Build.LazyPath) void {
    consumer.addImport("antfly_media", b.createModule(.{
        .root_source_file = root.path(b, "src/mod.zig"),
        .target = consumer.resolved_target,
        .optimize = consumer.optimize,
        .single_threaded = consumer.single_threaded,
    }));
}

pub fn addTests(b: *std.Build, root: std.Build.LazyPath, target: std.Build.ResolvedTarget, optimize: std.builtin.Optimize) void {
    const tests = b.addTest(.{ .root_module = b.createModule(.{ .root_source_file = root.path(b, "media_test_root.zig"), .target = target, .optimize = optimize }) });
    const run_tests = b.addRunArtifact(tests);
    run_tests.has_side_effects = true;
    b.step("test-media", "Run shared container, source and timeline tests").dependOn(&run_tests.step);
    b.step("install-media-tests", "Install portable container/source qualification for another host").dependOn(&b.addInstallArtifact(tests, .{ .dest_sub_path = "media-tests" }).step);
    b.step("check-media", "Compile shared media tests without executing them").dependOn(&tests.step);
}
