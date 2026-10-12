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
}
