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

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    defer @import("antfly_platform").bindBuild(b);

    _ = b.addModule("antfly-json", .{
        .root_source_file = b.path("src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const consumer_test = b.addSystemCommand(&.{"python3"});
    consumer_test.addFileArg(b.path("tests/test_standalone_consumer.py"));
    consumer_test.addArg(b.graph.zig_exe);
    b.step("test-standalone-consumer", "Compile libc-free Linux and freestanding WASM consumers")
        .dependOn(&consumer_test.step);
}
