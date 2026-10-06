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
const project = @import("project_build.zig");

pub fn build(b: *std.Build) void {
    _ = project.create(b);
    _ = b.step("cache-vopr-memory", "Check VOPR workflow admission claims without compiling production");
    var visited = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    for ([_][]const u8{
        "antfly-raft-transport-test",  "standby-vopr-test",                "vopr-runtime-test",
        "restore-admission-vopr-test", "vopr-determinism-audit",           "vopr-build",
        "antfly",                      "antfly-storage-owner-source-test",
    }) |name| {
        check(&b.top_level_steps.get(name).?.step, &visited);
    }
}

fn check(step: *std.Build.Step, visited: *std.AutoHashMap(*std.Build.Step, void)) void {
    if ((visited.getOrPut(step) catch @panic("OOM")).found_existing) return;
    if (step.cast(std.Build.Step.Compile)) |artifact| {
        if (std.mem.eql(u8, artifact.name, "vopr")) {
            if (artifact.filters.len != 1 or !std.mem.eql(u8, artifact.filters[0], "VOPR command entrypoint"))
                @panic("VOPR command build includes unrelated unit tests");
        }
    }
    if (step.tag == .compile or step.tag == .run) {
        if (step.max_rss == 0) std.debug.panic("unbudgeted VOPR work: {s}", .{step.name});
        if (step.max_rss > 22 * 1024 * 1024 * 1024)
            std.debug.panic("VOPR work exceeds workflow memory cap: {s}", .{step.name});
    }
    for (step.dependencies.items) |dependency| check(dependency, visited);
}
