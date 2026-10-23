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

//! Audit real stripped inference declarations without compiling model bodies.
const std = @import("std");
const project = @import("project_build.zig");
const profiles = @import("cache_profiles.zig");

pub fn build(b: *std.Build) void {
    if (comptime @hasDecl(project, "create")) {
        _ = project.create(b) orelse return;
    } else {
        project.build(b);
    }
    var steps = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    var modules = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
    for (b.top_level_steps.values()) |top| profiles.collectSteps(&top.step, &steps, &modules);
    var iterator = steps.keyIterator();
    var cli_found = false;
    var http_found = false;
    const gib = 1024 * 1024 * 1024;
    while (iterator.next()) |entry| {
        const artifact = entry.*.cast(std.Build.Step.Compile) orelse continue;
        if (std.mem.eql(u8, artifact.name, "antfly-inference-dev")) {
            cli_found = true;
            if (!(artifact.root_module.strip orelse false)) @panic("inference CLI is not stripped");
            // A stripped CLI must not inherit the heavier unstripped profile.
            if (artifact.step.max_rss == 0 or artifact.step.max_rss > 7 * @as(usize, gib))
                @panic("stripped inference CLI over-reserved");
        }
        if (std.mem.eql(u8, artifact.name, "antfly-inference-http-server")) {
            http_found = true;
            if (!(artifact.root_module.strip orelse false)) @panic("inference HTTP server is not stripped");
            const host = b.graph.host.result;
            const target = artifact.root_module.resolved_target.?.result;
            if (host.os.tag == .linux and host.cpu.arch == .x86_64 and
                target.os.tag == .linux and target.cpu.arch == .x86_64 and target.abi == .gnu)
            {
                // Preserve capacity for two 8 GiB compiles in the 22 GiB budget.
                if (artifact.step.max_rss == 0 or artifact.step.max_rss > 4 * @as(usize, gib))
                    @panic("native stripped HTTP server over-reserved");
            }
        }
    }
    if (!cli_found or !http_found) @panic("missing inference executable declarations");
    _ = b.step("cache-inference-memory", "Audit actual stripped inference memory admission");
}
