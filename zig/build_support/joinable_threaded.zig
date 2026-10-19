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

pub fn prepare(b: *std.Build) std.Build.LazyPath {
    const run = b.addSystemCommand(&.{"python3"});
    run.step.max_rss = 256 * 1024 * 1024;
    run.addFileArg(b.path("tools/prepare_joinable_threaded.py"));
    run.addDirectoryArg(std.Build.LazyPath.zig_lib);
    run.addFileArg(std.Build.LazyPath.zig_lib.path(b, "std/Io/Threaded.zig"));
    run.addFileArg(std.Build.LazyPath.zig_lib.path(b, "std/Thread.zig"));
    return run.addOutputDirectoryArg("joinable-zig-lib");
}

pub fn attach(b: *std.Build, library: std.Build.LazyPath) void {
    var visited = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    defer visited.deinit();
    for (b.top_level_steps.values()) |root| attachStep(&root.step, library, &visited);
}

fn attachStep(step: *std.Build.Step, library: std.Build.LazyPath, visited: *std.AutoHashMap(*std.Build.Step, void)) void {
    if ((visited.getOrPut(step) catch @panic("OOM")).found_existing) return;
    if (step.cast(std.Build.Step.Compile)) |compile| {
        // Chase module-linked archives too: no producer may retain the stock
        // layout while a consumer is built against the generated std library.
        for (compile.getCompileDependencies(true)) |dependency| {
            dependency.zig_lib_dir = library;
            library.addStepDependencies(&dependency.step);
            if (dependency != compile) attachStep(&dependency.step, library, visited);
        }
    }
    for (step.dependencies.items) |dependency| attachStep(dependency, library, visited);
}
