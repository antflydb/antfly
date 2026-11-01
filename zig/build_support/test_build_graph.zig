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

//! Inventory the test artifacts actually reachable from a selected gate.
//! A declared artifact is not necessarily compiled by that gate.
const std = @import("std");
const Row = struct {
    name: []const u8,
    root: []const u8,
    kind: []const u8,
    optimize: []const u8,
    reservation_bytes: usize,
    filters: []const []const u8,
};

pub fn add(b: *std.Build) void {
    const selected = b.option([]const u8, "test-graph-step", "Gate to inventory without compiling tests") orelse "unit-test";
    const root = b.top_level_steps.get(selected) orelse @panic("unknown -Dtest-graph-step");
    var rows: std.ArrayList(Row) = .empty;
    var visited = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    collect(b, &root.step, &visited, &rows);
    const data = std.json.Stringify.valueAlloc(b.allocator, .{ .selected_step = selected, .artifacts = rows.items }, .{ .whitespace = .indent_2 }) catch @panic("OOM");
    const files = b.addWriteFiles();
    const install = b.addInstallFile(files.add("test-build-graph.json", data), "share/antfly/test-build-graph.json");
    b.step("test-build-graph", "Inventory reachable test compilers without building the gate").dependOn(&install.step);
}

fn collect(b: *std.Build, step: *std.Build.Step, visited: *std.AutoHashMap(*std.Build.Step, void), rows: *std.ArrayList(Row)) void {
    if ((visited.getOrPut(step) catch @panic("OOM")).found_existing) return;
    if (step.cast(std.Build.Step.Compile)) |artifact| {
        if (artifact.kind.isTest()) {
            const source = if (artifact.root_module.root_source_file) |path| @import("antfly/source_paths.zig").authored(b, path) orelse "generated" else "no-source";
            const base = @import("antfly/source_paths.zig").authored(b, b.path("")).?;
            const relative = if (std.mem.startsWith(u8, source, base)) std.mem.trimStart(u8, source[base.len..], std.fs.path.sep_str) else source;
            rows.append(b.allocator, .{
                .name = artifact.name,
                .root = relative,
                .kind = @tagName(artifact.kind),
                .optimize = if (artifact.root_module.optimize) |mode| @tagName(mode) else "inherited",
                .reservation_bytes = artifact.step.max_rss,
                .filters = artifact.filters,
            }) catch @panic("OOM");
        }
    }
    for (step.dependencies.items) |dependency| collect(b, dependency, visited, rows);
}
