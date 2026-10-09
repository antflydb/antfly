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

//! Retire compiler outputs only after every selected consumer has completed.
//! Enabled only for a disposable unit invocation; manifests are retired by CI
//! before another build can reuse this cache.
const std = @import("std");
const paths = @import("source_paths.zig");

// Match the dependencies std.Build.Serialize creates after build() returns.
// Materialize them before taking the lifetime snapshot: generated roots,
// include files, and static-path objects can also retain compiler outputs.
fn moduleDependencies(artifact: *std.Build.Step.Compile) void {
    const step = &artifact.step;
    for (artifact.root_module.getGraph().modules) |module| {
        if (module.root_source_file) |path| path.addStepDependencies(step);
        for (module.include_dirs.items) |include| switch (include) {
            .path, .path_system, .path_after, .framework_path, .framework_path_system, .embed_path => |path| path.addStepDependencies(step),
            .other_step => |other| {
                other.getEmittedIncludeTree().addStepDependencies(step);
                step.dependOn(&other.step);
            },
            .config_header_step => |other| step.dependOn(&other.step),
        };
        for (module.lib_paths.items) |path| path.addStepDependencies(step);
        for (module.rpaths.items) |rpath| switch (rpath) {
            .lazy_path => |path| path.addStepDependencies(step),
            .special => {},
        };
        for (module.link_objects.items) |link| switch (link) {
            .static_path, .assembly_file => |path| path.addStepDependencies(step),
            .other_step => |other| step.dependOn(&other.step),
            .system_lib => {},
            .c_source_file => |source| source.file.addStepDependencies(step),
            .c_source_files => |sources| sources.root.addStepDependencies(step),
            .win32_resource_file => |source| {
                source.file.addStepDependencies(step);
                for (source.include_paths) |path| path.addStepDependencies(step);
            },
        };
    }
}

fn collect(step: *std.Build.Step, steps: *std.AutoHashMap(*std.Build.Step, void)) void {
    if ((steps.getOrPut(step) catch @panic("OOM")).found_existing) return;
    if (step.cast(std.Build.Step.Compile)) |artifact| moduleDependencies(artifact);
    for (step.dependencies.items) |dependency| collect(dependency, steps);
}

fn disposable(artifact: *std.Build.Step.Compile) bool {
    return switch (artifact.kind) {
        .@"test", .test_obj, .obj => true,
        .lib => artifact.linkage == .static,
        .exe => blk: {
            if (artifact.root_module.root_source_file != null) break :blk false;
            for (artifact.root_module.link_objects.items) |link|
                if (link == .other_step and link.other_step.kind == .test_obj) break :blk true;
            break :blk false;
        },
    };
}

pub fn add(b: *std.Build, gate: *std.Build.Step) void {
    if (!(b.option(bool, "unit-test-cache-release", "Release completed unit outputs; requires a disposable cache retired before another build") orelse false)) return;
    switch (b.graph.host.result.os.tag) {
        .linux, .macos => {},
        else => @panic("unit-test-cache-release requires a Unix host"),
    }
    var steps = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    collect(gate, &steps);
    var groups = std.StringHashMap(*std.Build.Step.Run).init(b.allocator);
    var releases = std.AutoHashMap(*std.Build.Step.Compile, *std.Build.Step.Run).init(b.allocator);
    var iterator = steps.keyIterator();
    while (iterator.next()) |entry| {
        const artifact = entry.*.cast(std.Build.Step.Compile) orelse continue;
        if (!disposable(artifact)) continue;
        // Distinct Compile nodes can produce the same cached output. Group
        // every potential alias (same name, authored root, and compile filters)
        // so one node cannot retire a binary still needed by another. Different
        // module profiles may over-group, which only delays reclamation.
        const root = if (artifact.root_module.root_source_file) |source| paths.authored(b, source) orelse "generated" else "linked";
        const filters = std.mem.join(b.allocator, "\x00", artifact.filters) catch @panic("OOM");
        const key = b.fmt("{s}\x00{s}\x00{s}", .{ artifact.name, root, filters });
        const group = groups.getOrPut(key) catch @panic("OOM");
        if (!group.found_existing) {
            const release = b.addSystemCommand(&.{"python3"});
            release.setName(b.fmt("release completed {s} outputs", .{artifact.name}));
            release.addFileArg(b.path("tools/release_test_artifact.py"));
            release.addArg("--cache-dir");
            release.addDirectoryArg2(std.Build.LazyPath.cache_root, .{ .make_absolute = true });
            release.has_side_effects = true;
            group.value_ptr.* = release;
            gate.dependOn(&release.step);
        }
        group.value_ptr.*.addDirectoryArg2(artifact.getEmittedBinDirectory(), .{ .make_absolute = true });
        releases.put(artifact, group.value_ptr.*) catch @panic("OOM");
    }
    // Include compiler/linker, run, install, and generated-file consumers. A
    // static object/archive dies after its last link or explicit file reader;
    // executable tests remain until runs and all inventories have exited.
    iterator = steps.keyIterator();
    while (iterator.next()) |entry| {
        if (entry.* == gate) continue;
        for (entry.*.dependencies.items) |dependency| {
            const artifact = dependency.cast(std.Build.Step.Compile) orelse continue;
            const release = releases.get(artifact) orelse continue;
            release.step.dependOn(entry.*);
        }
    }
}
