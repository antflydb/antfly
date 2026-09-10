// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Inspect production module profiles before fixtures substitute entry bodies.
const std = @import("std");

pub fn check(artifact: *std.Build.Step.Compile) void {
    var seen = std.AutoHashMap(*std.Build.Module, void).init(artifact.step.owner.allocator);
    inspect(artifact, artifact.root_module, &seen);
}

fn inspect(artifact: *std.Build.Step.Compile, module: *std.Build.Module, seen: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((seen.getOrPut(module) catch @panic("OOM")).found_existing) return;
    const root = artifact.root_module;
    if (module.resolved_target) |target| {
        if (!std.Target.Query.fromTarget(&target.result).eql(std.Target.Query.fromTarget(&root.resolved_target.?.result)))
            std.debug.panic("{s}: runtime dependency has a different target", .{artifact.name});
    }
    if (module.optimize) |optimize| {
        if (optimize != (root.optimize orelse .Debug))
            std.debug.panic("{s}: runtime dependency uses {s}, expected {s}: {s}", .{
                artifact.name,                                                                             @tagName(optimize), @tagName(root.optimize orelse .Debug),
                if (module.root_source_file) |source| source.getPath(artifact.step.owner) else "C module",
            });
    }
    for (module.link_objects.items) |object| switch (object) {
        .system_lib => |lib| {
            for ([_][]const u8{ "avformat", "avcodec", "avutil", "swresample" }) |name| {
                if (std.mem.eql(u8, lib.name, name)) @panic("runtime links an unused external FFmpeg library");
            }
        },
        // Linked artifacts and generated-source host tools own separate profiles.
        else => {},
    };
    for (module.import_table.values()) |dependency| inspect(artifact, dependency, seen);
}

/// Compile real audio/linalg imports with a small entry, then read builtin.mode.
pub fn addBenchmarkProbe(b: *std.Build, artifact: *std.Build.Step.Compile) void {
    const dependency: []const u8 = if (std.mem.eql(u8, artifact.name, "antfly-inference-audio-bench")) "inference_audio" else if (std.mem.eql(u8, artifact.name, "antfly-inference-linalg-bench")) "inference_linalg" else return;
    const files = b.addWriteFiles();
    artifact.root_module.root_source_file = files.add(b.fmt("{s}.zig", .{artifact.name}), b.fmt("const std = @import(\"std\"); pub fn main() void {{ std.debug.print(\"BENCH_PROFILE {{s}} {{s}}\\n\", .{{ @tagName(@import(\"builtin\").mode), @tagName(@import(\"{s}\").cache_test_profile) }}); }}", .{dependency}));
    b.step(b.fmt("cache-{s}", .{artifact.name}), "Read the actual benchmark/library profile").dependOn(&b.addRunArtifact(artifact).step);
}

// Follow generated sources as well as explicit steps, without freezing module
// graphs before the fixture replaces the expensive compilation bodies.
pub fn collectSteps(step: *std.Build.Step, steps: *std.AutoHashMap(*std.Build.Step, void), modules: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((steps.getOrPut(step) catch @panic("OOM")).found_existing) return;
    for (step.dependencies.items) |dependency| collectSteps(dependency, steps, modules);
    if (step.cast(std.Build.Step.Compile)) |artifact| collectModules(artifact.root_module, steps, modules);
}

fn collectModules(module: *std.Build.Module, steps: *std.AutoHashMap(*std.Build.Step, void), modules: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((modules.getOrPut(module) catch @panic("OOM")).found_existing) return;
    if (module.root_source_file) |source| switch (source) {
        .generated => |generated| collectSteps(generated.file.step, steps, modules),
        else => {},
    };
    for (module.link_objects.items) |object| switch (object) {
        .other_step => |artifact| collectSteps(&artifact.step, steps, modules),
        else => {},
    };
    for (module.import_table.values()) |dependency| collectModules(dependency, steps, modules);
}
