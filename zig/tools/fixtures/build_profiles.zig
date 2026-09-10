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

/// Exercise real CPU benchmark bodies; use small profile probes for audio/linalg.
pub fn addBenchmarkProbe(b: *std.Build, artifact: *std.Build.Step.Compile) void {
    const training = std.mem.eql(u8, artifact.name, "antfly-inference-training-bench");
    if (training or std.mem.eql(u8, artifact.name, "antfly-inference-paged-attention-bench")) {
        const run = b.addRunArtifact(artifact);
        run.addArgs(if (training) &.{
            "--mode",        "both", "--optimizer-len",       "64", "--optimizer-steps", "2",
            "--graph-batch", "2",    "--graph-width",         "8",  "--graph-depth",     "2",
            "--graph-steps", "2",    "--checkpoint-interval", "1",
        } else &.{
            "--backend",   "native", "--prompt-len",   "4", "--decode-steps",  "2",
            "--page-size", "4",      "--num-heads",    "2", "--num-kv-heads",  "1",
            "--head-dim",  "32",     "--warmup-iters", "0", "--measure-iters", "1",
        });
        b.step(b.fmt("cache-{s}", .{artifact.name}), "Run an actual bounded CPU benchmark workload").dependOn(&run.step);
        return;
    }
    const dependency: []const u8 = if (std.mem.eql(u8, artifact.name, "antfly-inference-audio-bench")) "inference_audio" else if (std.mem.eql(u8, artifact.name, "antfly-inference-linalg-bench")) "inference_linalg" else return;
    const files = b.addWriteFiles();
    artifact.root_module.root_source_file = files.add(b.fmt("{s}.zig", .{artifact.name}), b.fmt("const std = @import(\"std\"); pub fn main() void {{ std.debug.print(\"BENCH_PROFILE {{s}} {{s}}\\n\", .{{ @tagName(@import(\"builtin\").mode), @tagName(@import(\"{s}\").cache_test_profile) }}); }}", .{dependency}));
    b.step(b.fmt("cache-{s}", .{artifact.name}), "Read the actual benchmark/library profile").dependOn(&b.addRunArtifact(artifact).step);
}

/// Keep the actual inference qualification test's imports and runner.
pub fn addPjrtQualificationProbe(b: *std.Build, artifact: *std.Build.Step.Compile) bool {
    if (!artifact.kind.isTest() or artifact.test_runner == null) return false;
    const source = artifact.root_module.root_source_file orelse return false;
    switch (source) {
        .src_path => |path| if (!std.mem.eql(u8, path.sub_path, "src/inference.zig") and
            !std.mem.endsWith(u8, path.sub_path, "/src/inference.zig")) return false,
        else => return false,
    }
    artifact.root_module.root_source_file = b.addWriteFiles().add("pjrt_test.zig",
        \\test "PJRT cache probe" {
        \\    const revision = @import("pjrt").cache_test_revision;
        \\    try @import("std").testing.expect(revision > 0);
        \\    @import("std").debug.print("PJRT_REVISION {d}\n", .{revision});
        \\}
    );
    artifact.filters = &.{"PJRT cache probe"};
    b.step("cache-pjrt-tests", "Exercise actual PJRT qualification imports").dependOn(&b.addRunArtifact(artifact).step);
    return true;
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
