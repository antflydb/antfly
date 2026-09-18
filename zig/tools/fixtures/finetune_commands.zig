// Copyright 2026 Antfly, Inc.
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

/// Inspect the actual aggregate without compiling its commands in a disposable
/// cache. The normal unit gate already builds every executable in this registry.
pub fn build(b: *std.Build) void {
    _ = project.create(b);
    _ = b.step("cache-finetune-registry", "Check command compilation coverage without rebuilding commands");
    const tests = b.top_level_steps.get("inference-finetune-test").?;
    var test_runs: usize = 0;
    for (tests.step.dependencies.items) |dependency| {
        const run = dependency.cast(std.Build.Step.Run) orelse continue;
        for (run.argv.items) |arg| {
            if (arg != .artifact) continue;
            const artifact = arg.artifact.artifact;
            if (artifact.kind != .@"test" or !std.mem.eql(u8, artifact.name, "finetune-tests"))
                @panic("unexpected finetune test executable");
            test_runs += 1;
        }
    }
    if (test_runs != 1) @panic("finetune gate must execute one shared test artifact once");
    const test_root = @embedFile("pkg/inference/src/finetune_test_root.zig");
    var root_count: usize = 0;
    for (@import("pkg/inference/build/finetune/tests.zig").specs) |spec| {
        if (spec.covered_by_inference) continue;
        const import = b.fmt("@import(\"{s}\")", .{spec.root_source_file["src/".len..]});
        if (std.mem.count(u8, test_root, import) != 1)
            std.debug.panic("shared finetune root must import {s} exactly once", .{spec.root_source_file});
        root_count += 1;
    }
    if (std.mem.count(u8, test_root, "@import(") != root_count)
        @panic("unexpected import in shared finetune root");
    var inference_owner_found = false;
    for (b.top_level_steps.get("inference-test").?.step.dependencies.items) |dependency| {
        const run = dependency.cast(std.Build.Step.Run) orelse continue;
        for (run.argv.items) |arg| {
            if (arg != .artifact) continue;
            const source = arg.artifact.artifact.root_module.root_source_file orelse continue;
            if (!std.mem.endsWith(u8, source.getPath(b), "/src/inference.zig")) continue;
            inference_owner_found = true;
            for (@import("pkg/inference/build/finetune/tests.zig").inference_overlap_filters) |filter| {
                var excluded = false;
                for (run.argv.items[1..], 1..) |value, index| {
                    if (value != .bytes or !std.mem.eql(u8, value.bytes, filter)) continue;
                    const previous = run.argv.items[index - 1];
                    if (previous == .bytes and std.mem.eql(u8, previous.bytes, "--skip-test-filter")) excluded = true;
                }
                if (!excluded) @panic("inference repeats a finetuning-owned test group");
            }
        }
    }
    if (!inference_owner_found) @panic("missing inference test owner");
    const specs = @import("pkg/inference/build/finetune/tools.zig").specs ++
        @import("pkg/inference/build/finetune/workflows.zig").specs;
    const checks = b.top_level_steps.get("inference-finetune-command-check") orelse
        @panic("finetune aggregate does not compile command checks");
    if (std.mem.indexOfScalar(*std.Build.Step, tests.step.dependencies.items, &checks.step) == null)
        @panic("finetune aggregate does not compile command checks");
    var actual = std.StringHashMap(*std.Build.Module).init(b.allocator);
    for (checks.step.dependencies.items) |dependency| {
        const group = dependency.cast(std.Build.Step.Compile) orelse @panic("unexpected command check dependency");
        var imports = group.root_module.import_table.iterator();
        while (imports.next()) |entry| {
            if (!std.mem.startsWith(u8, entry.key_ptr.*, "command_")) continue;
            const module = entry.value_ptr.*;
            const path = module.root_source_file.?.getPath(b);
            const result = actual.getOrPut(path) catch @panic("OOM");
            if (result.found_existing) @panic("duplicate command in finetune aggregate");
            result.value_ptr.* = module;
        }
    }
    for (specs) |spec| {
        const path = b.path(b.fmt("pkg/inference/{s}", .{spec.root_source_file})).getPath(b);
        const module = actual.fetchRemove(path) orelse
            std.debug.panic("finetune aggregate does not compile {s}", .{spec.name});
        var expected = std.StringHashMap(void).init(b.allocator);
        for (spec.imports) |dependency| {
            expected.put(@tagName(dependency), {}) catch @panic("OOM");
            if (dependency == .onnx_graph) expected.put("onnx_data", {}) catch @panic("OOM");
        }
        if (spec.assets != null) expected.put("inference_finetune_assets", {}) catch @panic("OOM");
        if (spec.release_metadata) expected.put("build_info", {}) catch @panic("OOM");
        var imports = module.value.import_table.iterator();
        while (imports.next()) |entry| {
            const name = entry.key_ptr.*;
            if (spec.native_link != .none and
                (std.mem.eql(u8, name, "metal_jit_identity") or std.mem.eql(u8, name, "cuda_jit_identity"))) continue;
            if (!expected.remove(name))
                std.debug.panic("command {s} received undeclared import {s}", .{ spec.name, name });
        }
        if (expected.count() != 0) std.debug.panic("command {s} lost declared imports", .{spec.name});
        std.debug.print("FINETUNE_COMMAND {s}\n", .{spec.name});
    }
    if (actual.count() != 0 or specs.len == 0) @panic("unexpected finetune command coverage");
    std.debug.print("FINETUNE_GROUPS {d}\n", .{checks.step.dependencies.items.len});
}
