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
