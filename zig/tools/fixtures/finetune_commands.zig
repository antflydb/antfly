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
    var actual = std.StringHashMap(void).init(b.allocator);
    for (tests.step.dependencies.items) |dependency| {
        const command = dependency.cast(std.Build.Step.Compile) orelse continue;
        if ((actual.getOrPut(command.name) catch @panic("OOM")).found_existing)
            @panic("duplicate command in finetune aggregate");
    }
    for (specs) |spec| {
        if (!actual.remove(spec.name))
            std.debug.panic("finetune aggregate does not compile {s}", .{spec.name});
        std.debug.print("FINETUNE_COMMAND {s}\n", .{spec.name});
    }
    if (actual.count() != 0 or specs.len == 0) @panic("unexpected finetune command coverage");
}
