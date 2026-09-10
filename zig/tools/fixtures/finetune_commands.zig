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

/// The production aggregate gets these executable dependencies from the command
/// registries. Keep every real entry body and final link, without running models.
pub fn build(b: *std.Build) void {
    _ = project.create(b);
    const checks = b.step("cache-finetune-commands", "Compile registered finetune commands");
    const tests = b.top_level_steps.get("inference-finetune-test").?;
    var count: usize = 0;
    for (tests.step.dependencies.items) |dependency| {
        const command = dependency.cast(std.Build.Step.Compile) orelse continue;
        checks.dependOn(dependency);
        std.debug.print("FINETUNE_COMMAND {s}\n", .{command.name});
        count += 1;
    }
    if (count == 0) @panic("finetune aggregate has no command compilation coverage");
}
