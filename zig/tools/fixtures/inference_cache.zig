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

//! Wrap the standalone entrypoint without reconstructing its declarations.
const std = @import("std");
const project = @import("project_build.zig");
const profiles = @import("cache_profiles.zig");

pub fn build(b: *std.Build) void {
    project.build(b);
    var steps = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    var modules = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
    for (b.top_level_steps.values()) |top| profiles.collectSteps(&top.step, &steps, &modules);
    const files = b.addWriteFiles();
    var iterator = steps.keyIterator();
    var pilot_found = false;
    var reporting_found = false;
    while (iterator.next()) |entry| {
        const artifact = entry.*.cast(std.Build.Step.Compile) orelse continue;
        profiles.check(artifact);
        profiles.addBenchmarkProbe(b, artifact);
        if (std.mem.eql(u8, artifact.name, "generate-gemma4-pilot-dataset")) {
            // Compile and run the actual tool body: it uses inference internals,
            // but writes deterministic JSONL without any release metadata.
            for (artifact.root_module.link_objects.items) |object| switch (object) {
                .other_step => |dependency| if (std.mem.eql(u8, dependency.name, "antfly-build-info")) @panic("pilot tool depends on release metadata"),
                else => {},
            };
            const run = b.addRunArtifact(artifact);
            _ = run.addOutputFileArg("pilot.jsonl");
            run.addArg("2");
            b.step("cache-pilot", "Generate actual pilot data").dependOn(&run.step);
            pilot_found = true;
        }
        if (std.mem.eql(u8, artifact.name, "train-gliner2-autodiff")) {
            // Keep the real manifest writer's module and final-link inputs.
            // Its expensive trainer body becomes a direct version-reporting probe.
            artifact.root_module.root_source_file = files.add("training_version.zig",
                \\pub fn main() void {
                \\    @import("std").debug.print("TRAINING_VERSION {s}\n", .{@import("build_info").version()});
                \\}
            );
            b.step("cache-training-version", "Read actual training release metadata").dependOn(&b.addRunArtifact(artifact).step);
            reporting_found = true;
        }
    }
    if (!pilot_found or !reporting_found) @panic("standalone fixture did not find its actual tool consumers");
}
