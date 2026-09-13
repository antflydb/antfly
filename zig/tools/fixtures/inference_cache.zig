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
    var iterator = steps.keyIterator();
    var pilot_found = false;
    var reporting_found = false;
    var pjrt_test_found = false;
    // Give each probe its own output step so unordered artifact traversal cannot
    // change another probe's generated path and invalidate its compilation.
    while (iterator.next()) |entry| {
        const artifact = entry.*.cast(std.Build.Step.Compile) orelse continue;
        profiles.check(artifact);
        profiles.addInferenceWasmProbe(b, artifact);
        profiles.addBenchmarkProbe(b, artifact);
        pjrt_test_found = profiles.addPjrtQualificationProbe(b, artifact) or pjrt_test_found;
        if (std.mem.eql(u8, artifact.name, "antfly-inference")) {
            // Exercise the real executable's final links without its large body.
            artifact.root_module.root_source_file = b.addWriteFiles().add("inference_link.zig",
                \\pub fn main() void {
                \\    @import("std").debug.print("INFERENCE_VERSION {s}\n", .{@import("build_info").version()});
                \\}
            );
            b.step("cache-inference", "Link the actual inference dependency graph").dependOn(&b.addRunArtifact(artifact).step);
        }
        if (std.mem.eql(u8, artifact.name, "generate-gemma4-pilot-dataset")) {
            // Compile the actual deterministic JSONL tool, without runtime or
            // release metadata dependencies.
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
            artifact.root_module.root_source_file = b.addWriteFiles().add("training_version.zig",
                \\pub fn main() void {
                \\    @import("std").debug.print("TRAINING_VERSION {s}\n", .{@import("build_info").version()});
                \\}
            );
            b.step("cache-training-version", "Read actual training release metadata").dependOn(&b.addRunArtifact(artifact).step);
            reporting_found = true;
        }
    }
    profiles.addDataToolChecks(b, &steps);
    profiles.addAssetToolChecks(b, &steps);
    profiles.addOnnxTestChecks(b);
    if (!pilot_found or !reporting_found or !pjrt_test_found) @panic("standalone fixture did not find its actual tool consumers");
}
