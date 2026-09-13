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

//! ONNX file data and graph conversion share proto types, with explicit profiles.
const std = @import("std");

pub const Modules = struct {
    data: *std.Build.Module,
    graph: *std.Build.Module,
};

pub fn create(
    b: *std.Build,
    options: struct {
        root: std.Build.LazyPath,
        target: std.Build.ResolvedTarget,
        optimize: std.builtin.OptimizeMode,
        protobuf: *std.Build.Module,
        // Standalone library consumers can inject ML into the exported graph module.
        ml: ?*std.Build.Module = null,
        single_threaded: ?bool = null,
    },
) Modules {
    const data = b.createModule(.{
        .root_source_file = options.root.path(b, "src/data.zig"),
        .target = options.target,
        .optimize = options.optimize,
        .single_threaded = options.single_threaded,
        .imports = &.{.{ .name = "protobuf", .module = options.protobuf }},
    });
    const graph = b.createModule(.{
        .root_source_file = options.root.path(b, "src/root.zig"),
        .target = options.target,
        .optimize = options.optimize,
        .single_threaded = options.single_threaded,
        .imports = &.{
            .{ .name = "protobuf", .module = options.protobuf },
            .{ .name = "onnx_data", .module = data },
        },
    });
    if (options.ml) |ml| graph.addImport("ml", ml);
    return .{ .data = data, .graph = graph };
}

pub fn createDataTests(b: *std.Build, data: *std.Build.Module) *std.Build.Step.Run {
    return b.addRunArtifact(b.addTest(.{ .root_module = data }));
}

pub const Tests = struct {
    data: *std.Build.Step.Run,
    graph: *std.Build.Step.Run,
};

/// Named dependency modules do not contribute their tests to the importing root.
/// Entrypoints compose both test artifacts into their existing aggregates.
pub fn createTests(b: *std.Build, modules: Modules, runner: ?std.Build.Step.Compile.TestRunner) Tests {
    return .{
        .data = createDataTests(b, modules.data),
        .graph = b.addRunArtifact(b.addTest(.{ .root_module = modules.graph, .test_runner = runner })),
    };
}
