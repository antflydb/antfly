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

pub fn addToolSteps(ctx: anytype) void {
    const b = ctx.b;
    const target = ctx.target;
    const lib_mod = ctx.lib_mod;

    const hbc_trace_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/tools/hbc_trace.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_trace_mod.addImport("antfly-zig", lib_mod);

    const recall_common_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/recall_common.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    recall_common_mod.addImport("antfly-zig", lib_mod);
    hbc_trace_mod.addImport("recall_common", recall_common_mod);

    const hbc_trace = b.addExecutable(.{
        .name = "hbc_trace",
        .root_module = hbc_trace_mod,
    });

    const run_hbc_trace = b.addRunArtifact(hbc_trace);
    if (b.args) |args| {
        run_hbc_trace.addArgs(args);
    }
    const hbc_trace_step = b.step("hbc-trace", "Trace one Zig HBC query against an exported vector dataset");
    hbc_trace_step.dependOn(&run_hbc_trace.step);

    const hbc_leaf_debug_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/tools/hbc_leaf_debug.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_leaf_debug_mod.addImport("antfly-zig", lib_mod);
    hbc_leaf_debug_mod.addImport("recall_common", recall_common_mod);
    const hbc_leaf_debug = b.addExecutable(.{
        .name = "hbc_leaf_debug",
        .root_module = hbc_leaf_debug_mod,
    });
    const run_hbc_leaf_debug = b.addRunArtifact(hbc_leaf_debug);
    if (b.args) |args| {
        run_hbc_leaf_debug.addArgs(args);
    }
    const hbc_leaf_debug_step = b.step("hbc-leaf-debug", "Inspect cached versus fresh quantized HBC leaf scoring");
    hbc_leaf_debug_step.dependOn(&run_hbc_leaf_debug.step);
}
