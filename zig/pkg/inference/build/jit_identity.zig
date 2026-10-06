// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

pub const Modules = struct {
    metal: ?*std.Build.Module,
    cuda: ?*std.Build.Module,

    pub fn addImports(self: Modules, module: *std.Build.Module) void {
        if (self.metal) |identity| module.addImport("metal_jit_identity", identity);
        if (self.cuda) |identity| module.addImport("cuda_jit_identity", identity);
    }
};

/// Fingerprints belong to the enabled backend, not feature options. File
/// arguments participate in Zig's cache and watch graph. No source is read
/// while configuring the build, and disabled backends have no generator edge.
pub fn create(b: *std.Build, root: std.Build.LazyPath, metal: bool, cuda: bool) Modules {
    if (!metal and !cuda) return .{ .metal = null, .cuda = null };
    const tool = b.addExecutable(.{
        .name = "jit-source-identity",
        .root_module = b.createModule(.{
            .root_source_file = root.path(b, "tools/jit_identity.zig"),
            .target = b.graph.host,
            .optimize = .safe,
        }),
    });
    return .{
        .metal = if (metal) add(b, tool, root, .metal) else null,
        .cuda = if (cuda) add(b, tool, root, .cuda) else null,
    };
}

fn add(b: *std.Build, tool: *std.Build.Step.Compile, root: std.Build.LazyPath, backend: enum { metal, cuda }) *std.Build.Module {
    const run = b.addRunArtifact(tool);
    run.addArg("baseline");
    run.addFileArg2(root.path(b, switch (backend) {
        .metal => "src/backends/metal_kernels.m",
        .cuda => "src/ops/cuda/artifacts/inference_cuda_kernels.cu",
    }), .{ .make_absolute = true });
    run.addArg("qualification");
    const qualification: []const []const u8 = switch (backend) {
        .metal => &.{
            "src/backends/metal_runtime.zig",
            "src/graph/kernel_jit.zig",
            "src/graph/quant_kernel_compiler.zig",
            "src/graph/quant_matmul.zig",
            "src/gguf/quant_codec.zig",
            "src/gguf/tensor_types.zig",
        },
        .cuda => &.{
            "src/ops/cuda/kernels.zig",
            "src/graph/kernel_jit.zig",
            "src/graph/quant_kernel_compiler.zig",
            "src/graph/quant_kernel_cuda_renderer.zig",
            "src/graph/quant_matmul.zig",
            "src/gguf/quant_codec.zig",
            "src/gguf/tensor_types.zig",
        },
    };
    run.addArg(b.fmt("{d}", .{qualification.len}));
    for (qualification) |path| run.addFileArg2(root.path(b, path), .{ .make_absolute = true });
    if (backend == .cuda) {
        run.addArgs(&.{ "dispatch", "4" });
        for ([_][]const u8{
            "src/ops/cuda/cuda_compute.zig",
            "src/graph/quant_kernel_compiler.zig",
            "src/graph/quant_matmul.zig",
            "src/gguf/tensor_types.zig",
        }) |path| run.addFileArg2(root.path(b, path), .{ .make_absolute = true });
    }
    run.addArg("--output");
    return b.createModule(.{ .root_source_file = run.addOutputFileArg2(b.fmt("{s}_identity.zig", .{@tagName(backend)}), .{ .make_absolute = true }) });
}
