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
const runtime = @import("runtime.zig");

pub const Context = struct {
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    paths: runtime.Paths,
    backend: runtime.BackendOptions,
    graph: runtime.Graph,
    args: ?[]const []const u8,
    runtime_test_filter: bool = false,
    step_prefix: []const u8 = "",

    pub fn step(ctx: Context, name: []const u8, description: []const u8) *std.Build.Step {
        return ctx.b.step(ctx.b.fmt("{s}{s}", .{ ctx.step_prefix, name }), description);
    }

    pub fn path(ctx: Context, sub_path: []const u8) std.Build.LazyPath {
        return ctx.b.path(ctx.b.pathJoin(&.{ ctx.paths.inference_root, sub_path }));
    }
    pub fn addRunArtifact(ctx: Context, artifact: *std.Build.Step.Compile) *std.Build.Step.Run {
        const run = ctx.b.addRunArtifact(artifact);
        run.setCwd(ctx.path("."));
        return run;
    }
    pub fn configureNativeTool(ctx: Context, artifact: *std.Build.Step.Compile, metal: bool) void {
        if (ctx.backend.enable_system_blas) runtime.configureSystemBlas(ctx.b, artifact.root_module, ctx.target, ctx.backend.blas_root);
        runtime.configureMetal(ctx.b, artifact.root_module, ctx.target, metal, ctx.paths);
        artifact.root_module.link_libc = true;
    }
    pub fn hasAccelerator(ctx: Context) bool {
        return ctx.backend.enable_metal or ctx.backend.enable_cuda or ctx.backend.enable_onnx or ctx.backend.enable_pjrt;
    }

    pub fn runsOnHost(ctx: Context) bool {
        const host = ctx.b.graph.host.result;
        return ctx.target.result.cpu.arch == host.cpu.arch and ctx.target.result.os.tag == host.os.tag and ctx.target.result.abi == host.abi;
    }
};
