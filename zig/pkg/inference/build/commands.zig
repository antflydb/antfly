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
const Context = @import("context.zig").Context;
const runtime_build = @import("runtime.zig");

pub fn addCommands(ctx: Context, install_default: bool) *std.Build.Step.Compile {
    const b = ctx.b;
    const exe = runtime_build.addStandaloneExecutable(b, ctx.graph, ctx.target, ctx.optimize, ctx.paths.inference_root, ctx.backend.link_libc);
    const install_exe = b.addInstallArtifact(exe, .{
        .dest_sub_path = "antfly-inference",
    });
    if (install_default) b.getInstallStep().dependOn(&install_exe.step);

    const run_exe = ctx.addRunArtifact(exe);
    run_exe.step.dependOn(&install_exe.step);
    if (ctx.args) |args| {
        run_exe.addArgs(args);
    }
    const run_step = ctx.step("run", "Run the Antfly inference server");
    run_step.dependOn(&run_exe.step);

    const run_finetune = ctx.addRunArtifact(exe);
    run_finetune.step.dependOn(&install_exe.step);
    run_finetune.addArg("finetune");
    if (ctx.args) |args| {
        run_finetune.addArgs(args);
    }
    const finetune_step = ctx.step("finetune", "Run Antfly inference finetune");
    finetune_step.dependOn(&run_finetune.step);

    const bench_server = b.addExecutable(.{
        .name = "antfly-inference-bench-server",
        .max_rss = @as(usize, if (ctx.hasAccelerator()) 7 else 6) * 1024 * 1024 * 1024,
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/benchmark_server.zig"),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
    });
    ctx.graph.linkBuildInfo(bench_server.root_module);
    bench_server.root_module.addImport("inference", ctx.graph.inference_mod);
    bench_server.root_module.addImport("build_options", ctx.graph.build_options_mod);
    bench_server.root_module.addImport("structlog", ctx.graph.structlog_mod);
    bench_server.root_module.addImport("antfly_platform", ctx.graph.platform_mod);
    bench_server.root_module.link_libc = ctx.backend.link_libc;
    const install_bench_server = b.addInstallArtifact(bench_server, .{});
    const bench_server_step = ctx.step("bench-server", "Build the production HTTP server without the other CLI commands");
    bench_server_step.dependOn(&install_bench_server.step);

    return exe;
}
