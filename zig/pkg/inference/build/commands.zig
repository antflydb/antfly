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
const Context = @import("context.zig").Context;
const runtime_build = @import("runtime.zig");

pub fn addCommands(ctx: Context, install_default: bool) *std.Build.Step.Compile {
    const b = ctx.b;
    const exe = runtime_build.addStandaloneExecutable(b, ctx.graph, ctx.target, ctx.optimize, ctx.paths.inference_root, ctx.backend.link_libc, .{ .strip = ctx.strip, .has_accelerator = ctx.hasAccelerator() });
    const install_exe = b.addInstallArtifact(exe, .{
        .dest_sub_path = "antfly-inference-dev",
    });
    if (install_default) b.getInstallStep().dependOn(&install_exe.step);

    const run_exe = ctx.addRunArtifact(exe);
    run_exe.step.dependOn(&install_exe.step);
    run_exe.addPassthruArgs();
    const run_step = ctx.step("run", "Run the Antfly inference server");
    run_step.dependOn(&run_exe.step);

    const run_finetune = ctx.addRunArtifact(exe);
    run_finetune.step.dependOn(&install_exe.step);
    run_finetune.addArg("finetune");
    run_finetune.addPassthruArgs();
    const finetune_step = ctx.step("finetune", "Run Antfly inference finetune");
    finetune_step.dependOn(&run_finetune.step);

    const bench_server = b.addExecutable(.{
        .name = "antfly-inference-bench-server",
        // Retain video codec compiler headroom and main's larger Darwin Metal
        // allowance for the production prepared-pipeline benchmark.
        .max_rss = @as(usize, if (ctx.target.result.os.tag == .macos and ctx.backend.enable_metal) 14 else 12) * 1024 * 1024 * 1024,
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/benchmark_server.zig"),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
    });
    ctx.graph.linkBuildInfo(bench_server.root_module);
    bench_server.root_module.strip = ctx.strip;
    bench_server.root_module.addImport("inference", ctx.graph.inference_mod);
    bench_server.root_module.addImport("build_options", ctx.graph.build_options_mod);
    bench_server.root_module.addImport("structlog", ctx.graph.structlog_mod);
    bench_server.root_module.addImport("antfly_platform", ctx.graph.platform_mod);
    bench_server.root_module.link_libc = ctx.backend.link_libc;
    const install_bench_server = b.addInstallArtifact(bench_server, .{});
    const bench_server_step = ctx.step("bench-server", "Build the production HTTP server without the other CLI commands");
    bench_server_step.dependOn(&install_bench_server.step);

    const http_baseline_cpu = switch (ctx.target.query.cpu_model) {
        .baseline => true,
        else => false,
    };
    const native_cpu_http_release = ctx.b.graph.host.result.os.tag == .linux and
        ctx.b.graph.host.result.cpu.arch == .x86_64 and ctx.target.result.os.tag == .linux and
        ctx.target.result.cpu.arch == .x86_64 and ctx.target.result.abi == .gnu and
        http_baseline_cpu and ctx.optimize == .fast and ctx.strip and !ctx.hasAccelerator();
    // CI's pinned Nix/glibc compiler measured 3,174,465,536 bytes for this
    // stripped CPU profile. Peak + 25%, rounded up to 64 MiB, is 3,840 MiB.
    // That fits beside two 8 GiB steps inside the existing 22 GiB budget.
    // Other host/target, backend and optimization profiles keep their claims.
    // HTTP-only ReleaseSmall with Metal measured a 4.46 GiB build-tree
    // peak on an M4, under a 10 GiB watchdog with no additional swap.
    // Other optimization modes retain the broader Metal allowance.
    const http_default_memory_gib: usize = if (ctx.target.result.os.tag == .macos and ctx.backend.enable_metal)
        (if (ctx.optimize == .small) 10 else 14)
    else if (ctx.hasAccelerator()) 8 else 6;
    const http_compile_memory_mb: usize = if (native_cpu_http_release) 3840 else http_default_memory_gib * 1024;
    const http_server = b.addExecutable(.{
        .name = "antfly-inference-http-server",
        .max_rss = http_compile_memory_mb * 1024 * 1024,
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/http_server.zig"),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
    });
    ctx.graph.linkBuildInfo(http_server.root_module);
    http_server.root_module.strip = ctx.strip;
    http_server.root_module.addImport("inference", ctx.graph.inference_mod);
    http_server.root_module.addImport("build_options", ctx.graph.build_options_mod);
    http_server.root_module.addImport("structlog", ctx.graph.structlog_mod);
    http_server.root_module.addImport("antfly_platform", ctx.graph.platform_mod);
    http_server.root_module.link_libc = ctx.backend.link_libc;
    const install_http_server = b.addInstallArtifact(http_server, .{});
    const http_server_step = ctx.step("http-server", "Build the constrained production HTTP-only server");
    http_server_step.dependOn(&install_http_server.step);

    return exe;
}
