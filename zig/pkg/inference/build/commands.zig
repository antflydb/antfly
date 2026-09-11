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

    return exe;
}
