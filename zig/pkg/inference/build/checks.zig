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

pub const CreateCodegenResult = struct {
    quant_kernel_codegen_exe: *std.Build.Step.Compile,
    quant_kernel_codegen_check: *std.Build.Step.Run,
    quant_kernel_codegen_test_check: *std.Build.Step.Run,
};

pub fn createCodegen(ctx: Context) CreateCodegenResult {
    const b = ctx.b;
    const quant_kernel_codegen_exe = b.addExecutable(.{
        .name = "antfly-quant-kernel-codegen",
        .max_rss = 1024 * 1024 * 1024,
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/quant_kernel_codegen_main.zig"),
            .target = b.graph.host,
            .optimize = ctx.optimize,
        }),
    });
    quant_kernel_codegen_exe.root_module.addImport("build_options", ctx.graph.build_options_mod);
    quant_kernel_codegen_exe.root_module.link_libc = ctx.backend.link_libc;
    const quant_kernel_codegen_check = ctx.addRunArtifact(quant_kernel_codegen_exe);
    if (ctx.args) |args| {
        quant_kernel_codegen_check.addArgs(args);
    } else {
        quant_kernel_codegen_check.addArg("--check");
    }
    const quant_kernel_codegen_test_check = ctx.addRunArtifact(quant_kernel_codegen_exe);
    quant_kernel_codegen_test_check.addArg("--check");
    return .{
        .quant_kernel_codegen_exe = quant_kernel_codegen_exe,
        .quant_kernel_codegen_check = quant_kernel_codegen_check,
        .quant_kernel_codegen_test_check = quant_kernel_codegen_test_check,
    };
}

pub const CreateMetalRuntimeTestsResult = struct {
    run_quant_kernel_metal_runtime_check_tests: *std.Build.Step.Run,
};

pub fn createMetalRuntimeTests(ctx: Context) CreateMetalRuntimeTestsResult {
    const b = ctx.b;
    const quant_kernel_metal_runtime_check_tests = b.addTest(.{
        .max_rss = 1024 * 1024 * 1024,
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/quant_kernel_metal_runtime_check.zig"),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
        .filters = &.{"quant kernel metal runtime"},
    });
    const run_quant_kernel_metal_runtime_check_tests = ctx.addRunArtifact(quant_kernel_metal_runtime_check_tests);
    run_quant_kernel_metal_runtime_check_tests.step.max_rss = 64 * 1024 * 1024;
    return .{
        .run_quant_kernel_metal_runtime_check_tests = run_quant_kernel_metal_runtime_check_tests,
    };
}

pub const CreateCudaSourceCheckResult = struct {
    cuda_artifact_source_policy_check: *std.Build.Step.Run,
};

pub fn createCudaSourceCheck(ctx: Context) CreateCudaSourceCheckResult {
    const b = ctx.b;
    const cuda_artifact_source_policy_check = b.addSystemCommand(&.{
        "bash",
        "scripts/regen-cuda-artifacts.sh",
        "--check-source-policy",
    });
    cuda_artifact_source_policy_check.setCwd(ctx.path("."));
    return .{
        .cuda_artifact_source_policy_check = cuda_artifact_source_policy_check,
    };
}
