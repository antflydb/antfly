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
const benches = @import("benches.zig");
const checks = @import("checks.zig");
const tests = @import("tests.zig");
const finetune = @import("finetune/common.zig");

pub const Steps = struct {
    inference_test: *std.Build.Step,
    inference_finetune_test: *std.Build.Step,
};

/// The root publishes its supported inference surface, using the same owner
/// constructors as the standalone package. No package-local step lookup or
/// nested build process is needed.
pub fn add(ctx: Context, wasm_jinja: *std.Build.Module, wasm_platform: *std.Build.Module) Steps {
    _ = @import("commands.zig").addCommands(ctx, false);
    benches.addPagedAttention(ctx);
    benches.addTrainingAndLinalg(ctx);
    benches.addGliner(ctx);
    tests.addGliner25Trained(ctx, benches.addGliner25(ctx));
    benches.addAudio(ctx);

    var finetune_ctx = finetune.fromWorkflow(ctx);
    finetune_ctx.publish_targets = false;
    const finetune_step = @import("finetune/tests.zig").addTests(finetune_ctx, "inference-finetune-test");
    const commands = @import("finetune/tools.zig").register(finetune_ctx);
    const workflows = @import("finetune/workflows.zig").register(finetune_ctx);
    for (commands) |command| {
        finetune_step.dependOn(&command.executable.step);
        if (std.mem.eql(u8, command.executable.name, "train-gliner25"))
            ctx.step("train-gliner25", "Run a bounded GLiNER2.5 native training job").dependOn(&command.run.step);
        if (std.mem.eql(u8, command.executable.name, "materialize-gliner25-adapter"))
            ctx.step("materialize-gliner25-adapter", "Materialize a GLiNER2.5 adapter into an atomic FP32 bundle").dependOn(&command.run.step);
    }
    for (workflows) |command| {
        finetune_step.dependOn(&command.executable.step);
        if (std.mem.eql(u8, command.executable.name, "gliner2-entity-training-readiness"))
            ctx.step("gliner2-entity-training-readiness", "Run GLiNER2 entity training readiness").dependOn(&command.run.step);
    }

    const suite = tests.create(ctx);
    const bge_tests = benches.createBge(ctx).tests;
    const test_step = tests.addDefault(ctx, suite, .{
        .codegen = checks.createCodegen(ctx).quant_kernel_codegen_test_check,
        .cuda_source = checks.createCudaSourceCheck(ctx).cuda_artifact_source_policy_check,
        .metal_runtime = checks.createMetalRuntimeTests(ctx).run_quant_kernel_metal_runtime_check_tests,
        .bge_benchmark = ctx.addRunArtifact(bge_tests),
    });
    _ = @import("wasm.zig").addWasm(ctx, wasm_jinja, wasm_platform);
    return .{ .inference_test = test_step, .inference_finetune_test = finetune_step };
}
