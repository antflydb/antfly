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

const delegated_steps = [_][]const u8{
    "run",
    "finetune",
    "bench-paged-attention",
    "bench-training",
    "bench-linalg",
    "bench-audio",
    "bench-gliner2-native",
    "gliner2-production-readiness",
    "test-finetune",
    "test",
    "wasm",
};

const DelegatedPackageStep = struct {
    run: *std.Build.Step.Run,
    step: *std.Build.Step,
};

pub const DelegatedBuildSteps = struct {
    inference_test: *std.Build.Step,
    inference_finetune_test: *std.Build.Step,
};

pub fn addDelegatedBuildSteps(ctx: anytype) DelegatedBuildSteps {
    const b = ctx.b;
    const enable_metal = ctx.enable_metal;
    const enable_onnx = ctx.enable_onnx;
    const onnx_root = ctx.onnx_root;
    const enable_cuda = ctx.enable_cuda;
    const cuda_artifacts = ctx.cuda_artifacts;
    const enable_system_blas = ctx.enable_system_blas;
    const blas_root = ctx.blas_root;

    var test_step: ?*std.Build.Step = null;
    var finetune_test_step: ?*std.Build.Step = null;
    for (delegated_steps) |step_name| {
        const delegated = addDelegatedPackageStep(b, step_name);
        const run = delegated.run;
        addDelegatedOptions(b, run, enable_metal, enable_onnx, onnx_root, enable_cuda, cuda_artifacts, enable_system_blas, blas_root);
        forwardBuildArgs(b, run);
        if (std.mem.eql(u8, step_name, "test")) {
            test_step = delegated.step;
        } else if (std.mem.eql(u8, step_name, "test-finetune")) {
            finetune_test_step = delegated.step;
        }
    }
    return .{
        .inference_test = test_step.?,
        .inference_finetune_test = finetune_test_step.?,
    };
}

fn addDelegatedPackageStep(b: *std.Build, step_name: []const u8) DelegatedPackageStep {
    const run = b.addSystemCommand(&.{
        b.graph.zig_exe,
        "build",
        step_name,
    });
    run.setCwd(b.path("pkg/inference"));
    const delegated = b.step(
        b.fmt("inference-{s}", .{step_name}),
        b.fmt("Delegate to pkg/inference zig build {s}", .{step_name}),
    );
    delegated.dependOn(&run.step);
    return .{
        .run = run,
        .step = delegated,
    };
}

fn forwardBuildArgs(b: *std.Build, run: *std.Build.Step.Run) void {
    if (b.args) |args| {
        run.addArg("--");
        run.addArgs(args);
    }
}

fn addDelegatedOptions(
    b: *std.Build,
    run: *std.Build.Step.Run,
    enable_metal: bool,
    enable_onnx: bool,
    onnx_root: []const u8,
    enable_cuda: bool,
    cuda_artifacts: []const u8,
    enable_system_blas: bool,
    blas_root: ?[]const u8,
) void {
    run.addArg("-Dshared-lib-root=../..");
    run.addArg(if (enable_metal) "-Dmetal=true" else "-Dmetal=false");
    run.addArg(if (enable_onnx) "-Donnx=true" else "-Donnx=false");
    if (enable_onnx) {
        run.addArg(b.fmt("-Donnx-root={s}", .{onnx_root}));
    }
    run.addArg(if (enable_cuda) "-Dcuda=true" else "-Dcuda=false");
    run.addArg(b.fmt("-Dcuda-artifacts={s}", .{cuda_artifacts}));
    run.addArg(if (enable_system_blas) "-Dsystem-blas=true" else "-Dsystem-blas=false");
    if (enable_system_blas) {
        if (blas_root) |root| run.addArg(b.fmt("-Dblas-root={s}", .{root}));
    }
}
