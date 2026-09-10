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

pub fn addPagedAttention(ctx: Context) void {
    const b = ctx.b;
    const bench_exe = b.addExecutable(.{
        .name = "antfly-inference-paged-attention-bench",
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/paged_attention_bench.zig"),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
    });
    bench_exe.root_module.addImport("build_options", ctx.graph.build_options_mod);
    bench_exe.root_module.addImport("inference_linalg", ctx.graph.inference_linalg_mod);
    if (ctx.backend.enable_system_blas) {
        runtime_build.configureSystemBlas(b, bench_exe.root_module, ctx.target, ctx.backend.blas_root);
    }
    runtime_build.configureMetal(b, bench_exe.root_module, ctx.target, ctx.backend.enable_metal, ctx.paths);
    bench_exe.root_module.link_libc = true;

    const run_bench = ctx.addRunArtifact(bench_exe);
    if (ctx.args) |args| {
        run_bench.addArgs(args);
    }
    const bench_step = ctx.step("bench-paged-attention", "Run the native paged-attention benchmark");
    bench_step.dependOn(&run_bench.step);
}

pub fn addTrainingAndLinalg(ctx: Context) void {
    const b = ctx.b;
    const training_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-training-bench",
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/bench/training_bench.zig"),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
    });
    const linalg_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-linalg-bench",
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/linalg_bench.zig"),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
    });
    training_bench_exe.root_module.addImport("build_options", ctx.graph.build_options_mod);
    training_bench_exe.root_module.addImport("ml", ctx.graph.ml_mod);
    ctx.configureNativeTool(training_bench_exe, ctx.backend.enable_metal);
    const run_training_bench = ctx.addRunArtifact(training_bench_exe);
    if (ctx.args) |args| {
        run_training_bench.addArgs(args);
    }
    const training_bench_step = ctx.step("bench-training", "Run the native training benchmark");
    training_bench_step.dependOn(&run_training_bench.step);
    linalg_bench_exe.root_module.addImport("inference_linalg", ctx.graph.inference_linalg_mod);
    const run_linalg_bench = ctx.addRunArtifact(linalg_bench_exe);
    if (ctx.args) |args| {
        run_linalg_bench.addArgs(args);
    }
    const linalg_bench_step = ctx.step("bench-linalg", "Run the shared linalg benchmark");
    linalg_bench_step.dependOn(&run_linalg_bench.step);
}

pub fn addGliner(ctx: Context) void {
    const b = ctx.b;
    const gliner2_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-gliner2-native-bench",
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/bench/gliner2_native.zig"),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
    });
    gliner2_bench_exe.root_module.addImport("build_options", ctx.graph.build_options_mod);
    gliner2_bench_exe.root_module.addImport("ml", ctx.graph.ml_mod);
    if (ctx.graph.pjrt_mod) |pjrt| gliner2_bench_exe.root_module.addImport("pjrt", pjrt);
    gliner2_bench_exe.root_module.addImport("inference_linalg", ctx.graph.inference_linalg_mod);
    gliner2_bench_exe.root_module.addImport("inference_hf_tokenizer", ctx.graph.inference_hf_tokenizer_mod);
    gliner2_bench_exe.root_module.addImport("antfly_image", ctx.graph.image_mod);
    gliner2_bench_exe.root_module.addImport("inference_audio", ctx.graph.inference_audio_mod);
    gliner2_bench_exe.root_module.addImport("protobuf", ctx.graph.protobuf_mod);
    gliner2_bench_exe.root_module.addImport("onnx_graph", ctx.graph.onnx_graph_mod);
    gliner2_bench_exe.root_module.addImport("antfly_platform", ctx.graph.platform_mod);
    ctx.graph.identities.addImports(gliner2_bench_exe.root_module);
    gliner2_bench_exe.root_module.addImport("inference_internal", ctx.graph.inference_internal_mod);
    // inference_internal already owns the native backend linkage, including
    // metal_kernels.m. Linking it again at the executable root produces
    // duplicate Metal symbols in these standalone benchmark tools.
    gliner2_bench_exe.root_module.link_libc = true;
    runtime_build.configureOnnxRuntime(b, gliner2_bench_exe.root_module, ctx.backend.enable_onnx, ctx.backend.onnx_root);
    const run_gliner2_bench = ctx.addRunArtifact(gliner2_bench_exe);
    if (ctx.args) |args| {
        run_gliner2_bench.addArgs(args);
    }
    const gliner2_bench_step = ctx.step("bench-gliner2-native", "Run an end-to-end GLiNER2 bench against the native backend with random weights");
    gliner2_bench_step.dependOn(&run_gliner2_bench.step);
}

pub fn addAudio(ctx: Context) void {
    const b = ctx.b;
    const audio_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-audio-bench",
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/audio_bench.zig"),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
    });
    audio_bench_exe.root_module.addImport("inference_audio", ctx.graph.inference_audio_mod);
    audio_bench_exe.root_module.link_libc = true;
    const run_audio_bench = ctx.addRunArtifact(audio_bench_exe);
    if (ctx.args) |args| {
        run_audio_bench.addArgs(args);
    }
    const audio_bench_step = ctx.step("bench-audio", "Run the checked-in audio decode and synthesis benchmark");
    audio_bench_step.dependOn(&run_audio_bench.step);
}

pub const CreateBgeResult = struct {
    bge_m3_e2e_bench_exe: *std.Build.Step.Compile,
    tests: *std.Build.Step.Compile,
};

pub fn createBge(ctx: Context) CreateBgeResult {
    const b = ctx.b;
    const module_options: std.Build.Module.CreateOptions = .{
        .root_source_file = ctx.path("src/bench/bge_m3_e2e.zig"),
        .target = ctx.target,
        .optimize = ctx.optimize,
    };
    const bge_m3_e2e_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-bge-m3-e2e-bench",
        .root_module = b.createModule(module_options),
    });
    const tests = b.addTest(.{ .root_module = b.createModule(module_options) });
    const bge_m3_runtime_mod = b.createModule(.{
        .root_source_file = ctx.path("src/bge_m3_runtime.zig"),
        .target = ctx.target,
        .optimize = ctx.optimize,
    });
    runtime_build.addInferenceRootImports(bge_m3_runtime_mod, .{
        .build_info_mod = ctx.graph.build_info_mod,
        .identities = ctx.graph.identities,
        .build_options_mod = ctx.graph.build_options_mod,
        .json_mod = ctx.graph.json_mod,
        .httpx_mod = ctx.graph.httpx_mod,
        .inference_api_mod = ctx.graph.inference_api_mod,
        .inference_audio_mod = ctx.graph.inference_audio_mod,
        .inference_chunker_mod = ctx.graph.inference_chunker_mod,
        .jinja_mod = ctx.graph.jinja_mod,
        .inference_tokenizer_mod = ctx.graph.inference_tokenizer_mod,
        .inference_hf_tokenizer_mod = ctx.graph.inference_hf_tokenizer_mod,
        .inference_linalg_mod = ctx.graph.inference_linalg_mod,
        .inference_fixed_tokenizer_data_mod = ctx.graph.inference_fixed_tokenizer_data_mod,
        .jsonschema_mod = ctx.graph.jsonschema_mod,
        .scraping_mod = ctx.graph.scraping_mod,
        .image_mod = ctx.graph.image_mod,
        .ml_mod = ctx.graph.ml_mod,
        .ml_tabular_mod = ctx.graph.ml_tabular_mod,
        .prometheus_mod = ctx.graph.prometheus_mod,
        .structlog_mod = ctx.graph.structlog_mod,
        .onnx_graph_mod = ctx.graph.onnx_graph_mod,
        .pjrt_mod = ctx.graph.pjrt_mod,
        .platform_mod = ctx.graph.platform_mod,
        .protobuf_mod = ctx.graph.protobuf_mod,
        .reader_config_mod = ctx.graph.reader_config_mod,
        .inference_client_mod = ctx.graph.inference_client_mod,
    });
    bge_m3_runtime_mod.addImport("antfly_generating_openapi", ctx.graph.generating_openapi_mod);
    bge_m3_runtime_mod.addImport("antfly_extraction_openapi", ctx.graph.extraction_openapi_mod);
    bge_m3_runtime_mod.addImport("antfly_extracting", ctx.graph.extracting_mod);
    for ([_]*std.Build.Step.Compile{ bge_m3_e2e_bench_exe, tests }) |artifact| {
        artifact.root_module.addImport("build_options", ctx.graph.build_options_mod);
        artifact.root_module.addImport("ml", ctx.graph.ml_mod);
        const pjrt = if (artifact.kind.isTest()) ctx.graph.qualification_pjrt_mod else ctx.graph.pjrt_mod;
        if (pjrt) |module| artifact.root_module.addImport("pjrt", module);
        artifact.root_module.addImport("inference_linalg", ctx.graph.inference_linalg_mod);
        artifact.root_module.addImport("inference_hf_tokenizer", ctx.graph.inference_hf_tokenizer_mod);
        artifact.root_module.addImport("antfly_image", ctx.graph.image_mod);
        artifact.root_module.addImport("inference_audio", ctx.graph.inference_audio_mod);
        artifact.root_module.addImport("protobuf", ctx.graph.protobuf_mod);
        artifact.root_module.addImport("onnx_graph", ctx.graph.onnx_graph_mod);
        artifact.root_module.addImport("bge_m3_runtime", bge_m3_runtime_mod);
        ctx.configureNativeTool(artifact, ctx.backend.enable_metal);
        runtime_build.configureOnnxRuntime(b, artifact.root_module, ctx.backend.enable_onnx, ctx.backend.onnx_root);
    }
    return .{
        .bge_m3_e2e_bench_exe = bge_m3_e2e_bench_exe,
        .tests = tests,
    };
}
