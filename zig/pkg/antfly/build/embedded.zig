// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

pub fn addUnifiedWasmSteps(ctx: anytype) void {
    const b = ctx.b;
    const wasm_target = ctx.wasm_target;
    const antfly_version = ctx.antfly_version;
    const sentencepiece_proto_mod = ctx.sentencepiece_proto_mod;
    const protobuf_mod = ctx.protobuf_mod;
    const wasm_image_mod = ctx.wasm_image_mod;
    const wasm_platform_mod = ctx.wasm_platform_mod;
    const antfly_embedded_db_pkg_wasm_mod = ctx.antfly_embedded_db_pkg_wasm_mod;
    const antfly_embedded_api_pkg_wasm_mod = ctx.antfly_embedded_api_pkg_wasm_mod;

    const inference_wasm_build_options = b.addOptions();
    inference_wasm_build_options.addOption(bool, "enable_onnx", false);
    inference_wasm_build_options.addOption(bool, "enable_pjrt", false);
    inference_wasm_build_options.addOption(bool, "enable_cuda", false);
    inference_wasm_build_options.addOption([]const u8, "cuda_artifacts", "portable");
    inference_wasm_build_options.addOption(bool, "enable_metal", false);
    inference_wasm_build_options.addOption(bool, "enable_native", false);
    inference_wasm_build_options.addOption(bool, "enable_system_blas", false);
    inference_wasm_build_options.addOption(bool, "enable_wasm", true);
    inference_wasm_build_options.addOption(bool, "enable_webgpu", true);
    inference_wasm_build_options.addOption(bool, "enable_ffmpeg_audio", false);
    inference_wasm_build_options.addOption(bool, "link_libc", false);
    inference_wasm_build_options.addOption(bool, "skip_openapi", false);
    inference_wasm_build_options.addOption([]const u8, "inference_version", antfly_version);
    inference_wasm_build_options.addOption([]const u8, "wasm_memory_model", "wasm32");
    const inference_wasm_build_options_mod = inference_wasm_build_options.createModule();

    const wasm_inference_jinja_mod = b.createModule(.{
        .root_source_file = b.path("lib/jinja/src/jinja.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    const wasm_inference_tokenizer_mod = b.createModule(.{
        .root_source_file = b.path("lib/tokenizer/src/tokenizer.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    wasm_inference_tokenizer_mod.addImport("sentencepiece_proto", sentencepiece_proto_mod);
    const wasm_inference_hf_tokenizer_mod = b.createModule(.{
        .root_source_file = b.path("lib/tokenizer/src/hf_root.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    wasm_inference_hf_tokenizer_mod.addImport("inference_tokenizer", wasm_inference_tokenizer_mod);
    const wasm_inference_linalg_mod = b.createModule(.{
        .root_source_file = b.path("lib/linalg/src/mod.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    const wasm_inference_ml_mod = b.createModule(.{
        .root_source_file = b.path("lib/ml/src/root.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    const wasm_inference_onnx_graph_mod = b.createModule(.{
        .root_source_file = b.path("lib/onnx/src/root.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    wasm_inference_onnx_graph_mod.addImport("protobuf", protobuf_mod);
    wasm_inference_onnx_graph_mod.addImport("ml", wasm_inference_ml_mod);
    const wasm_inference_audio_mod = b.createModule(.{
        .root_source_file = b.path("lib/audio/src/mod.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    const inference_wasm_inference_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/wasm_entry.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    inference_wasm_inference_mod.addImport("build_options", inference_wasm_build_options_mod);
    inference_wasm_inference_mod.addImport("inference_audio", wasm_inference_audio_mod);
    inference_wasm_inference_mod.addImport("inference_linalg", wasm_inference_linalg_mod);
    inference_wasm_inference_mod.addImport("inference_tokenizer", wasm_inference_tokenizer_mod);
    inference_wasm_inference_mod.addImport("inference_hf_tokenizer", wasm_inference_hf_tokenizer_mod);
    inference_wasm_inference_mod.addImport("antfly_image", wasm_image_mod);
    inference_wasm_inference_mod.addImport("antfly_platform", wasm_platform_mod);
    inference_wasm_inference_mod.addImport("jinja", wasm_inference_jinja_mod);
    inference_wasm_inference_mod.addImport("ml", wasm_inference_ml_mod);
    inference_wasm_inference_mod.addImport("onnx_graph", wasm_inference_onnx_graph_mod);

    const antfly_wasm_mod = b.createModule(.{
        .root_source_file = b.path("examples/antfly_wasm.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    antfly_wasm_mod.addImport("antfly_embedded_db", antfly_embedded_db_pkg_wasm_mod);
    antfly_wasm_mod.addImport("antfly_embedded_api", antfly_embedded_api_pkg_wasm_mod);
    antfly_wasm_mod.addImport("inference_runtime", inference_wasm_inference_mod);

    const antfly_wasm = b.addExecutable(.{
        .name = "antfly_wasm",
        .root_module = antfly_wasm_mod,
    });
    antfly_wasm.entry = .disabled;
    antfly_wasm.rdynamic = true;
    antfly_wasm.export_memory = true;
    const install_antfly_wasm = b.addInstallArtifact(antfly_wasm, .{
        .dest_dir = .{ .override = .prefix },
        .dest_sub_path = "antfly-wasm/antfly.wasm",
    });
    const install_antfly_wasm_smoke_run = b.addInstallFile(
        b.path("pkg/antfly-embedded/wasm_smoke_run.mjs"),
        "antfly-wasm/run.mjs",
    );
    const install_antfly_wasm_client = b.addInstallFile(
        b.path("pkg/antfly-embedded/wasm_client.mjs"),
        "antfly-wasm/antfly_embedded_wasm_client.mjs",
    );
    const install_antfly_wasm_browser = b.addInstallFile(
        b.path("pkg/antfly-embedded/wasm_smoke_browser.mjs"),
        "antfly-wasm/browser.mjs",
    );
    const install_antfly_wasm_index = b.addInstallFile(
        b.path("pkg/antfly-embedded/wasm_smoke_index.html"),
        "antfly-wasm/index.html",
    );
    const install_antfly_wasm_readme = b.addInstallFile(
        b.path("pkg/antfly-embedded/WASM.md"),
        "antfly-wasm/README.md",
    );

    const install_antfly_wasm_webgpu_ops = b.addInstallFile(
        b.path("pkg/antfly-embedded/webgpu_ops.mjs"),
        "antfly-wasm/webgpu_ops.mjs",
    );
    const shader_names = [_][]const u8{
        "attention",            "causal_attention",     "cross_attention",
        "gqa_cached_attention", "gqa_causal_attention", "layer_norm",
        "matmul",               "matmul_transb",        "matmul_transb_q4_0",
        "matmul_transb_q4_1",   "matmul_transb_q5_0",   "matmul_transb_q5_1",
        "matmul_transb_q8_0",   "matmul_transb_q8_1",   "matmul_transb_iq4_nl",
        "matmul_transb_iq4_xs", "matmul_transb_q2_k",   "matmul_transb_q3_k",
        "matmul_transb_q4_k",   "matmul_transb_q5_k",   "matmul_transb_q6_k",
        "matmul_transb_q8_k",   "rms_norm",
    };
    var install_shader_steps: [shader_names.len]*std.Build.Step = undefined;
    for (shader_names, 0..) |name, i| {
        const install_shader = b.addInstallFile(
            b.path(b.fmt("pkg/antfly-embedded/shaders/{s}.wgsl", .{name})),
            b.fmt("antfly-wasm/shaders/{s}.wgsl", .{name}),
        );
        install_shader_steps[i] = &install_shader.step;
    }

    const install_wasm_step = b.step("install-wasm", "Build and install the unified antfly wasm target (antfly-embedded + inference runtime)");
    install_wasm_step.dependOn(&install_antfly_wasm.step);
    install_wasm_step.dependOn(&install_antfly_wasm_smoke_run.step);
    install_wasm_step.dependOn(&install_antfly_wasm_client.step);
    install_wasm_step.dependOn(&install_antfly_wasm_browser.step);
    install_wasm_step.dependOn(&install_antfly_wasm_index.step);
    install_wasm_step.dependOn(&install_antfly_wasm_readme.step);
    install_wasm_step.dependOn(&install_antfly_wasm_webgpu_ops.step);
    for (&install_shader_steps) |step| {
        install_wasm_step.dependOn(step);
    }

    const run_antfly_wasm_smoke = b.addSystemCommand(&.{
        "node",
        b.getInstallPath(.prefix, "antfly-wasm/run.mjs"),
    });
    run_antfly_wasm_smoke.step.dependOn(&install_antfly_wasm.step);
    run_antfly_wasm_smoke.step.dependOn(&install_antfly_wasm_smoke_run.step);
    run_antfly_wasm_smoke.step.dependOn(&install_antfly_wasm_client.step);

    const wasm_step = b.step("wasm", "Build and run the antfly wasm smoke test under Node");
    wasm_step.dependOn(&run_antfly_wasm_smoke.step);
}

pub fn configureModule(
    b: *std.Build,
    mod: *std.Build.Module,
    build_options: *std.Build.Step.Options,
    lmdb_engine_mod: *std.Build.Module,
    json_mod: *std.Build.Module,
    public_openapi_mod: *std.Build.Module,
    query_openapi_mod: *std.Build.Module,
    indexes_openapi_mod: *std.Build.Module,
    metadata_openapi_mod: *std.Build.Module,
    reranking_mod: *std.Build.Module,
    objectstore_mod: *std.Build.Module,
    platform_mod: *std.Build.Module,
    chunking_mod: *std.Build.Module,
    bloom_mod: *std.Build.Module,
    vector_mod: *std.Build.Module,
    vectorindex_mod: *std.Build.Module,
    vellum_mod: *std.Build.Module,
    regex_mod: *std.Build.Module,
    image_mod: *std.Build.Module,
    font_mod: *std.Build.Module,
    pdf_mod: *std.Build.Module,
    handlebars_mod: *std.Build.Module,
    add_snowball_module: *const fn (*std.Build, *std.Build.Module) void,
) void {
    mod.addOptions("build_options", build_options);
    mod.addImport("lmdb_engine", lmdb_engine_mod);
    mod.addImport("antfly-json", json_mod);
    mod.addImport("antfly_public_openapi", public_openapi_mod);
    mod.addImport("antfly_query_openapi", query_openapi_mod);
    mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    mod.addImport("antfly_metadata_openapi", metadata_openapi_mod);
    mod.addImport("antfly_reranking", reranking_mod);
    mod.addImport("objectstore", objectstore_mod);
    mod.addImport("antfly_platform", platform_mod);
    mod.addImport("antfly_chunking", chunking_mod);
    mod.addImport("bloom", bloom_mod);
    mod.addImport("antfly_vector", vector_mod);
    mod.addImport("antfly_vectorindex", vectorindex_mod);
    mod.addImport("antfly_vellum", vellum_mod);
    mod.addImport("antfly_regex", regex_mod);
    mod.addImport("antfly_image", image_mod);
    mod.addImport("antfly_font", font_mod);
    mod.addImport("antfly_pdf", pdf_mod);
    mod.addImport("handlebars", handlebars_mod);
    add_snowball_module(b, mod);
}
