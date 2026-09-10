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

//! The browser runtime owns one target/profile and never receives native modules.
const std = @import("std");
const storage_build = @import("storage.zig");
const platform_build = @import("../../../lib/platform/build_support.zig");
const image_build = @import("../../../lib/image/build_support.zig");
const pdf_build = @import("../../../lib/pdf/build_support.zig");
const tokenizer_build = @import("../../../lib/tokenizer/build_support.zig");
const codegen = @import("codegen.zig");
const configureEmbeddedModule = @import("embedded.zig").configureModule;
const addSnowballModule = @import("snowball.zig").addSnowballModule;

pub const Result = struct {
    artifact: *std.Build.Step.Compile,
    install: []const *std.Build.Step,
    smoke: *std.Build.Step.Run,
};

pub fn add(b: *std.Build, sentencepiece_proto_source: std.Build.LazyPath) Result {
    // Inference requires ReleaseSafe to avoid LLVM WASM -O3/-Os miscompilation.
    const optimize: std.builtin.OptimizeMode = .ReleaseSafe;
    const wasm_target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
        .cpu_features_add = std.Target.wasm.featureSet(&.{ .atomics, .bulk_memory, .simd128 }),
    });
    const lmdb_build_options = storage_build.makeLmdbBuildOptions(b, .zig, false, false);
    const build_options = storage_build.makeRootBuildOptions(b, .zig, false, false, false, false, false, false, true);
    const json_mod = b.createModule(.{ .root_source_file = b.path("lib/json/src/mod.zig"), .target = wasm_target, .optimize = optimize });
    const httpx_mod = b.createModule(.{ .root_source_file = b.path("lib/httpx/src/httpx.zig"), .target = wasm_target, .optimize = optimize });
    httpx_mod.addImport("antfly-json", json_mod);
    const api = codegen.createCommittedModules(b, .{
        .root = b.path("pkg/antfly/src/openapi/generated"),
        .target = wasm_target,
        .optimize = optimize,
        .json = json_mod,
        .httpx = httpx_mod,
    });
    const lmdb_engine_wasm_mod = storage_build.makeLmdbEngineModule(b, wasm_target, optimize, false, lmdb_build_options);
    const wasm_protobuf_mod = b.dependency("protobuf", .{ .target = wasm_target, .optimize = optimize }).module("protobuf");
    const wasm_handlebars_mod = b.dependency("handlebars", .{ .target = wasm_target, .optimize = optimize }).module("handlebars");
    const wasm_platform_mod = platform_build.createModule(b, .{
        .root_source_file = b.path("lib/platform/src/root.zig"),
        .filesystem_capacity_source_file = b.path("lib/platform/src/filesystem_capacity.c"),
        .target = wasm_target,
        .optimize = optimize,
        .link_libc = false,
    });
    const wasm_credentials_mod = b.createModule(.{
        .root_source_file = b.path("lib/credentials/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_objectstore_mod = b.createModule(.{
        .root_source_file = b.path("lib/objectstore/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_google_mod = b.createModule(.{
        .root_source_file = b.path("lib/google/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_vector_mod = b.createModule(.{
        .root_source_file = b.path("lib/vector/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_hash_mod = b.createModule(.{
        .root_source_file = b.path("lib/hash/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_vectorindex_mod = b.createModule(.{
        .root_source_file = b.path("lib/vectorindex/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_bloom_mod = b.createModule(.{
        .root_source_file = b.path("lib/bloom/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_image_mod = image_build.createModule(b, b.path("lib/image"), wasm_target, optimize, wasm_hash_mod);
    const wasm_pdf_standard_fonts_mod = b.createModule(.{
        .root_source_file = b.path("pdf_standard_fonts.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_font_mod = b.createModule(.{
        .root_source_file = b.path("lib/font/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_pdf_mod = pdf_build.createModule(b, b.path("lib/pdf"), wasm_target, optimize, wasm_image_mod, wasm_hash_mod, wasm_font_mod, wasm_pdf_standard_fonts_mod);
    const wasm_sentencepiece_proto_mod = tokenizer_build.createSentencePieceProtoModule(b, sentencepiece_proto_source, wasm_protobuf_mod);
    wasm_google_mod.addImport("httpx", httpx_mod);
    wasm_google_mod.addImport("antfly_credentials", wasm_credentials_mod);
    wasm_google_mod.addImport("antfly_platform", wasm_platform_mod);
    wasm_objectstore_mod.addImport("httpx", httpx_mod);
    wasm_objectstore_mod.addImport("antfly_platform", wasm_platform_mod);
    wasm_objectstore_mod.addImport("antfly_google", wasm_google_mod);
    wasm_vector_mod.addImport("protobuf", wasm_protobuf_mod);
    wasm_vectorindex_mod.addImport("antfly_vector", wasm_vector_mod);
    wasm_vectorindex_mod.addImport("antfly_platform", wasm_platform_mod);
    const vellum_mod = b.createModule(.{ .root_source_file = b.path("lib/vellum/src/mod.zig"), .target = wasm_target, .optimize = optimize });
    const regex_mod = b.createModule(.{ .root_source_file = b.path("lib/regex/src/mod.zig"), .target = wasm_target, .optimize = optimize });
    regex_mod.addImport("antfly_vellum", vellum_mod);
    const chunking_mod = b.createModule(.{ .root_source_file = b.path("lib/chunking/src/mod.zig"), .target = wasm_target, .optimize = optimize });
    chunking_mod.addImport("antfly-json", json_mod);
    chunking_mod.addImport("antfly_chunking_api_openapi", api.chunking_api);
    chunking_mod.addImport("antfly_chunking_openapi", api.chunking);
    const reranking_mod = b.createModule(.{ .root_source_file = b.path("lib/reranking/src/mod.zig"), .target = wasm_target, .optimize = optimize });
    reranking_mod.addImport("antfly-json", json_mod);
    reranking_mod.addImport("antfly_reranking_openapi", api.reranking);
    const embedded_wasm_deps = .{
        build_options,
        lmdb_engine_wasm_mod,
        json_mod,
        api.public,
        api.query,
        api.indexes,
        api.sort,
        api.metadata,
        reranking_mod,
        wasm_objectstore_mod,
        httpx_mod,
        wasm_platform_mod,
        chunking_mod,
        wasm_bloom_mod,
        wasm_vector_mod,
        wasm_vectorindex_mod,
        wasm_hash_mod,
        vellum_mod,
        regex_mod,
        wasm_image_mod,
        wasm_font_mod,
        wasm_pdf_mod,
        wasm_handlebars_mod,
    };

    const embedded_support_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded_root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    @call(.auto, configureEmbeddedModule, .{ b, embedded_support_wasm_mod } ++ embedded_wasm_deps ++ .{addSnowballModule});

    const embedded_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    embedded_wasm_mod.addImport("embedded_support", embedded_support_wasm_mod);

    const embedded_db_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/db.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    embedded_db_wasm_mod.addImport("embedded_support", embedded_support_wasm_mod);

    const embedded_api_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/api.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    embedded_api_wasm_mod.addImport("embedded_support", embedded_support_wasm_mod);
    embedded_api_wasm_mod.addImport("embedded_db_surface", embedded_db_wasm_mod);
    embedded_wasm_mod.addImport("embedded_db_surface", embedded_db_wasm_mod);
    embedded_wasm_mod.addImport("embedded_api_surface", embedded_api_wasm_mod);

    const antfly_embedded_db_pkg_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/db.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    antfly_embedded_db_pkg_wasm_mod.addImport("embedded_db_surface", embedded_db_wasm_mod);

    const antfly_embedded_api_pkg_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/api.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    antfly_embedded_api_pkg_wasm_mod.addImport("embedded_api_surface", embedded_api_wasm_mod);

    const antfly_embedded_pkg_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    antfly_embedded_pkg_wasm_mod.addImport("embedded_surface", embedded_wasm_mod);

    // --- Inference WASM modules for unified antfly.wasm ---
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
    inference_wasm_build_options.addOption(bool, "link_libc", false);
    inference_wasm_build_options.addOption(bool, "skip_openapi", false);
    inference_wasm_build_options.addOption([]const u8, "wasm_memory_model", "wasm32");
    const inference_wasm_build_options_mod = inference_wasm_build_options.createModule();

    const wasm_inference_jinja_mod = b.createModule(.{
        .root_source_file = b.path("lib/jinja/src/jinja.zig"),
        .target = wasm_target,
        .optimize = optimize,
        .single_threaded = true,
    });
    const wasm_inference_tokenizer_mod = b.createModule(.{
        .root_source_file = b.path("lib/tokenizer/src/tokenizer.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    wasm_inference_tokenizer_mod.addImport("sentencepiece_proto", wasm_sentencepiece_proto_mod);
    const wasm_inference_hf_tokenizer_mod = b.createModule(.{
        .root_source_file = b.path("lib/tokenizer/src/hf_root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    wasm_inference_hf_tokenizer_mod.addImport("inference_tokenizer", wasm_inference_tokenizer_mod);
    const wasm_inference_linalg_mod = b.createModule(.{
        .root_source_file = b.path("lib/linalg/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_inference_ml_mod = b.createModule(.{
        .root_source_file = b.path("lib/ml/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
        .single_threaded = true,
    });
    wasm_inference_ml_mod.addImport("antfly_platform", wasm_platform_mod);
    const wasm_inference_onnx_graph_mod = b.createModule(.{
        .root_source_file = b.path("lib/onnx/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
        .single_threaded = true,
    });
    wasm_inference_onnx_graph_mod.addImport("protobuf", wasm_protobuf_mod);
    wasm_inference_onnx_graph_mod.addImport("ml", wasm_inference_ml_mod);
    const wasm_inference_audio_mod = b.createModule(.{
        .root_source_file = b.path("lib/audio/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const inference_wasm_inference_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/wasm_entry.zig"),
        .target = wasm_target,
        .optimize = optimize,
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
        .optimize = optimize,
    });
    antfly_wasm_mod.addImport("antfly_embedded_db", antfly_embedded_db_pkg_wasm_mod);
    antfly_wasm_mod.addImport("antfly_embedded_api", antfly_embedded_api_pkg_wasm_mod);
    antfly_wasm_mod.addImport("inference_runtime", inference_wasm_inference_mod);

    antfly_wasm_mod.single_threaded = true;
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

    const run_antfly_wasm_smoke = b.addSystemCommand(&.{
        "node",
        b.getInstallPath(.prefix, "antfly-wasm/run.mjs"),
    });
    return .{
        .artifact = antfly_wasm,
        .install = std.mem.concat(b.allocator, *std.Build.Step, &.{
            &.{
                &install_antfly_wasm.step,
                &install_antfly_wasm_smoke_run.step,
                &install_antfly_wasm_client.step,
                &install_antfly_wasm_browser.step,
                &install_antfly_wasm_index.step,
                &install_antfly_wasm_readme.step,
                &install_antfly_wasm_webgpu_ops.step,
            },
            &install_shader_steps,
        }) catch @panic("OOM"),
        .smoke = run_antfly_wasm_smoke,
    };
}
